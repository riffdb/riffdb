//! Closed, disposable canonical staging. No evaluation or publication authority.
//!
//! The named whole-redb reservation is tied to the pinned implementation census.
//! Raw database/transaction/backend/page handles never leave this module family.

use super::{SourceOwnedStagingBackend, SourceStagingDiskOwner, integrity, limit, unavailable};
use crate::store::OriginalPrefixRetentionLease;
use redb::{Database, Durability, TableDefinition};
use riffdb_storage_api::{
    CheckedProjectionSchema, EncodedPageItem, ProjectionSourceRowV1, StorageError,
};
use riffdb_types::{
    CommitSequence, ProjectionGeneration, ProjectionGroupKey, ProjectionSourceLocatorV1,
    ProjectionSourceWorkAllocationV1, ProjectionSourceWorkReservationV1,
    ProjectionSourceWorkingBudgetV1,
};
use std::{marker::PhantomData, rc::Rc, sync::atomic::Ordering};

#[cfg(all(test, feature = "projection-source-driver"))]
#[path = "source_staging_map_evaluate.rs"]
mod test_evaluate;
#[cfg(all(test, feature = "projection-source-driver"))]
pub(crate) use test_evaluate::SourceStageEvaluationError;
#[path = "source_staging_map_update.rs"]
mod update;
pub(crate) use update::SourceStageUpdate;
#[path = "source_staging_map_cursor.rs"]
mod cursor;
pub(crate) use cursor::{SourceStageCursor, SourceStagedRow};

pub(crate) const REDB_WORKING_BYTES: u64 = 40 * 1024 * 1024;
const CACHE_BYTES: usize = 64 * 1024;
const ROWS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("source_rows_v1");
const PROGRESS: TableDefinition<u8, u64> = TableDefinition::new("source_progress_v1");

pub(crate) struct SourceStageMap<'stage, 'work, 'schema> {
    // Physical destruction order is part of admission: redb/cache before disk
    // cleanup, and the complete redb reservation last. No alias can escape.
    database: Option<Database>,
    disk: SourceStagingDiskOwner<'stage, 'work>,
    schema: &'schema CheckedProjectionSchema,
    prefix: &'work OriginalPrefixRetentionLease,
    tentative_locator: ProjectionSourceLocatorV1,
    budget: ProjectionSourceWorkingBudgetV1,
    applied: Option<CommitSequence>,
    rows: u32,
    #[cfg(test)]
    partial_finishes: u32,
    encoded_bytes: u64,
    // One executing worker/thread, including drop. This is not a timer or task.
    _single_thread: PhantomData<Rc<()>>,
    _memory: ProjectionSourceWorkReservationV1,
}

impl<'stage, 'work, 'schema> SourceStageMap<'stage, 'work, 'schema> {
    pub(crate) fn new(
        owned: SourceOwnedStagingBackend<'stage, 'work>,
        schema: &'schema CheckedProjectionSchema,
        prefix: &'work OriginalPrefixRetentionLease,
    ) -> Result<Self, StorageError> {
        let tentative = prefix.tentative_source_locator().ok_or_else(unavailable)?;
        Self::create(owned, schema, prefix, tentative)
    }

    // Structural-map qualification only. Old pre-activation fixtures have no
    // allocator; this cannot mint a worker candidate or assigned locator.
    #[cfg(test)]
    pub(crate) fn new_structural(
        owned: SourceOwnedStagingBackend<'stage, 'work>,
        schema: &'schema CheckedProjectionSchema,
        prefix: &'work OriginalPrefixRetentionLease,
        tentative: ProjectionSourceLocatorV1,
    ) -> Result<Self, StorageError> {
        Self::create(owned, schema, prefix, tentative)
    }

    fn create(
        owned: SourceOwnedStagingBackend<'stage, 'work>,
        schema: &'schema CheckedProjectionSchema,
        prefix: &'work OriginalPrefixRetentionLease,
        tentative_locator: ProjectionSourceLocatorV1,
    ) -> Result<Self, StorageError> {
        // Exact capture-object binding is stronger than coincident lineage or
        // budget limits. Check before any redb reservation or construction.
        match &owned.disk._job._admission {
            super::StageAdmission::Combined { _slot: slot } if slot.matches_prefix(prefix) => {}
            _ => return Err(integrity()),
        }
        let history = prefix.captured_history();
        if history.lineage().database_id() != tentative_locator.database_id()
            || history.lineage().history_incarnation()
                != tentative_locator.allocation_history_incarnation()
            || history.tail().frontier().application() != Some(prefix.captured_through())
        {
            return Err(integrity());
        }
        super::cancelled(&owned.disk._job.cancellation)?;
        let budget = owned.disk._job.budget.clone();
        // This must precede Builder, Database, tables, cache or redb file writes.
        // Locals also destroy a failed/partial Database and custody before memory.
        let memory = budget
            .reserve(REDB_WORKING_BYTES)
            .map_err(|_| unavailable())?;
        let SourceOwnedStagingBackend { backend, disk } = owned;
        let mut builder = Database::builder();
        builder.set_cache_size(CACHE_BYTES);
        let database = builder
            .create_with_backend(backend)
            .map_err(|_| unavailable())?;
        {
            let mut write = database.begin_write().map_err(|_| unavailable())?;
            write
                .set_durability(Durability::Immediate)
                .map_err(|_| unavailable())?;
            {
                let _rows = write.open_table(ROWS).map_err(|_| unavailable())?;
                let mut progress = write.open_table(PROGRESS).map_err(|_| unavailable())?;
                for key in [0_u8, 1, 2] {
                    progress.insert(key, 0).map_err(|_| unavailable())?;
                }
            }
            write.commit().map_err(|_| unavailable())?;
        }
        Ok(Self {
            database: Some(database),
            disk,
            schema,
            prefix,
            tentative_locator,
            budget,
            applied: None,
            rows: 0,
            #[cfg(test)]
            partial_finishes: 0,
            encoded_bytes: 0,
            _single_thread: PhantomData,
            _memory: memory,
        })
    }

    pub(crate) fn begin_update(
        &mut self,
    ) -> Result<SourceStageUpdate<'_, 'stage, 'work, 'schema>, StorageError> {
        let result = self
            .active()
            .and_then(|database| database.begin_write().map_err(|_| unavailable()))
            .and_then(|mut transaction| {
                transaction
                    .set_durability(Durability::Immediate)
                    .map_err(|_| unavailable())?;
                Ok(transaction)
            });
        match result {
            Ok(transaction) => Ok(SourceStageUpdate::new(transaction, self)),
            Err(error) => {
                self.retire();
                Err(error)
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn partial_finishes_for_test(&self) -> u32 {
        self.partial_finishes
    }

    #[cfg(test)]
    pub(crate) fn applied_for_test(&self) -> Option<CommitSequence> {
        self.applied
    }

    pub(crate) fn freeze(
        mut self,
    ) -> Result<SourceFrozenStage<'stage, 'work, 'schema>, StorageError> {
        if self.active().is_err() || self.applied != Some(self.prefix.captured_through()) {
            self.retire();
            return Err(integrity());
        }
        Ok(SourceFrozenStage { map: self })
    }

    fn active(&self) -> Result<&Database, StorageError> {
        if self.disk._job.cancellation.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        self.database.as_ref().ok_or_else(unavailable)
    }

    fn retire(&mut self) {
        // Caller must drop transaction/range/page owners first. They are sealed
        // inside this module family and never returned to evaluation code.
        drop(self.database.take());
    }

    fn checked_key(&self, key: &ProjectionGroupKey) -> Result<(), StorageError> {
        if key.identity() != self.schema.identity()
            || key.generation() != ProjectionGeneration::first()
        {
            return Err(integrity());
        }
        self.schema.validate_group_key(key).map_err(|_| integrity())
    }

    fn decode_row(
        &self,
        bytes: &[u8],
        key: &ProjectionGroupKey,
    ) -> Result<
        EncodedPageItem<ProjectionSourceWorkAllocationV1<ProjectionSourceRowV1>>,
        StorageError,
    > {
        self.checked_key(key)?;
        let item = riffdb_storage_api::proto_codec::decode_projection_source_row_admitted_v1(
            bytes,
            self.schema,
            self.tentative_locator,
            self.prefix.captured_through(),
            &self.budget,
        )
        .map_err(decode_error)?;
        if item.value().group_values() != key.components() {
            return Err(integrity());
        }
        Ok(item)
    }
}

fn decode_error(
    error: riffdb_storage_api::proto_codec::ProjectionSourceRecordDecodeErrorV1,
) -> StorageError {
    match error {
        riffdb_storage_api::proto_codec::ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable => unavailable(),
        riffdb_storage_api::proto_codec::ProjectionSourceRecordDecodeErrorV1::Codec(_) => integrity(),
    }
}

pub(crate) struct SourceFrozenStage<'stage, 'work, 'schema> {
    map: SourceStageMap<'stage, 'work, 'schema>,
}

impl<'stage, 'work, 'schema> SourceFrozenStage<'stage, 'work, 'schema> {
    pub(crate) fn rows(&self) -> u32 {
        self.map.rows
    }
    pub(crate) fn encoded_bytes(&self) -> u64 {
        self.map.encoded_bytes
    }
    pub(crate) fn cursor(
        &mut self,
    ) -> Result<SourceStageCursor<'_, 'stage, 'work, 'schema>, StorageError> {
        SourceStageCursor::new(&mut self.map)
    }

    pub(crate) fn readonly_cursor(
        &self,
    ) -> Result<cursor::SourceReadonlyStageCursor<'_, 'stage, 'work, 'schema>, StorageError> {
        cursor::SourceReadonlyStageCursor::new(&self.map)
    }

    pub(crate) fn cursor_after(
        &mut self,
        after: Option<&[u8]>,
    ) -> Result<SourceStageCursor<'_, 'stage, 'work, 'schema>, StorageError> {
        SourceStageCursor::new_after(&mut self.map, after)
    }
}
