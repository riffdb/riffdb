//! The only staging mutation surface: bounded typed rows, no callback or raw txn.
use super::*;
use redb::{ReadableTable, WriteTransaction};
use riffdb_storage_api::{
    MAX_PROJECTION_SOURCE_ROWS, MAX_PROJECTION_SOURCE_STAGING_BYTES,
    MAX_PROJECTION_SOURCE_STEP_BYTES, MAX_PROJECTION_SOURCE_STEP_ROWS,
    proto_codec::{
        encode_projection_source_row_v1, projection_source_row_encode_work_bound_v1,
        projection_source_row_encoded_charge_v1, projection_source_row_max_encoded_bytes_v1,
    },
};

type SourceCurrentAndPreimage = (
    Option<EncodedPageItem<ProjectionSourceWorkAllocationV1<ProjectionSourceRowV1>>>,
    Option<[u8; 32]>,
);

pub(crate) struct SourceStageUpdate<'update, 'stage, 'work, 'schema> {
    transaction: Option<WriteTransaction>,
    map: &'update mut SourceStageMap<'stage, 'work, 'schema>,
    rows: u32,
    encoded_bytes: u64,
    step_rows: u16,
    step_bytes: u64,
    finished: bool,
}

impl<'update, 'stage, 'work, 'schema> SourceStageUpdate<'update, 'stage, 'work, 'schema> {
    pub(super) fn new(
        transaction: WriteTransaction,
        map: &'update mut SourceStageMap<'stage, 'work, 'schema>,
    ) -> Self {
        let rows = map.rows;
        let encoded_bytes = map.encoded_bytes;
        Self {
            transaction: Some(transaction),
            map,
            rows,
            encoded_bytes,
            step_rows: 0,
            step_bytes: 0,
            finished: false,
        }
    }

    pub(crate) fn current(
        &mut self,
        key: &ProjectionSourceWorkAllocationV1<ProjectionGroupKey>,
    ) -> Result<
        Option<EncodedPageItem<ProjectionSourceWorkAllocationV1<ProjectionSourceRowV1>>>,
        StorageError,
    > {
        let result = if key.belongs_to(&self.map.budget) {
            self.current_inner(key)
        } else {
            Err(integrity())
        };
        self.finish_operation(result)
    }

    pub(crate) fn current_with_preimage(
        &mut self,
        key: &ProjectionSourceWorkAllocationV1<ProjectionGroupKey>,
    ) -> Result<SourceCurrentAndPreimage, StorageError> {
        let result = (|| {
            if !key.belongs_to(&self.map.budget) {
                return Err(integrity());
            }
            self.map.active()?;
            self.map.checked_key(key)?;
            let table = self
                .transaction
                .as_ref()
                .ok_or_else(unavailable)?
                .open_table(ROWS)
                .map_err(|_| unavailable())?;
            let current = table.get(key.as_bytes()).map_err(|_| unavailable())?;
            current
                .as_ref()
                .map(|value| {
                    use sha2::{Digest, Sha256};
                    let preimage = Sha256::digest(value.value()).into();
                    self.map
                        .decode_row(value.value(), key)
                        .map(|row| (Some(row), Some(preimage)))
                })
                .unwrap_or(Ok((None, None)))
        })();
        self.finish_operation(result)
    }

    pub(crate) fn put_with_preimage(
        &mut self,
        key: &ProjectionSourceWorkAllocationV1<ProjectionGroupKey>,
        row: &ProjectionSourceWorkAllocationV1<ProjectionSourceRowV1>,
        expected: Option<[u8; 32]>,
    ) -> Result<(), StorageError> {
        let result = (|| {
            if !key.belongs_to(&self.map.budget) || row.group_values() != key.components() {
                return Err(integrity());
            }
            self.map.active()?;
            self.map.checked_key(key)?;
            {
                use sha2::{Digest, Sha256};
                let table = self
                    .transaction
                    .as_ref()
                    .ok_or_else(unavailable)?
                    .open_table(ROWS)
                    .map_err(|_| unavailable())?;
                let current = table.get(key.as_bytes()).map_err(|_| unavailable())?;
                let actual = current
                    .as_ref()
                    .map(|value| Sha256::digest(value.value()).into());
                if actual != expected {
                    return Err(integrity());
                }
            }
            self.put_inner(row)
        })();
        self.finish_operation(result)
    }

    fn current_inner(
        &self,
        key: &ProjectionGroupKey,
    ) -> Result<
        Option<EncodedPageItem<ProjectionSourceWorkAllocationV1<ProjectionSourceRowV1>>>,
        StorageError,
    > {
        self.map.active()?;
        self.map.checked_key(key)?;
        let table = self
            .transaction
            .as_ref()
            .ok_or_else(unavailable)?
            .open_table(ROWS)
            .map_err(|_| unavailable())?;
        let value = table.get(key.as_bytes()).map_err(|_| unavailable())?;
        // Decoder admits generated, canonical, semantic and inspection scratch
        // before allocating. The page guard is destroyed before returning data.
        value
            .as_ref()
            .map(|value| self.map.decode_row(value.value(), key))
            .transpose()
    }

    // Allocation-free preflight for the bridge before put can reach an existing
    // transaction limit. This never mutates the counters or bypasses put checks.
    pub(crate) fn can_fit_put(
        &mut self,
        key: &ProjectionSourceWorkAllocationV1<ProjectionGroupKey>,
        row: &ProjectionSourceWorkAllocationV1<ProjectionSourceRowV1>,
    ) -> Result<bool, StorageError> {
        let result = self.can_fit_put_inner(key, row);
        self.finish_operation(result)
    }

    fn can_fit_put_inner(
        &self,
        key: &ProjectionSourceWorkAllocationV1<ProjectionGroupKey>,
        row: &ProjectionSourceWorkAllocationV1<ProjectionSourceRowV1>,
    ) -> Result<bool, StorageError> {
        self.map.active()?;
        if !key.belongs_to(&self.map.budget)
            || !row.belongs_to(&self.map.budget)
            || row.identity() != self.map.schema.identity()
            || row.locator() != self.map.tentative_locator
            || row.group_values() != key.components()
        {
            return Err(integrity());
        }
        self.map.checked_key(key)?;
        let charge = projection_source_row_encoded_charge_v1(row).map_err(|_| integrity())?;
        if charge.get() > projection_source_row_max_encoded_bytes_v1().map_err(|_| integrity())? {
            return Err(limit());
        }
        let bytes = u64::try_from(key.as_bytes().len())
            .map_err(|_| limit())?
            .checked_add(u64::try_from(charge.get()).map_err(|_| limit())?)
            .ok_or_else(limit)?;
        if bytes > MAX_PROJECTION_SOURCE_STEP_BYTES {
            return Err(limit());
        }
        Ok(self.step_rows < MAX_PROJECTION_SOURCE_STEP_ROWS
            && self
                .step_bytes
                .checked_add(bytes)
                .is_some_and(|sum| sum <= MAX_PROJECTION_SOURCE_STEP_BYTES))
    }

    pub(crate) fn put(
        &mut self,
        row: &ProjectionSourceWorkAllocationV1<ProjectionSourceRowV1>,
    ) -> Result<(), StorageError> {
        let result = self.put_inner(row);
        self.finish_operation(result)
    }

    fn put_inner(
        &mut self,
        row: &ProjectionSourceWorkAllocationV1<ProjectionSourceRowV1>,
    ) -> Result<(), StorageError> {
        self.map.active()?;
        if !row.belongs_to(&self.map.budget)
            || row.identity() != self.map.schema.identity()
            || row.locator() != self.map.tentative_locator
            || row.last_changed_sequence() > self.map.prefix.captured_through()
        {
            return Err(integrity());
        }
        let key_work =
            ProjectionSourceRowV1::stored_parts_work_bound_v1(row.group_values(), row.measures())
                .map_err(|_| limit())?;
        let _key_work = self
            .map
            .budget
            .reserve(key_work)
            .map_err(|_| unavailable())?;
        let key = self
            .map
            .schema
            .group_key(ProjectionGeneration::first(), row.group_values())
            .map_err(|_| integrity())?;
        self.map.checked_key(&key)?;
        self.map
            .schema
            .validate_measure_record(row.measures())
            .map_err(|_| integrity())?;
        // Preflight is allocation-free and comes from the codec, not a guessed
        // multiplier or encoded-size-as-owned-memory assumption.
        let work = projection_source_row_encode_work_bound_v1(row).map_err(|_| limit())?;
        let charge = projection_source_row_encoded_charge_v1(row).map_err(|_| integrity())?;
        if charge.get() > projection_source_row_max_encoded_bytes_v1().map_err(|_| integrity())? {
            return Err(limit());
        }
        let _encode_work = self.map.budget.reserve(work).map_err(|_| unavailable())?;
        let encoded = encode_projection_source_row_v1(row).map_err(|_| integrity())?;
        if encoded.as_bytes().len() != charge.get() {
            return Err(integrity());
        }
        // Existing rows are fully admitted/validated against exact key/context
        // before overwrite, rather than treating any old byte length as valid.
        let old = self.current_inner(&key)?;
        let key_bytes = u64::try_from(key.as_bytes().len()).map_err(|_| limit())?;
        let new_bytes = key_bytes
            .checked_add(u64::try_from(charge.get()).map_err(|_| limit())?)
            .ok_or_else(limit)?;
        let old_bytes = old
            .as_ref()
            .map(|item| {
                key_bytes
                    .checked_add(item.encoded_content_charge().get() as u64)
                    .ok_or_else(limit)
            })
            .transpose()?
            .unwrap_or(0);
        let rows = self
            .rows
            .checked_add(u32::from(old.is_none()))
            .ok_or_else(limit)?;
        let bytes = self
            .encoded_bytes
            .checked_sub(old_bytes)
            .and_then(|v| v.checked_add(new_bytes))
            .ok_or_else(integrity)?;
        self.admit_mutation(new_bytes, rows, bytes)?;
        drop(old);
        {
            let mut table = self
                .transaction
                .as_ref()
                .ok_or_else(unavailable)?
                .open_table(ROWS)
                .map_err(|_| unavailable())?;
            // No AccessGuard escapes: dropping it returns every WritablePage
            // into the evictable write cache before the next operation.
            drop(
                table
                    .insert(key.as_bytes(), encoded.as_bytes())
                    .map_err(|_| unavailable())?,
            );
        }
        self.record_mutation(new_bytes, rows, bytes);
        Ok(())
    }

    pub(crate) fn remove(
        &mut self,
        key: &ProjectionSourceWorkAllocationV1<ProjectionGroupKey>,
    ) -> Result<(), StorageError> {
        let result = self.remove_inner(key);
        self.finish_operation(result)
    }

    fn remove_inner(
        &mut self,
        key: &ProjectionSourceWorkAllocationV1<ProjectionGroupKey>,
    ) -> Result<(), StorageError> {
        if !key.belongs_to(&self.map.budget) {
            return Err(integrity());
        }
        let old = self.current_inner(key)?;
        let Some(old) = old else {
            return Ok(());
        };
        let bytes = u64::try_from(key.as_bytes().len())
            .map_err(|_| limit())?
            .checked_add(old.encoded_content_charge().get() as u64)
            .ok_or_else(limit)?;
        let remaining_rows = self.rows.checked_sub(1).ok_or_else(integrity)?;
        let remaining_bytes = self
            .encoded_bytes
            .checked_sub(bytes)
            .ok_or_else(integrity)?;
        self.admit_mutation(bytes, remaining_rows, remaining_bytes)?;
        drop(old);
        {
            let mut table = self
                .transaction
                .as_ref()
                .ok_or_else(unavailable)?
                .open_table(ROWS)
                .map_err(|_| unavailable())?;
            drop(table.remove(key.as_bytes()).map_err(|_| unavailable())?);
        }
        self.record_mutation(bytes, remaining_rows, remaining_bytes);
        Ok(())
    }

    fn admit_mutation(&self, bytes: u64, rows: u32, total: u64) -> Result<(), StorageError> {
        if rows > MAX_PROJECTION_SOURCE_ROWS
            || total > MAX_PROJECTION_SOURCE_STAGING_BYTES
            || self.step_rows >= MAX_PROJECTION_SOURCE_STEP_ROWS
            || self
                .step_bytes
                .checked_add(bytes)
                .is_none_or(|v| v > MAX_PROJECTION_SOURCE_STEP_BYTES)
        {
            return Err(limit());
        }
        Ok(())
    }

    fn record_mutation(&mut self, bytes: u64, rows: u32, total: u64) {
        self.step_rows += 1;
        self.step_bytes += bytes;
        self.rows = rows;
        self.encoded_bytes = total;
    }

    pub(crate) fn finish(
        mut self,
        completed_command: Option<CommitSequence>,
    ) -> Result<(), StorageError> {
        let result = self.finish_inner(completed_command);
        if result.is_err() {
            self.retire();
        }
        result
    }

    fn finish_inner(
        &mut self,
        completed_command: Option<CommitSequence>,
    ) -> Result<(), StorageError> {
        self.map.active()?;
        let next = match self.map.applied {
            None => Some(CommitSequence::first()),
            Some(sequence) => sequence.checked_next(),
        };
        if completed_command.is_some_and(|sequence| {
            Some(sequence) != next || sequence > self.map.prefix.captured_through()
        }) {
            return Err(integrity());
        }
        let applied = completed_command.or(self.map.applied);
        {
            let mut progress = self
                .transaction
                .as_ref()
                .ok_or_else(unavailable)?
                .open_table(PROGRESS)
                .map_err(|_| unavailable())?;
            progress
                .insert(0, applied.map_or(0, CommitSequence::get))
                .map_err(|_| unavailable())?;
            progress
                .insert(1, u64::from(self.rows))
                .map_err(|_| unavailable())?;
            progress
                .insert(2, self.encoded_bytes)
                .map_err(|_| unavailable())?;
        }
        let transaction = self.transaction.take().ok_or_else(unavailable)?;
        transaction.commit().map_err(|_| unavailable())?;
        #[cfg(test)]
        if completed_command.is_none() {
            self.map.partial_finishes += 1;
        }
        self.map.applied = applied;
        self.map.rows = self.rows;
        self.map.encoded_bytes = self.encoded_bytes;
        self.finished = true;
        Ok(())
    }

    fn finish_operation<T>(&mut self, result: Result<T, StorageError>) -> Result<T, StorageError> {
        if result.is_err() {
            self.retire();
        }
        result
    }

    fn retire(&mut self) {
        // Abort destroys transaction/page owners before destroying Database.
        drop(self.transaction.take());
        self.map.retire();
    }
}

impl Drop for SourceStageUpdate<'_, '_, '_, '_> {
    fn drop(&mut self) {
        if !self.finished {
            self.retire();
        }
    }
}
