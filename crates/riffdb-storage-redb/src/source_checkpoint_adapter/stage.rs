//! Fixed opaque stage adapter. No grouping, event normalization or arithmetic.
use super::*;
use crate::maintenance::{
    SourceFrozenStage, SourceOwnedStagingBackend, SourceStageCursor, SourceStageMap,
    SourceStageUpdate, SourceStagedRow, SourceStagingDiskJob,
};
use riffdb_projection::source_driver::{
    SourceCheckpointStagingBackend, SourceCreateJobInstruction, SourceCreateStageInstruction,
    SourceCurrentEvidence, SourceCurrentInstruction, SourceFinishUpdateInstruction,
    SourceFrozenRow, SourceFrozenRowEvidence, SourcePutCapacityEvidence, SourcePutInstruction,
    SourceStageDestroyedEvidence, SourceStageOperation, SourceStageSession,
};
use riffdb_storage_api::{EncodedContentCharge, ProjectionSourceRowV1};
use riffdb_types::ProjectionGroupKey;

#[doc(hidden)]
pub struct NativeSourceStageJob<'slot> {
    brand: NativeSourceStageBrand,
    disk: SourceStagingDiskJob<'slot>,
}
#[doc(hidden)]
pub struct NativeSourceStage<'stage, 'slot, 'work> {
    inner: SourceStageMap<'stage, 'slot, 'work>,
    brand: &'stage NativeSourceStageBrand,
}
#[doc(hidden)]
pub struct NativeSourceUpdate<'update, 'stage, 'slot, 'work> {
    inner: SourceStageUpdate<'update, 'stage, 'slot, 'work>,
    brand: &'stage NativeSourceStageBrand,
}
#[doc(hidden)]
pub struct NativeSourceFrozen<'stage, 'slot, 'work> {
    pub(super) inner: SourceFrozenStage<'stage, 'slot, 'work>,
    pub(crate) brand: &'stage NativeSourceStageBrand,
}
#[doc(hidden)]
pub struct NativeSourceFrozenCursor<'cursor, 'stage, 'slot, 'work> {
    inner: SourceStageCursor<'cursor, 'stage, 'slot, 'work>,
    brand: &'stage NativeSourceStageBrand,
}
#[doc(hidden)]
pub struct NativeSourceFrozenRow {
    pub(super) inner: SourceStagedRow,
    pub(super) native_key_bytes: usize,
}
impl SourceFrozenRow for NativeSourceFrozenRow {
    fn row(&self) -> &ProjectionSourceRowV1 {
        self.inner.row()
    }
    fn order_key(&self) -> &ProjectionGroupKey {
        self.inner.order_key()
    }
    fn encoded_content_charge(&self) -> EncodedContentCharge {
        self.inner.encoded_content_charge()
    }
    fn native_key_bytes(&self) -> usize {
        self.native_key_bytes
    }
}
fn check_stage(
    actual: &NativeSourceStageBrand,
    expected: &NativeSourceStageBrand,
) -> Result<(), SourceDriverError> {
    if !std::ptr::eq(actual, expected) {
        return Err(SourceDriverError::ForeignCustody);
    }
    Ok(())
}
impl<'work> SourceCheckpointStagingBackend for NativeSourceCheckpointBackend<'work> {
    type Factory = NativeSourceFactory<'work>;
    type Job<'slot>
        = NativeSourceStageJob<'slot>
    where
        Self: 'slot;
    type Stage<'stage, 'slot>
        = NativeSourceStage<'stage, 'slot, 'work>
    where
        Self: 'slot,
        'slot: 'stage;
    type Update<'update, 'stage, 'slot>
        = NativeSourceUpdate<'update, 'stage, 'slot, 'work>
    where
        Self: 'slot,
        'slot: 'stage,
        'stage: 'update;
    type FrozenStage<'stage, 'slot>
        = NativeSourceFrozen<'stage, 'slot, 'work>
    where
        Self: 'slot,
        'slot: 'stage;
    type Cursor<'cursor, 'stage, 'slot>
        = NativeSourceFrozenCursor<'cursor, 'stage, 'slot, 'work>
    where
        Self: 'slot,
        'slot: 'stage,
        'stage: 'cursor;
    type Row = NativeSourceFrozenRow;
    fn create_job<'session, 'slot>(
        factory: &'slot mut Self::Factory,
        actual: &Self::Brand,
        request: SourceCreateJobInstruction<'session, Self>,
    ) -> Result<Self::Job<'slot>, SourceDriverError>
    where
        Self: 'session,
        'session: 'slot,
    {
        let pass = request.pass();
        request.authorize_create(actual)?;
        if !(1..=2).contains(&pass) || !factory.inner.matches_prefix(actual.prefix) {
            return Err(SourceDriverError::ForeignCustody);
        }
        let disk = SourceStagingDiskJob::from_slot(factory.inner.acquire())?;
        Ok(NativeSourceStageJob {
            brand: NativeSourceStageBrand { pass },
            disk,
        })
    }
    fn create_stage<'session, 'stage, 'slot>(
        job: &'stage mut Self::Job<'slot>,
        actual: &Self::Brand,
        request: SourceCreateStageInstruction<'session, Self>,
    ) -> Result<SourceStageSession<'session, 'stage, 'slot, Self>, SourceDriverError>
    where
        Self: 'session,
        'session: 'slot,
        'slot: 'stage,
    {
        request.check_before_create(actual)?;
        let owned = SourceOwnedStagingBackend::new(job.disk.prepare(actual.private_parent)?)?;
        let inner = SourceStageMap::new(owned, actual.plan.schema(), actual.prefix)?;
        request.attach(
            actual,
            &job.brand,
            NativeSourceStage {
                inner,
                brand: &job.brand,
            },
        )
    }
    fn begin_update<'session, 'update, 'stage, 'slot>(
        stage: &'update mut Self::Stage<'stage, 'slot>,
        actual: &Self::Brand,
        actual_stage: &Self::StageBrand,
        request: SourceStageOperation<'session, 'stage, Self>,
    ) -> Result<Self::Update<'update, 'stage, 'slot>, SourceDriverError>
    where
        Self: 'session,
        'session: 'slot,
        'slot: 'stage,
        'stage: 'update,
    {
        check_stage(stage.brand, actual_stage)?;
        request.authorize_update(actual, actual_stage)?;
        Ok(NativeSourceUpdate {
            inner: stage.inner.begin_update()?,
            brand: stage.brand,
        })
    }
    fn current<'key, 'session, 'stage, 'slot>(
        update: &mut Self::Update<'_, 'stage, 'slot>,
        actual: &Self::Brand,
        actual_stage: &Self::StageBrand,
        request: SourceCurrentInstruction<'key, 'session, 'stage, Self>,
    ) -> Result<SourceCurrentEvidence<'key, 'session, 'stage, Self>, SourceDriverError>
    where
        Self: 'session,
        'session: 'slot,
        'slot: 'stage,
    {
        check_stage(update.brand, actual_stage)?;
        request.check_before_read(actual, actual_stage)?;
        let (current, preimage) = update.inner.current_with_preimage(request.key())?;
        request.attach(actual, actual_stage, current, preimage)
    }
    fn can_fit_put<'row, 'key, 'session, 'stage, 'slot>(
        update: &mut Self::Update<'_, 'stage, 'slot>,
        actual: &Self::Brand,
        actual_stage: &Self::StageBrand,
        put: SourcePutInstruction<'row, 'key, 'session, 'stage, Self>,
    ) -> Result<SourcePutCapacityEvidence<'row, 'key, 'session, 'stage, Self>, SourceDriverError>
    where
        Self: 'session,
        'session: 'slot,
        'slot: 'stage,
    {
        check_stage(update.brand, actual_stage)?;
        put.check_before_put(actual, actual_stage)?;
        let fits = update.inner.can_fit_put(put.key(), put.row())?;
        put.capacity(actual, actual_stage, fits)
    }
    fn put<'session, 'stage, 'slot>(
        update: &mut Self::Update<'_, 'stage, 'slot>,
        actual: &Self::Brand,
        actual_stage: &Self::StageBrand,
        request: SourcePutInstruction<'_, '_, 'session, 'stage, Self>,
    ) -> Result<(), SourceDriverError>
    where
        Self: 'session,
        'session: 'slot,
        'slot: 'stage,
    {
        check_stage(update.brand, actual_stage)?;
        request.check_before_put(actual, actual_stage)?;
        update.inner.put_with_preimage(
            request.key(),
            request.row(),
            request.expected_preimage(),
        )?;
        request.authorize(actual, actual_stage)
    }
    fn finish_update<'session, 'stage, 'slot>(
        update: Self::Update<'_, 'stage, 'slot>,
        actual: &Self::Brand,
        actual_stage: &Self::StageBrand,
        request: SourceFinishUpdateInstruction<'session, 'stage, Self>,
    ) -> Result<(), SourceDriverError>
    where
        Self: 'session,
        'session: 'slot,
        'slot: 'stage,
    {
        check_stage(update.brand, actual_stage)?;
        let completed = request.completed_command();
        request.authorize(actual, actual_stage)?;
        update.inner.finish(completed)?;
        Ok(())
    }
    fn freeze<'session, 'stage, 'slot>(
        stage: Self::Stage<'stage, 'slot>,
        actual: &Self::Brand,
        actual_stage: &Self::StageBrand,
        request: SourceStageOperation<'session, 'stage, Self>,
    ) -> Result<Self::FrozenStage<'stage, 'slot>, SourceDriverError>
    where
        Self: 'session,
        'session: 'slot,
        'slot: 'stage,
    {
        check_stage(stage.brand, actual_stage)?;
        request.authorize_freeze(actual, actual_stage)?;
        Ok(NativeSourceFrozen {
            inner: stage.inner.freeze()?,
            brand: stage.brand,
        })
    }
    fn cursor<'session, 'cursor, 'stage, 'slot>(
        frozen: &'cursor mut Self::FrozenStage<'stage, 'slot>,
        actual: &Self::Brand,
        actual_stage: &Self::StageBrand,
        request: SourceStageOperation<'session, 'stage, Self>,
    ) -> Result<Self::Cursor<'cursor, 'stage, 'slot>, SourceDriverError>
    where
        Self: 'session,
        'session: 'slot,
        'slot: 'stage,
        'stage: 'cursor,
    {
        check_stage(frozen.brand, actual_stage)?;
        request.authorize_cursor(actual, actual_stage)?;
        Ok(NativeSourceFrozenCursor {
            inner: frozen.inner.cursor()?,
            brand: frozen.brand,
        })
    }
    fn next_row<'session, 'stage, 'slot>(
        cursor: &mut Self::Cursor<'_, 'stage, 'slot>,
        actual: &Self::Brand,
        actual_stage: &Self::StageBrand,
        request: SourceStageOperation<'session, 'stage, Self>,
    ) -> Result<SourceFrozenRowEvidence<'session, 'stage, Self>, SourceDriverError>
    where
        Self: 'session,
        'session: 'slot,
        'slot: 'stage,
    {
        check_stage(cursor.brand, actual_stage)?;
        request.check_before_next(actual, actual_stage)?;
        let row = cursor
            .inner
            .next()?
            .map(|inner| {
                let native_key_bytes = crate::source_keys::source_row_key_len(inner.order_key())
                    .map_err(|_| SourceDriverError::InputChanged)?;
                Ok::<_, SourceDriverError>(NativeSourceFrozenRow {
                    inner,
                    native_key_bytes,
                })
            })
            .transpose()?;
        request.attach_row(actual, actual_stage, row)
    }
    fn destroy_stage<'session, 'stage, 'slot>(
        frozen: Self::FrozenStage<'stage, 'slot>,
        actual: &Self::Brand,
        actual_stage: &Self::StageBrand,
        request: SourceStageOperation<'session, 'stage, Self>,
    ) -> Result<SourceStageDestroyedEvidence<'session, 'stage, Self>, SourceDriverError>
    where
        Self: 'session,
        'session: 'slot,
        'slot: 'stage,
    {
        check_stage(frozen.brand, actual_stage)?;
        request.check_before_destroy(actual, actual_stage)?;
        drop(frozen);
        request.destroyed(actual, actual_stage)
    }
    fn destroy_job<'session, 'slot>(
        job: Self::Job<'slot>,
        actual: &Self::Brand,
        request: SourceCreateJobInstruction<'session, Self>,
    ) -> Result<(), SourceDriverError>
    where
        Self: 'session,
        'session: 'slot,
    {
        if job.brand.pass != request.pass() {
            return Err(SourceDriverError::ForeignCustody);
        }
        request.authorize_destroy(actual)?;
        drop(job);
        Ok(())
    }
}

impl Drop for NativeSourceFrozenCursor<'_, '_, '_, '_> {
    fn drop(&mut self) {}
}
