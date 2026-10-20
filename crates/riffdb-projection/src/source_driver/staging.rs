//! Fixed opaque native staging operations. Projection alone selects semantic work.
use super::seal::{SourceBrand, SourceStageBrand};
use super::{SourceCheckpointBackend, SourceDriverError, SourceReplayInputBackend};
use riffdb_storage_api::{EncodedContentCharge, EncodedPageItem, ProjectionSourceRowV1};
use riffdb_types::{CommitSequence, ProjectionGroupKey, ProjectionSourceWorkAllocationV1};

type OwnedRow = ProjectionSourceWorkAllocationV1<ProjectionSourceRowV1>;
type OwnedKey = ProjectionSourceWorkAllocationV1<ProjectionGroupKey>;

/// Fixed physical map interface; implementing it alone grants no native brand.
#[doc(hidden)]
pub trait SourceCheckpointStagingBackend: SourceReplayInputBackend {
    /// Exclusive one-slot factory, retained by the original work owner.
    type Factory;
    /// Stable outer job owns its stage brand separately from its mutable disk job.
    type Job<'slot>
    where
        Self: 'slot;
    /// Opaque map; native exposes no raw redb handle.
    type Stage<'stage, 'slot>
    where
        Self: 'slot,
        'slot: 'stage;
    /// One bounded private transaction, exclusively borrowing its map.
    type Update<'update, 'stage, 'slot>
    where
        Self: 'slot,
        'slot: 'stage,
        'stage: 'update;
    /// Immutable completed stage, borrowing the same physical job.
    type FrozenStage<'stage, 'slot>
    where
        Self: 'slot,
        'slot: 'stage;
    /// Bounded streaming reader of the private frozen map only.
    type Cursor<'cursor, 'stage, 'slot>
    where
        Self: 'slot,
        'slot: 'stage,
        'stage: 'cursor;
    /// Charged owned row and canonical ordering key from the frozen map.
    type Row: SourceFrozenRow;

    /// Consumes the exact projection request before borrowing the single slot.
    fn create_job<'session, 'slot>(
        factory: &'slot mut Self::Factory,
        actual: &Self::Brand,
        request: SourceCreateJobInstruction<'session, Self>,
    ) -> Result<Self::Job<'slot>, SourceDriverError>
    where
        Self: 'session,
        'session: 'slot;
    /// Creates one map and attaches its brand from the stable outer job.
    fn create_stage<'session, 'stage, 'slot>(
        job: &'stage mut Self::Job<'slot>,
        actual: &Self::Brand,
        request: SourceCreateStageInstruction<'session, Self>,
    ) -> Result<SourceStageSession<'session, 'stage, 'slot, Self>, SourceDriverError>
    where
        Self: 'session,
        'session: 'slot,
        'slot: 'stage;
    /// Begins a bounded transaction after consuming its exact stage request.
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
        'stage: 'update;
    /// Reads actual canonical current bytes and their exact preimage.
    fn current<'key, 'session, 'stage, 'slot>(
        update: &mut Self::Update<'_, 'stage, 'slot>,
        actual: &Self::Brand,
        actual_stage: &Self::StageBrand,
        request: SourceCurrentInstruction<'key, 'session, 'stage, Self>,
    ) -> Result<SourceCurrentEvidence<'key, 'session, 'stage, Self>, SourceDriverError>
    where
        Self: 'session,
        'session: 'slot,
        'slot: 'stage;
    /// Checks fixed chunk capacity without changing either semantic progress or rows.
    fn can_fit_put<'row, 'key, 'session, 'stage, 'slot>(
        update: &mut Self::Update<'_, 'stage, 'slot>,
        actual: &Self::Brand,
        actual_stage: &Self::StageBrand,
        put: SourcePutInstruction<'row, 'key, 'session, 'stage, Self>,
    ) -> Result<SourcePutCapacityEvidence<'row, 'key, 'session, 'stage, Self>, SourceDriverError>
    where
        Self: 'session,
        'session: 'slot,
        'slot: 'stage;
    /// Rechecks the current preimage before applying the projection-prepared row.
    fn put<'session, 'stage, 'slot>(
        update: &mut Self::Update<'_, 'stage, 'slot>,
        actual: &Self::Brand,
        actual_stage: &Self::StageBrand,
        request: SourcePutInstruction<'_, '_, 'session, 'stage, Self>,
    ) -> Result<(), SourceDriverError>
    where
        Self: 'session,
        'session: 'slot,
        'slot: 'stage;
    /// Commits only bounded rows and the exact optional completed command frontier.
    fn finish_update<'session, 'stage, 'slot>(
        update: Self::Update<'_, 'stage, 'slot>,
        actual: &Self::Brand,
        actual_stage: &Self::StageBrand,
        request: SourceFinishUpdateInstruction<'session, 'stage, Self>,
    ) -> Result<(), SourceDriverError>
    where
        Self: 'session,
        'session: 'slot,
        'slot: 'stage;
    /// Freezes only the actual complete captured application frontier.
    fn freeze<'session, 'stage, 'slot>(
        stage: Self::Stage<'stage, 'slot>,
        actual: &Self::Brand,
        actual_stage: &Self::StageBrand,
        request: SourceStageOperation<'session, 'stage, Self>,
    ) -> Result<Self::FrozenStage<'stage, 'slot>, SourceDriverError>
    where
        Self: 'session,
        'session: 'slot,
        'slot: 'stage;
    /// Starts a fresh linear frozen-map walk; no primary transaction is exposed.
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
        'stage: 'cursor;
    /// Attaches one charged row or the actual end to its consumed stream request.
    fn next_row<'session, 'stage, 'slot>(
        cursor: &mut Self::Cursor<'_, 'stage, 'slot>,
        actual: &Self::Brand,
        actual_stage: &Self::StageBrand,
        request: SourceStageOperation<'session, 'stage, Self>,
    ) -> Result<SourceFrozenRowEvidence<'session, 'stage, Self>, SourceDriverError>
    where
        Self: 'session,
        'session: 'slot,
        'slot: 'stage;
    /// Physically destroys the frozen map before confirming this exact stage.
    fn destroy_stage<'session, 'stage, 'slot>(
        frozen: Self::FrozenStage<'stage, 'slot>,
        actual: &Self::Brand,
        actual_stage: &Self::StageBrand,
        request: SourceStageOperation<'session, 'stage, Self>,
    ) -> Result<SourceStageDestroyedEvidence<'session, 'stage, Self>, SourceDriverError>
    where
        Self: 'session,
        'session: 'slot,
        'slot: 'stage;
    /// Physically destroys the outer disk job before the next slot acquisition.
    fn destroy_job<'session, 'slot>(
        job: Self::Job<'slot>,
        actual: &Self::Brand,
        request: SourceCreateJobInstruction<'session, Self>,
    ) -> Result<(), SourceDriverError>
    where
        Self: 'session,
        'session: 'slot;
}

/// An owned admitted native row, usable only after consuming a branded request.
#[doc(hidden)]
pub trait SourceFrozenRow {
    /// Existing semantic row, retaining its native allocation owner.
    fn row(&self) -> &ProjectionSourceRowV1;
    /// Complete canonical group ordering key from the same native bytes.
    fn order_key(&self) -> &ProjectionGroupKey;
    /// Exact encoded value charge; native key charge is added separately.
    fn encoded_content_charge(&self) -> EncodedContentCharge;
    /// Exact durable Source key length from native codec, not the staging key.
    fn native_key_bytes(&self) -> usize;
}

/// Projection-minted creation or physical-destruction role for a single job.
pub struct SourceCreateJobInstruction<'session, B: SourceCheckpointBackend> {
    pub(super) brand: SourceBrand<'session, B>,
    pub(super) pass: u8,
    pub(super) destroying: bool,
}
impl<'session, B: SourceCheckpointBackend> SourceCreateJobInstruction<'session, B> {
    /// Exact first or second pass; not a source ordinal or allocation authority.
    #[must_use]
    pub const fn pass(&self) -> u8 {
        self.pass
    }
    /// Checks actual private instance before the native slot operation.
    pub fn authorize_create(self, actual: &B::Brand) -> Result<(), SourceDriverError> {
        self.brand.check(actual)?;
        if self.destroying {
            return Err(SourceDriverError::InputChanged);
        }
        Ok(())
    }
    /// Consumes only the physical-job destruction role, never a creation role.
    pub fn authorize_destroy(self, actual: &B::Brand) -> Result<(), SourceDriverError> {
        self.brand.check(actual)?;
        if !self.destroying {
            return Err(SourceDriverError::InputChanged);
        }
        Ok(())
    }
}
/// The exact request to attach an actual stable outer job and new map.
pub struct SourceCreateStageInstruction<'session, B: SourceCheckpointBackend> {
    pub(super) brand: SourceBrand<'session, B>,
}
impl<'session, B: SourceCheckpointStagingBackend> SourceCreateStageInstruction<'session, B> {
    /// Checks exact session before any native map allocation or I/O.
    pub fn check_before_create(&self, actual: &B::Brand) -> Result<(), SourceDriverError> {
        self.brand.check(actual)
    }
    /// Attaches genuine native map custody and its stable nonzero job brand.
    pub fn attach<'stage, 'slot>(
        self,
        actual: &B::Brand,
        actual_stage: &'stage B::StageBrand,
        stage: B::Stage<'stage, 'slot>,
    ) -> Result<SourceStageSession<'session, 'stage, 'slot, B>, SourceDriverError>
    where
        B: 'session,
        'session: 'slot,
        'slot: 'stage,
    {
        self.brand.check(actual)?;
        Ok(SourceStageSession {
            brand: SourceStageBrand::new(self.brand, actual_stage),
            stage,
        })
    }
}
/// Private stage custody; construction requires the consumed create instruction.
pub struct SourceStageSession<'session, 'stage, 'slot, B: SourceCheckpointStagingBackend>
where
    B: 'session,
    'session: 'slot,
    'slot: 'stage,
{
    pub(super) brand: SourceStageBrand<'session, 'stage, B>,
    pub(super) stage: B::Stage<'stage, 'slot>,
}
/// One consuming stage operation; constructors stay inside the projection driver.
pub struct SourceStageOperation<'session, 'stage, B: SourceCheckpointBackend> {
    pub(super) brand: SourceStageBrand<'session, 'stage, B>,
    pub(super) role: SourceStageRole,
}
#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum SourceStageRole {
    BeginUpdate,
    Freeze,
    Cursor,
    NextRow,
    Destroy,
}
impl<'session, 'stage, B: SourceCheckpointBackend> SourceStageOperation<'session, 'stage, B> {
    fn check(
        &self,
        actual: &B::Brand,
        stage: &B::StageBrand,
        role: SourceStageRole,
    ) -> Result<(), SourceDriverError> {
        self.brand.check(actual, stage)?;
        if self.role != role {
            return Err(SourceDriverError::InputChanged);
        }
        Ok(())
    }
    /// Consumes only the begin-update role for the exact stage.
    pub fn authorize_update(
        self,
        actual: &B::Brand,
        stage: &B::StageBrand,
    ) -> Result<(), SourceDriverError> {
        self.check(actual, stage, SourceStageRole::BeginUpdate)
    }
    /// Consumes only the complete-map freeze role for the exact stage.
    pub fn authorize_freeze(
        self,
        actual: &B::Brand,
        stage: &B::StageBrand,
    ) -> Result<(), SourceDriverError> {
        self.check(actual, stage, SourceStageRole::Freeze)
    }
    /// Consumes only the fresh frozen-stream cursor role for the exact stage.
    pub fn authorize_cursor(
        self,
        actual: &B::Brand,
        stage: &B::StageBrand,
    ) -> Result<(), SourceDriverError> {
        self.check(actual, stage, SourceStageRole::Cursor)
    }
}
/// One exact current-row read, bound to the complete requested key and stage.
pub struct SourceCurrentInstruction<'key, 'session, 'stage, B: SourceCheckpointBackend> {
    pub(super) brand: SourceStageBrand<'session, 'stage, B>,
    pub(super) key: &'key OwnedKey,
}
impl<'key, 'session, 'stage, B: SourceCheckpointBackend>
    SourceCurrentInstruction<'key, 'session, 'stage, B>
{
    /// Refuses foreign stage custody before native read/decoder admission.
    pub fn check_before_read(
        &self,
        actual: &B::Brand,
        stage: &B::StageBrand,
    ) -> Result<(), SourceDriverError> {
        self.brand.check(actual, stage)
    }
    /// Complete read-only key from admitted projection preparation.
    #[must_use]
    pub fn key(&self) -> &OwnedKey {
        self.key
    }
    /// Attaches actual current row and exact canonical envelope preimage.
    pub fn attach(
        self,
        actual: &B::Brand,
        stage: &B::StageBrand,
        current: Option<EncodedPageItem<OwnedRow>>,
        preimage: Option<[u8; 32]>,
    ) -> Result<SourceCurrentEvidence<'key, 'session, 'stage, B>, SourceDriverError> {
        self.brand.check(actual, stage)?;
        if current.is_some() != preimage.is_some() {
            return Err(SourceDriverError::InputChanged);
        }
        Ok(SourceCurrentEvidence {
            brand: self.brand,
            key: self.key,
            current,
            preimage,
        })
    }
}
/// Exact native preimage stays attached until the prepared replacement consumes it.
pub struct SourceCurrentEvidence<'key, 'session, 'stage, B: SourceCheckpointBackend> {
    pub(super) brand: SourceStageBrand<'session, 'stage, B>,
    #[allow(dead_code)]
    pub(super) key: &'key OwnedKey,
    pub(super) current: Option<EncodedPageItem<OwnedRow>>,
    pub(super) preimage: Option<[u8; 32]>,
}
/// Exact row replacement; arbitrary semantic rows cannot construct this instruction.
pub struct SourcePutInstruction<'row, 'key, 'session, 'stage, B: SourceCheckpointBackend> {
    pub(super) brand: SourceStageBrand<'session, 'stage, B>,
    pub(super) key: &'key OwnedKey,
    pub(super) row: &'row OwnedRow,
    pub(super) preimage: Option<[u8; 32]>,
}
impl<B: SourceCheckpointBackend> SourcePutInstruction<'_, '_, '_, '_, B> {
    /// Checks stage custody before any native mutation or encode work.
    pub fn check_before_put(
        &self,
        actual: &B::Brand,
        stage: &B::StageBrand,
    ) -> Result<(), SourceDriverError> {
        self.brand.check(actual, stage)
    }
    /// The exact complete canonical key checked by the original current read.
    #[must_use]
    pub fn key(&self) -> &OwnedKey {
        self.key
    }
    /// Existing freshly prepared Source-owned replacement, borrowed only.
    #[must_use]
    pub fn row(&self) -> &OwnedRow {
        self.row
    }
    /// Exact native before-image, including absence; never source authority.
    #[must_use]
    pub const fn expected_preimage(&self) -> Option<[u8; 32]> {
        self.preimage
    }
    /// Consumes this one role only for the matching live session and stage.
    pub fn authorize(
        self,
        actual: &B::Brand,
        stage: &B::StageBrand,
    ) -> Result<(), SourceDriverError> {
        self.brand.check(actual, stage)
    }
}
/// Exact optional command completion attached to one bounded native chunk.
pub struct SourceFinishUpdateInstruction<'session, 'stage, B: SourceCheckpointBackend> {
    pub(super) brand: SourceStageBrand<'session, 'stage, B>,
    pub(super) completed: Option<CommitSequence>,
}
impl<B: SourceCheckpointBackend> SourceFinishUpdateInstruction<'_, '_, B> {
    /// None flushes rows without advancing the completed command frontier.
    #[must_use]
    pub const fn completed_command(&self) -> Option<CommitSequence> {
        self.completed
    }
    /// Consumes the exact chunk/command completion role.
    pub fn authorize(
        self,
        actual: &B::Brand,
        stage: &B::StageBrand,
    ) -> Result<(), SourceDriverError> {
        self.brand.check(actual, stage)
    }
}
/// Actual next row or exact end of the matching immutable frozen stage.
pub struct SourceFrozenRowEvidence<'session, 'stage, B: SourceCheckpointStagingBackend> {
    pub(super) brand: SourceStageBrand<'session, 'stage, B>,
    pub(super) row: Option<B::Row>,
}
impl<'session, 'stage, B: SourceCheckpointStagingBackend>
    SourceStageOperation<'session, 'stage, B>
{
    /// Checks the fixed next-row role before native read/decoding allocations.
    pub fn check_before_next(
        &self,
        actual: &B::Brand,
        stage: &B::StageBrand,
    ) -> Result<(), SourceDriverError> {
        self.check(actual, stage, SourceStageRole::NextRow)
    }
    /// Checks the fixed teardown role before destroying actual native custody.
    pub fn check_before_destroy(
        &self,
        actual: &B::Brand,
        stage: &B::StageBrand,
    ) -> Result<(), SourceDriverError> {
        self.check(actual, stage, SourceStageRole::Destroy)
    }
    /// Attaches native charged row/end only while consuming this exact request.
    pub fn attach_row(
        self,
        actual: &B::Brand,
        stage: &B::StageBrand,
        row: Option<B::Row>,
    ) -> Result<SourceFrozenRowEvidence<'session, 'stage, B>, SourceDriverError> {
        self.check(actual, stage, SourceStageRole::NextRow)?;
        Ok(SourceFrozenRowEvidence {
            brand: self.brand,
            row,
        })
    }
    /// Called only after actual native frozen/map destruction.
    pub fn destroyed(
        self,
        actual: &B::Brand,
        stage: &B::StageBrand,
    ) -> Result<SourceStageDestroyedEvidence<'session, 'stage, B>, SourceDriverError> {
        self.check(actual, stage, SourceStageRole::Destroy)?;
        Ok(SourceStageDestroyedEvidence { brand: self.brand })
    }
}
/// Non-reusable confirmation of this exact native stage teardown role.
pub struct SourceStageDestroyedEvidence<'session, 'stage, B: SourceCheckpointBackend> {
    pub(super) brand: SourceStageBrand<'session, 'stage, B>,
}
impl<B: SourceCheckpointBackend> Drop for SourceStageDestroyedEvidence<'_, '_, B> {
    fn drop(&mut self) {}
}
impl<B: SourceCheckpointStagingBackend> Drop for SourceFrozenRowEvidence<'_, '_, B> {
    fn drop(&mut self) {}
}
impl<B: SourceCheckpointBackend> Drop for SourceCurrentEvidence<'_, '_, '_, B> {
    fn drop(&mut self) {}
}

/// Capacity result consumes and retains the exact pending put instruction.
pub struct SourcePutCapacityEvidence<'row, 'key, 'session, 'stage, B: SourceCheckpointBackend> {
    pub(super) put: SourcePutInstruction<'row, 'key, 'session, 'stage, B>,
    pub(super) fits: bool,
}
impl<'row, 'key, 'session, 'stage, B: SourceCheckpointBackend>
    SourcePutInstruction<'row, 'key, 'session, 'stage, B>
{
    /// Attaches only the fixed chunk-capacity observation, retaining this same put.
    pub fn capacity(
        self,
        actual: &B::Brand,
        stage: &B::StageBrand,
        fits: bool,
    ) -> Result<SourcePutCapacityEvidence<'row, 'key, 'session, 'stage, B>, SourceDriverError> {
        self.brand.check(actual, stage)?;
        Ok(SourcePutCapacityEvidence { put: self, fits })
    }
}
