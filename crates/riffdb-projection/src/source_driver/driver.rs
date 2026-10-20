//! Two complete evaluations, with physical first-stage destruction between them.
use super::seal::{SourceBrand, SourceStageBrand};
use super::staging::SourceStageRole;
use super::*;
use crate::{
    ProjectionSourceRowRootV1, evaluate_projection_commit_source_admitted_v1,
    projection_source_row_preimage_bytes_v1, projection_source_row_scratch_bytes_v1,
};
use riffdb_storage_api::{
    ChangelogHistoryStateV3, EncodedPageItem, MAX_PROJECTION_SOURCE_OBJECT_BYTES,
    MAX_PROJECTION_SOURCE_ROWS, NativeCatalogOriginReferenceV1, ProjectionSourceDescriptorV1,
    ProjectionSourceRootV1, StorageValueError,
};
use riffdb_types::{
    CommitSequence, ContentDigest, ProjectionSourceLocatorV1, owned_allocation_capacity_bound_v1,
};
use std::mem::{align_of, size_of};

/// Fixed composition backend; only its genuine closed factory creates a session.
#[doc(hidden)]
pub trait SourceCheckpointDriverBackend: SourceCheckpointStagingBackend {
    /// Actual native maintenance state, branded by this same session split.
    type MaintenancePort<'session>: SourceCheckpointNativeMaintenancePort<'session, Self>
    where
        Self: 'session;
    /// Actual same-root native allocator/prior selection observation.
    type PreparationEvidence: SourceNativePreparedAllocationEvidence;
    /// Actual definite native read-back for a fixed applied role.
    type StepEvidence: SourceNativeStepEvidence;
    /// Actual native proof that no mutation was submitted before a stale refusal.
    type PreSubmitRefusalEvidence: SourceNativePreSubmitRefusalEvidence;
    /// Actual preadmitted canonical controls/usage owner, moved into native apply.
    type PreparedStep: SourceNativePreparedStepEvidence;
    /// Actual bounded charged canonical copy rows, prepared outside primary gate.
    type CopyBatch: SourcePreparedCopyBatch;
    /// Fixed off-gate native preparation; no callbacks, sequencing or apply.
    fn prepare_maintenance_step<'prepare, 'candidate, 'stage, 'slot, 'session>(
        actual: &Self::Brand,
        request: SourcePrepareMaintenanceInstruction<
            'prepare,
            'candidate,
            'stage,
            'slot,
            'session,
            Self,
        >,
    ) -> Result<SourcePreparedMaintenanceEvidence<'session, 'stage, Self>, SourceDriverError>
    where
        Self: 'session,
        'session: 'slot,
        'slot: 'stage,
        'stage: 'candidate;
    /// Fixed off-gate bounded frozen-map read and canonical row preparation.
    fn prepare_copy_batch<'prepare, 'session, 'stage, 'slot>(
        frozen: &mut Self::FrozenStage<'stage, 'slot>,
        actual: &Self::Brand,
        request: SourcePrepareCopyInstruction<'prepare, 'session, 'stage, Self>,
    ) -> Result<SourcePreparedCopyEvidence<'session, 'stage, Self>, SourceDriverError>
    where
        Self: 'session,
        'session: 'slot,
        'slot: 'stage;
    /// Exact native publication read-back evidence, constructor-private in native.
    type PublicationEvidence: SourceNativePublicationEvidence;
    /// Borrows separate genuine input, factory and port fields from one backend.
    fn session(&mut self) -> Result<SourceCheckpointSession<'_, Self>, SourceDriverError>;
}
/// Read-only exact native publication facts; implementation alone cannot mint
/// the concrete native backend's private evidence or a branded confirmation.
#[doc(hidden)]
pub trait SourceNativePublicationEvidence {
    /// Descriptor from exact durable reconciliation, retaining its charged owner.
    fn descriptor(&self) -> &ProjectionSourceDescriptorV1;
    /// Actual selected Root from that same durable publication/read-back.
    fn root(&self) -> &ProjectionSourceRootV1;
}
/// One fixed coordinator type, with a fixed associated backend for all calls.
#[doc(hidden)]
pub trait SourceCheckpointMaintenanceCoordinator {
    /// Actual composition fixes this to its concrete native adapter.
    type Backend: SourceCheckpointDriverBackend;
    /// Consumes candidate while map/job/input owners remain on this worker stack.
    fn maintain<'candidate, 'stage, 'slot, 'session>(
        &mut self,
        candidate: SourceCheckpointCandidate<'candidate, 'stage, 'slot, 'session, Self::Backend>,
        native: &mut <Self::Backend as SourceCheckpointDriverBackend>::MaintenancePort<'session>,
    ) -> Result<SourceNativePublicationConfirmation<'session, Self::Backend>, SourceDriverError>
    where
        Self::Backend: 'session,
        'session: 'slot,
        'slot: 'stage,
        'stage: 'candidate;
}
/// One non-detachable borrowed native session. Its factory requires the actual
/// private brand reference; no IDs-only constructor exists for that brand.
pub struct SourceCheckpointSession<'session, B: SourceCheckpointDriverBackend + 'session> {
    brand: SourceBrand<'session, B>,
    input: &'session mut B::Input,
    stages: &'session mut B::Factory,
    plan: &'session SourceDriverPlan<'session>,
    native: B::MaintenancePort<'session>,
    metadata: SourceCapturedMetadata,
}
#[derive(Clone, Copy)]
pub(super) struct SourceCapturedMetadata {
    pub(super) history: ChangelogHistoryStateV3,
    pub(super) locator: ProjectionSourceLocatorV1,
    pub(super) origin: NativeCatalogOriginReferenceV1,
    pub(super) frontier: CommitSequence,
}
impl<'session, B: SourceCheckpointDriverBackend> SourceCheckpointSession<'session, B> {
    /// Called only by the concrete backend's closed factory after engine/pin,
    /// same-root allocator/pointer/origin and plan-ledger checks. These values
    /// have no authority without the inaccessible actual native brand reference.
    pub fn attach(
        actual: &'session B::Brand,
        input: &'session mut B::Input,
        stages: &'session mut B::Factory,
        plan: &'session SourceDriverPlan<'session>,
        native: B::MaintenancePort<'session>,
        captured: (
            ChangelogHistoryStateV3,
            ProjectionSourceLocatorV1,
            NativeCatalogOriginReferenceV1,
        ),
    ) -> Result<Self, SourceDriverError> {
        let (history, locator, origin) = captured;
        let frontier = history
            .tail()
            .frontier()
            .application()
            .ok_or(SourceDriverError::SourceUnavailable)?;
        if history.lineage().database_id() != locator.database_id()
            || history.lineage().history_incarnation() != locator.allocation_history_incarnation()
        {
            return Err(SourceDriverError::InputChanged);
        }
        Ok(Self {
            brand: SourceBrand::new(actual),
            input,
            stages,
            plan,
            native,
            metadata: SourceCapturedMetadata {
                history,
                locator,
                origin,
                frontier,
            },
        })
    }
}
/// Structural stream measurements; no constructor grants evaluation authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceCheckpointStreamSummary {
    /// Complete independent canonical row count.
    pub row_count: u32,
    /// Semantic row-preimage bytes, excluding native key/value framing.
    pub semantic_row_bytes: u64,
    /// Exact durable native Source key/value bytes, distinct from staging keys.
    pub canonical_row_bytes: u64,
    /// Largest complete durable Source key, authenticated by both frozen passes.
    pub maximum_native_key_bytes: usize,
    /// Existing independent semantic row root, excluding ordinal/generation.
    pub row_root: ContentDigest,
}
/// Constructed only after the second full same-input evaluation and comparison.
/// All borrowed physical owners remain live even when this value is unused.
pub struct SourceCheckpointCandidate<
    'candidate,
    'stage,
    'slot,
    'session,
    B: SourceCheckpointDriverBackend,
> where
    B: 'session,
    'session: 'slot,
    'slot: 'stage,
    'stage: 'candidate,
{
    pub(super) brand: SourceBrand<'session, B>,
    pub(super) stage_brand: SourceStageBrand<'session, 'stage, B>,
    pub(super) frozen: &'candidate mut B::FrozenStage<'stage, 'slot>,
    #[allow(dead_code)]
    pub(super) input: &'candidate mut B::Input,
    pub(super) plan: &'session SourceDriverPlan<'session>,
    pub(super) metadata: SourceCapturedMetadata,
    pub(super) summary: SourceCheckpointStreamSummary,
}
impl<B: SourceCheckpointDriverBackend> SourceCheckpointCandidate<'_, '_, '_, '_, B> {
    /// Read-only stream measurements; these cannot reconstruct a candidate.
    #[must_use]
    pub const fn summary(&self) -> SourceCheckpointStreamSummary {
        self.summary
    }
    /// Same captured application frontier, unaffected by later commands.
    #[must_use]
    pub const fn applied_through(&self) -> CommitSequence {
        self.metadata.frontier
    }
}
impl<B: SourceCheckpointDriverBackend> Drop for SourceCheckpointCandidate<'_, '_, '_, '_, B> {
    fn drop(&mut self) {}
}
/// Successful result requires actual native evidence and the consumed matching
/// publication instruction. There is intentionally no nominal-success factory.
pub struct SourceNativePublicationConfirmation<'session, B: SourceCheckpointDriverBackend> {
    pub(super) brand: SourceBrand<'session, B>,
    pub(super) evidence: B::PublicationEvidence,
}
impl<B: SourceCheckpointDriverBackend> Drop for SourceNativePublicationConfirmation<'_, B> {
    fn drop(&mut self) {}
}

/// Consumes two real staging passes and invokes the fixed coordinator only after
/// equal independent results. Returns complete publication or a typed refusal.
#[doc(hidden)]
pub fn drive_source_checkpoint<B, C>(
    backend: &mut B,
    coordinator: &mut C,
) -> Result<(), SourceDriverError>
where
    B: SourceCheckpointDriverBackend,
    C: SourceCheckpointMaintenanceCoordinator<Backend = B>,
{
    let SourceCheckpointSession {
        brand,
        input,
        stages,
        plan,
        mut native,
        metadata,
    } = B::session(backend)?;
    let first = {
        let mut job = B::create_job(
            stages,
            brand.native(),
            SourceCreateJobInstruction {
                brand: brand.next(),
                pass: 1,
                destroying: false,
            },
        )?;
        let session = B::create_stage(
            &mut job,
            brand.native(),
            SourceCreateStageInstruction {
                brand: brand.next(),
            },
        )?;
        let (mut frozen, stage_brand) =
            evaluate_stage::<B>(&brand, input, plan, metadata, session)?;
        let summary = verify_frozen::<B>(&brand, &stage_brand, &mut frozen, plan, metadata)?;
        let destroyed = B::destroy_stage(
            frozen,
            brand.native(),
            stage_brand.native_stage(),
            operation(&stage_brand, SourceStageRole::Destroy),
        )?;
        destroyed
            .brand
            .check(brand.native(), stage_brand.native_stage())?;
        drop(destroyed);
        drop(stage_brand);
        B::destroy_job(
            job,
            brand.native(),
            SourceCreateJobInstruction {
                brand: brand.next(),
                pass: 1,
                destroying: true,
            },
        )?;
        summary
    };
    let mut job = B::create_job(
        stages,
        brand.native(),
        SourceCreateJobInstruction {
            brand: brand.next(),
            pass: 2,
            destroying: false,
        },
    )?;
    let session = B::create_stage(
        &mut job,
        brand.native(),
        SourceCreateStageInstruction {
            brand: brand.next(),
        },
    )?;
    let (mut frozen, stage_brand) = evaluate_stage::<B>(&brand, input, plan, metadata, session)?;
    let second = verify_frozen::<B>(&brand, &stage_brand, &mut frozen, plan, metadata)?;
    if first != second {
        return Err(SourceDriverError::InputChanged);
    }
    let candidate = SourceCheckpointCandidate {
        brand: brand.next(),
        stage_brand: stage_brand.next(),
        frozen: &mut frozen,
        input,
        plan,
        metadata,
        summary: second,
    };
    let outcome = coordinator.maintain(candidate, &mut native);
    if let Ok(confirmation) = &outcome {
        brand.check_same(&confirmation.brand)?;
        check_publication(
            confirmation.evidence.descriptor(),
            confirmation.evidence.root(),
            plan,
            metadata,
            second,
        )?;
    }
    // Physical teardown also occurs on typed refusal; no durable rollback is implied.
    let destroyed = B::destroy_stage(
        frozen,
        brand.native(),
        stage_brand.native_stage(),
        operation(&stage_brand, SourceStageRole::Destroy),
    )?;
    destroyed
        .brand
        .check(brand.native(), stage_brand.native_stage())?;
    drop(destroyed);
    drop(stage_brand);
    B::destroy_job(
        job,
        brand.native(),
        SourceCreateJobInstruction {
            brand: brand.next(),
            pass: 2,
            destroying: true,
        },
    )?;
    outcome.map(|_| ())
}

fn evaluate_stage<'session, 'stage, 'slot, B>(
    brand: &SourceBrand<'session, B>,
    input: &mut B::Input,
    plan: &SourceDriverPlan<'_>,
    metadata: SourceCapturedMetadata,
    session: SourceStageSession<'session, 'stage, 'slot, B>,
) -> Result<
    (
        B::FrozenStage<'stage, 'slot>,
        SourceStageBrand<'session, 'stage, B>,
    ),
    SourceDriverError,
>
where
    B: SourceCheckpointDriverBackend + 'session,
    'session: 'slot,
    'slot: 'stage,
{
    let rewind = B::rewind(
        input,
        brand.native(),
        SourceRewindInstruction {
            brand: brand.next(),
        },
    )?;
    brand.check_same(&rewind.brand)?;
    drop(rewind);
    let SourceStageSession {
        brand: stage_brand,
        mut stage,
    } = session;
    let mut expected = Some(CommitSequence::first());
    loop {
        let response = B::read_command(
            input,
            brand.native(),
            SourceReadInstruction::new(brand.next(), expected, Some(metadata.frontier)),
        )?;
        match response {
            SourceCommandResponse::ExactEnd(end) => {
                end.check(brand)?;
                break;
            }
            SourceCommandResponse::Command(command) => {
                command.check(brand)?;
                let evaluated = evaluate_projection_commit_source_admitted_v1(
                    plan.captured.plan(),
                    plan.captured.schema(),
                    command.command(),
                    plan.originating_budget(),
                )?;
                if !evaluated.belongs_to(plan.originating_budget()) {
                    return Err(SourceDriverError::ForeignCustody);
                }
                let mut update = B::begin_update(
                    &mut stage,
                    brand.native(),
                    stage_brand.native_stage(),
                    operation(&stage_brand, SourceStageRole::BeginUpdate),
                )?;
                for (key, _) in evaluated.groups() {
                    let key = evaluated.prepare_source_group_key_v1(key)?;
                    let current = B::current(
                        &mut update,
                        brand.native(),
                        stage_brand.native_stage(),
                        SourceCurrentInstruction {
                            brand: stage_brand.next(),
                            key: &key,
                        },
                    )?;
                    current
                        .brand
                        .check(brand.native(), stage_brand.native_stage())?;
                    let row = evaluated.prepare_source_row_v1(
                        &key,
                        current.current.as_ref().map(EncodedPageItem::value),
                        metadata.locator,
                    )?;
                    let preimage = current.preimage;
                    drop(current); // decoded previous graph is gone before encoding/put
                    let put = SourcePutInstruction {
                        brand: stage_brand.next(),
                        key: &key,
                        row: &row,
                        preimage,
                    };
                    let capacity = B::can_fit_put(
                        &mut update,
                        brand.native(),
                        stage_brand.native_stage(),
                        put,
                    )?;
                    capacity
                        .put
                        .brand
                        .check(brand.native(), stage_brand.native_stage())?;
                    if !capacity.fits {
                        B::finish_update(
                            update,
                            brand.native(),
                            stage_brand.native_stage(),
                            SourceFinishUpdateInstruction {
                                brand: stage_brand.next(),
                                completed: None,
                            },
                        )?;
                        update = B::begin_update(
                            &mut stage,
                            brand.native(),
                            stage_brand.native_stage(),
                            operation(&stage_brand, SourceStageRole::BeginUpdate),
                        )?;
                    }
                    B::put(
                        &mut update,
                        brand.native(),
                        stage_brand.native_stage(),
                        capacity.put,
                    )?;
                }
                B::finish_update(
                    update,
                    brand.native(),
                    stage_brand.native_stage(),
                    SourceFinishUpdateInstruction {
                        brand: stage_brand.next(),
                        completed: Some(evaluated.sequence()),
                    },
                )?;
                expected = evaluated.sequence().checked_next();
            }
        }
    }
    let frozen = B::freeze(
        stage,
        brand.native(),
        stage_brand.native_stage(),
        operation(&stage_brand, SourceStageRole::Freeze),
    )?;
    Ok((frozen, stage_brand))
}
fn operation<'session, 'stage, B: SourceCheckpointBackend>(
    brand: &SourceStageBrand<'session, 'stage, B>,
    role: SourceStageRole,
) -> SourceStageOperation<'session, 'stage, B> {
    SourceStageOperation {
        brand: brand.next(),
        role,
    }
}
pub(super) fn invalid(_: StorageValueError) -> SourceDriverError {
    SourceDriverError::InputChanged
}

fn verify_frozen<'session, 'stage, 'slot, B>(
    brand: &SourceBrand<'session, B>,
    stage_brand: &SourceStageBrand<'session, 'stage, B>,
    frozen: &mut B::FrozenStage<'stage, 'slot>,
    plan: &SourceDriverPlan<'_>,
    metadata: SourceCapturedMetadata,
) -> Result<SourceCheckpointStreamSummary, SourceDriverError>
where
    B: SourceCheckpointDriverBackend + 'session,
    'session: 'slot,
    'slot: 'stage,
{
    let mut rows = 0_u32;
    let mut semantic_bytes = 0_u64;
    let mut native_bytes = 0_u64;
    let mut maximum_native_key_bytes = 0_usize;
    {
        let mut cursor = B::cursor(
            frozen,
            brand.native(),
            stage_brand.native_stage(),
            operation(stage_brand, SourceStageRole::Cursor),
        )?;
        loop {
            let evidence = B::next_row(
                &mut cursor,
                brand.native(),
                stage_brand.native_stage(),
                operation(stage_brand, SourceStageRole::NextRow),
            )?;
            evidence
                .brand
                .check(brand.native(), stage_brand.native_stage())?;
            let Some(row) = evidence.row.as_ref() else {
                break;
            };
            rows = rows.checked_add(1).ok_or(SourceDriverError::InputChanged)?;
            semantic_bytes = semantic_bytes
                .checked_add(projection_source_row_preimage_bytes_v1(row.row()).map_err(invalid)?)
                .ok_or(SourceDriverError::InputChanged)?;
            let key_bytes = row.native_key_bytes();
            if key_bytes == 0 || key_bytes > riffdb_types::MAX_KEY_BYTES {
                return Err(SourceDriverError::InputChanged);
            }
            maximum_native_key_bytes = maximum_native_key_bytes.max(key_bytes);
            let native = u64::try_from(key_bytes)
                .ok()
                .and_then(|key| key.checked_add(row.encoded_content_charge().get() as u64))
                .ok_or(SourceDriverError::InputChanged)?;
            native_bytes = native_bytes
                .checked_add(native)
                .ok_or(SourceDriverError::InputChanged)?;
            if rows > MAX_PROJECTION_SOURCE_ROWS
                || native_bytes > MAX_PROJECTION_SOURCE_OBJECT_BYTES
                || semantic_bytes > MAX_PROJECTION_SOURCE_OBJECT_BYTES
            {
                return Err(SourceDriverError::SourceUnavailable);
            }
        }
    }
    // Root state contains its fixed4096-byte canonical ordering witness. Admit
    // that entire inline owner before constructing it; reusable byte scratch has
    // an exact owning-codec bound separately admitted before each allocation.
    let _root_work = plan
        .originating_budget()
        .reserve(size_of::<ProjectionSourceRowRootV1<'_>>() as u64)
        .map_err(|_| SourceDriverError::SourceUnavailable)?;
    let mut root = ProjectionSourceRowRootV1::new(
        plan.identity(),
        Some(metadata.locator),
        metadata.frontier,
        rows,
        semantic_bytes,
    )
    .map_err(invalid)?;
    let mut observed_rows = 0_u32;
    let mut observed_native_bytes = 0_u64;
    let mut observed_maximum_native_key_bytes = 0_usize;
    {
        let mut cursor = B::cursor(
            frozen,
            brand.native(),
            stage_brand.native_stage(),
            operation(stage_brand, SourceStageRole::Cursor),
        )?;
        loop {
            let evidence = B::next_row(
                &mut cursor,
                brand.native(),
                stage_brand.native_stage(),
                operation(stage_brand, SourceStageRole::NextRow),
            )?;
            evidence
                .brand
                .check(brand.native(), stage_brand.native_stage())?;
            let Some(row) = evidence.row.as_ref() else {
                break;
            };
            observed_rows = observed_rows
                .checked_add(1)
                .ok_or(SourceDriverError::InputChanged)?;
            observed_maximum_native_key_bytes =
                observed_maximum_native_key_bytes.max(row.native_key_bytes());
            observed_native_bytes = observed_native_bytes
                .checked_add(
                    u64::try_from(row.native_key_bytes())
                        .ok()
                        .and_then(|key| key.checked_add(row.encoded_content_charge().get() as u64))
                        .ok_or(SourceDriverError::InputChanged)?,
                )
                .ok_or(SourceDriverError::InputChanged)?;
            if observed_rows > rows || observed_native_bytes > native_bytes {
                return Err(SourceDriverError::InputChanged);
            }
            let capacity = projection_source_row_scratch_bytes_v1(row.row()).map_err(invalid)?;
            let scratch_bound = owned_allocation_capacity_bound_v1(capacity, align_of::<u8>())
                .map_err(|_| SourceDriverError::SourceUnavailable)?;
            let _scratch_work = plan
                .originating_budget()
                .reserve(scratch_bound)
                .map_err(|_| SourceDriverError::SourceUnavailable)?;
            let mut scratch = Vec::with_capacity(capacity);
            root.append(row.order_key(), row.row(), &mut scratch)
                .map_err(invalid)?;
            drop(scratch); // work charge remains until physical scratch destruction
        }
    }
    if observed_rows != rows
        || observed_native_bytes != native_bytes
        || observed_maximum_native_key_bytes != maximum_native_key_bytes
    {
        return Err(SourceDriverError::InputChanged);
    }
    let row_root = root.finish().map_err(invalid)?;
    Ok(SourceCheckpointStreamSummary {
        row_count: rows,
        semantic_row_bytes: semantic_bytes,
        canonical_row_bytes: native_bytes,
        maximum_native_key_bytes,
        row_root,
    })
}
pub(super) fn check_publication(
    descriptor: &ProjectionSourceDescriptorV1,
    root: &ProjectionSourceRootV1,
    plan: &SourceDriverPlan<'_>,
    metadata: SourceCapturedMetadata,
    summary: SourceCheckpointStreamSummary,
) -> Result<(), SourceDriverError> {
    let reference = descriptor.reference();
    let expected_origin = riffdb_storage_api::projection_source_origin_commitment_v1(
        metadata.locator.database_id(),
        metadata.locator.allocation_history_incarnation(),
        metadata.origin,
    )
    .map_err(invalid)?;
    if reference.identity() != plan.identity()
        || reference.locator() != metadata.locator
        || reference.origin() != expected_origin
        || descriptor.applied_through() != metadata.frontier
        || descriptor.captured_source() != metadata.history
        || descriptor.row_count() != summary.row_count
        || descriptor.canonical_row_bytes() != summary.canonical_row_bytes
        || descriptor.row_root() != summary.row_root
        || root.reference() != reference
    {
        return Err(SourceDriverError::InputChanged);
    }
    Ok(())
}
