//! Fixed consuming checkpoint maintenance, prepared outside the primary gate.
use super::driver::SourceCheckpointCandidate;
use super::seal::SourceStageBrand;
use super::*;
use riffdb_storage_api::{
    ActiveCatalogPointerV1, AuthoritativeTransactionBindingV3, ChangelogHistoryStateV3,
    ProjectionCheckpointOrdinalAllocationV1, ProjectionCheckpointOrdinalAllocatorV1,
    ProjectionSourceDescriptorPartsV1, ProjectionSourceDescriptorV1, ProjectionSourceMaintenanceV1,
    ProjectionSourceRootV1,
};
use riffdb_types::{DatabaseId, ProjectionSourceLocatorV1, ProjectionSourceWorkReservationV1};
use std::mem::size_of;

/// Complete finite operation bound, including bounded abort/removal work.
pub const MAX_SOURCE_CHECKPOINT_STEPS: usize =
    2 * riffdb_storage_api::MAX_PROJECTION_SOURCE_ROWS as usize + 6;
/// Closed fixed physical role; no arbitrary source mutation callback exists.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceCheckpointOperationKind {
    /// Read actual allocator/root/preimages/version without consuming an ordinal.
    PrepareAllocation,
    /// Commit exactly one assigned ordinal and hidden candidate controls.
    Allocate,
    /// Copy at most128 rows/4MiB with strict exact progress.
    Copy,
    /// Publish only complete independently verified rows and expected chain.
    Publish,
    /// Definitely abort the exact hidden candidate under its native owner.
    Abort,
    /// Remove an eligible exact namespace in bounded steps.
    Collect,
}
/// Actual native allocation observation; the concrete type has private factories.
#[doc(hidden)]
pub trait SourceNativePreparedAllocationEvidence {
    /// Exact stored scalar from the same native read as expected prior selection.
    fn allocator(&self) -> ProjectionCheckpointOrdinalAllocatorV1;
    /// Exact currently selected root for this checked identity, or proven absence.
    fn prior_root(&self) -> Option<&ProjectionSourceRootV1>;
}
/// Readonly native preparation. A proposed binding is not an assignment or apply
/// permission; the commit coordinator must explicitly authorize its exact value.
#[doc(hidden)]
pub trait SourceNativePreparedStepEvidence {
    /// Actual same-access current receipt predecessor, independent of captured F.
    fn observed_history(&self) -> ChangelogHistoryStateV3;
    /// Pure proposed current binding for the prepared canonical controls/receipt.
    fn proposed_transaction_binding(&self) -> AuthoritativeTransactionBindingV3;
}
/// Actual definite native read-back for one fixed applied step.
#[doc(hidden)]
pub trait SourceNativeStepEvidence {
    /// Exact applied operation role, not a caller-selected status.
    fn operation(&self) -> SourceCheckpointOperationKind;
    /// Exact durable progress for Allocate/Copy; no numeric census substitute.
    fn progress(&self) -> Option<&ProjectionSourceMaintenanceV1>;
}
/// Actual known pre-submission refusal. Concrete native constructor must prove
/// no physical mutation was submitted; an unknown error cannot produce this.
#[doc(hidden)]
pub trait SourceNativePreSubmitRefusalEvidence {}
/// Read-only prepared canonical row; every graph/output retains actual admission.
#[doc(hidden)]
pub trait SourcePreparedCopyRow: SourceFrozenRow {
    /// Complete native Source key, excluding derived generation.
    fn native_key(&self) -> &[u8];
    /// Canonical complete SourceRow envelope for that exact key/context.
    fn canonical_value(&self) -> &[u8];
}
/// One bounded native preparation, not a durable copy/completion certificate.
#[doc(hidden)]
pub trait SourcePreparedCopyBatch {
    /// Its exact charged row representation.
    type Row: SourcePreparedCopyRow;
    /// At most128 rows/4MiB, retained until physical reconciliation.
    fn rows(&self) -> &[Self::Row];
}
/// Scalar pacing observation. It authorizes no mutation, source or timer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceGatePacingObservation {
    gate_cost_nanos: u64,
    rest_nanos: u64,
}
impl SourceGatePacingObservation {
    /// Records actual existing gate cost; actual owner charges even error exits.
    pub fn from_native_gate_cost(gate_cost_nanos: u64) -> Result<Self, SourceDriverError> {
        let rest_nanos = gate_cost_nanos
            .checked_mul(19)
            .ok_or(SourceDriverError::SourceUnavailable)?;
        Ok(Self {
            gate_cost_nanos,
            rest_nanos,
        })
    }
    /// Actual bounded scalar observation from the existing source gate.
    #[must_use]
    pub const fn gate_cost_nanos(self) -> u64 {
        self.gate_cost_nanos
    }
    /// Existing drained owner's required19x rest, never a competing timer.
    #[must_use]
    pub const fn rest_nanos(self) -> u64 {
        self.rest_nanos
    }
}
/// Fixed native port. Borrowed candidate/map/input never enters an actor queue.
#[doc(hidden)]
pub trait SourceCheckpointNativeMaintenancePort<'session, B: SourceCheckpointDriverBackend> {
    /// Actual cancellation state of this existing maintenance owner.
    fn cancellation_requested(&self) -> bool;
    /// Exactly one fixed native prepare or definite/unknown physical operation.
    fn apply<'candidate, 'stage, 'slot>(
        &mut self,
        instruction: SourceCheckpointInstruction<'candidate, 'stage, 'slot, 'session, B>,
    ) -> Result<SourceCheckpointResponse<'candidate, 'stage, 'slot, 'session, B>, SourceDriverError>
    where
        B: 'session,
        'session: 'slot,
        'slot: 'stage,
        'stage: 'candidate;
    /// Waits only through the existing joined drained owner using scalar permits.
    fn await_maintenance_turn(
        &mut self,
        observation: SourceGatePacingObservation,
    ) -> Result<(), SourceDriverError>;
}

struct MaintenanceState<'candidate, 'stage, 'slot, 'session, B: SourceCheckpointDriverBackend>
where
    B: 'session,
    'session: 'slot,
    'slot: 'stage,
    'stage: 'candidate,
{
    candidate: SourceCheckpointCandidate<'candidate, 'stage, 'slot, 'session, B>,
    preparation: Option<B::PreparationEvidence>,
    descriptor: Option<ProjectionSourceDescriptorV1>,
    before: Option<ProjectionSourceMaintenanceV1>,
    after: Option<ProjectionSourceMaintenanceV1>,
    assignment: Option<ProjectionCheckpointOrdinalAllocationV1>,
    transaction_assignment: Option<AuthoritativeTransactionBindingV3>,
    prepared_step: Option<B::PreparedStep>,
    batch: Option<B::CopyBatch>,
    hash: Option<crate::ProjectionSourceRowRootV1<'session>>,
    last_order_key: Vec<u8>,
    // Last: semantic/control graphs and copy buffers drop before this work.
    _work: ProjectionSourceWorkReservationV1,
}
/// Fields-private exact operation, retaining genuine candidate/stage/input owners.
pub struct SourceCheckpointInstruction<
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
    operation: SourceCheckpointOperationKind,
    state: MaintenanceState<'candidate, 'stage, 'slot, 'session, B>,
}
impl<'candidate, 'stage, 'slot, 'session, B: SourceCheckpointDriverBackend>
    SourceCheckpointCandidate<'candidate, 'stage, 'slot, 'session, B>
where
    B: 'session,
    'session: 'slot,
    'slot: 'stage,
    'stage: 'candidate,
{
    /// Only the private fully verified candidate can begin this consuming chain.
    pub fn into_prepare_instruction(
        self,
    ) -> Result<
        SourceCheckpointInstruction<'candidate, 'stage, 'slot, 'session, B>,
        SourceDriverError,
    > {
        let bound = ProjectionSourceMaintenanceV1::initial_work_bound_v1()
            .map_err(super::driver::invalid)?
            .checked_add(
                size_of::<MaintenanceState<'candidate, 'stage, 'slot, 'session, B>>() as u64,
            )
            .ok_or(SourceDriverError::SourceUnavailable)?;
        let work = self
            .plan
            .originating_budget()
            .reserve(bound)
            .map_err(|_| SourceDriverError::SourceUnavailable)?;
        Ok(SourceCheckpointInstruction {
            operation: SourceCheckpointOperationKind::PrepareAllocation,
            state: MaintenanceState {
                candidate: self,
                preparation: None,
                descriptor: None,
                before: None,
                after: None,
                assignment: None,
                transaction_assignment: None,
                prepared_step: None,
                batch: None,
                hash: None,
                last_order_key: Vec::with_capacity(riffdb_types::MAX_KEY_BYTES),
                _work: work,
            },
        })
    }
}
impl<'candidate, 'stage, 'slot, 'session, B: SourceCheckpointDriverBackend>
    SourceCheckpointInstruction<'candidate, 'stage, 'slot, 'session, B>
where
    B: 'session,
    'session: 'slot,
    'slot: 'stage,
    'stage: 'candidate,
{
    /// Closed fixed role; not permission to select a different native action.
    #[must_use]
    pub const fn operation(&self) -> SourceCheckpointOperationKind {
        self.operation
    }
    /// Actual captured full locator expectation, never allocated by a read.
    #[must_use]
    pub const fn captured_locator(&self) -> ProjectionSourceLocatorV1 {
        self.state.candidate.metadata.locator
    }
    /// Actual immutable source fence, never replaced by a later live head.
    #[must_use]
    pub const fn captured_history(&self) -> ChangelogHistoryStateV3 {
        self.state.candidate.metadata.history
    }
    /// Genuine captured catalog pointer; publication independently rechecks it.
    #[must_use]
    pub fn catalog_pointer(&self) -> &ActiveCatalogPointerV1 {
        self.state.candidate.plan.catalog_pointer()
    }
    /// Exact native frozen stage as a readonly opaque view, never mutable/raw.
    #[must_use]
    pub fn native_stage(&self) -> &B::FrozenStage<'stage, 'slot> {
        &*self.state.candidate.frozen
    }
    /// Matching actual private session/stage must be checked before native work.
    pub fn check_native_custody(
        &self,
        actual: &B::Brand,
        stage: &B::StageBrand,
    ) -> Result<(), SourceDriverError> {
        self.state.candidate.brand.check(actual)?;
        self.state.candidate.stage_brand.check(actual, stage)
    }
    /// Scoped descriptor, required for every mutating candidate operation.
    #[must_use]
    pub fn descriptor(&self) -> Option<&ProjectionSourceDescriptorV1> {
        self.state.descriptor.as_ref()
    }
    /// Exact complete independently evaluated row count.
    #[must_use]
    pub const fn expected_row_count(&self) -> u32 {
        self.state.candidate.summary.row_count
    }
    /// Exact complete durable native key/value bytes, independent of staging keys.
    #[must_use]
    pub const fn expected_canonical_row_bytes(&self) -> u64 {
        self.state.candidate.summary.canonical_row_bytes
    }
    /// Maximum complete native key length from the same private verified candidate.
    #[must_use]
    pub const fn expected_maximum_native_key_bytes(&self) -> usize {
        self.state.candidate.summary.maximum_native_key_bytes
    }
    /// Exact durable before-progress expected by this physical operation.
    #[must_use]
    pub fn before_progress(&self) -> Option<&ProjectionSourceMaintenanceV1> {
        self.state.before.as_ref()
    }
    /// Exact prepared progress after this physical operation.
    #[must_use]
    pub fn after_progress(&self) -> Option<&ProjectionSourceMaintenanceV1> {
        self.state.after.as_ref()
    }
    /// Sealed native allocation observation retained across the candidate chain.
    #[must_use]
    pub fn native_preparation(&self) -> Option<&B::PreparationEvidence> {
        self.state.preparation.as_ref()
    }
    /// Off-gate prepared native controls/preimages, retaining actual admission.
    #[must_use]
    pub fn prepared_step(&self) -> Option<&B::PreparedStep> {
        self.state.prepared_step.as_ref()
    }
    /// Actual canonical rows for this bounded Copy only.
    #[must_use]
    pub fn copy_batch(&self) -> Option<&B::CopyBatch> {
        self.state.batch.as_ref()
    }
    /// Checked assignment authorized by the commit-owned coordinator.
    #[must_use]
    pub const fn ordinal_assignment(&self) -> Option<ProjectionCheckpointOrdinalAllocationV1> {
        self.state.assignment
    }
    /// Actual native current receipt observation only while assignment is pending.
    /// It is never the captured input fence and cannot consume any sequence.
    #[must_use]
    pub fn observed_transaction_history(&self) -> Option<ChangelogHistoryStateV3> {
        if self.operation == SourceCheckpointOperationKind::PrepareAllocation
            || self.state.transaction_assignment.is_some()
        {
            return None;
        }
        self.state
            .prepared_step
            .as_ref()
            .map(SourceNativePreparedStepEvidence::observed_history)
    }
    /// Exact pure native proposal; it conveys no durable assignment permission.
    #[must_use]
    pub fn proposed_transaction_binding(&self) -> Option<AuthoritativeTransactionBindingV3> {
        self.state
            .prepared_step
            .as_ref()
            .map(SourceNativePreparedStepEvidence::proposed_transaction_binding)
    }
    /// The exact binding explicitly authorized by the consuming coordinator.
    #[must_use]
    pub const fn transaction_binding_assignment(
        &self,
    ) -> Option<AuthoritativeTransactionBindingV3> {
        self.state.transaction_assignment
    }
    /// Coordinator-only sequence authorization on this sealed native candidate.
    /// Today's Source roles preserve both application/administration frontiers;
    /// no additional audit/administration allocation is inferred from a proposal.
    pub fn authorize_transaction_binding(
        mut self,
        assigned: AuthoritativeTransactionBindingV3,
    ) -> Result<Self, SourceDriverError> {
        let observed = self
            .observed_transaction_history()
            .ok_or(SourceDriverError::InputChanged)?;
        let proposed = self
            .proposed_transaction_binding()
            .ok_or(SourceDriverError::InputChanged)?;
        let tail = observed.tail();
        if assigned != proposed
            || observed.lineage() != self.captured_history().lineage()
            || tail.sequence() < self.captured_history().tail().sequence()
            || assigned.database_id != observed.lineage().database_id()
            || assigned.history_incarnation != observed.lineage().history_incarnation()
            || assigned.predecessor != Some(tail.sequence())
            || tail.sequence().checked_next() != Some(assigned.sequence)
            || assigned.predecessor_frontier != tail.frontier()
            || assigned.covered_frontier != tail.frontier()
            || assigned.prior_history_hash != tail.history_hash()
        {
            return Err(SourceDriverError::InputChanged);
        }
        self.state.transaction_assignment = Some(assigned);
        Ok(self)
    }
    /// Only Allocate exposes the sealed actual observation to ordinal assignment.
    #[must_use]
    pub fn observed_ordinal(
        &self,
    ) -> Option<(ProjectionCheckpointOrdinalAllocatorV1, DatabaseId, u64)> {
        if self.operation != SourceCheckpointOperationKind::Allocate
            || self.state.assignment.is_some()
        {
            return None;
        }
        let before = self.state.preparation.as_ref()?.allocator();
        let locator = self.captured_locator();
        Some((
            before,
            locator.database_id(),
            locator.allocation_history_incarnation(),
        ))
    }
    /// The coordinator's exact allocation must match captured Next and observed scalar.
    pub fn authorize_ordinal(
        mut self,
        assigned: ProjectionCheckpointOrdinalAllocationV1,
    ) -> Result<Self, SourceDriverError> {
        let (before, database, incarnation) = self
            .observed_ordinal()
            .ok_or(SourceDriverError::InputChanged)?;
        let expected = before
            .allocate_one(database, incarnation)
            .map_err(|_| SourceDriverError::SourceUnavailable)?;
        if assigned != expected || assigned.locator() != self.captured_locator() {
            return Err(SourceDriverError::InputChanged);
        }
        self.state.assignment = Some(assigned);
        self.prepare_native_step()?;
        Ok(self)
    }
}

/// Off-gate fixed native control/receipt preparation request, scoped to this role.
pub struct SourcePrepareMaintenanceInstruction<
    'prepare,
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
    instruction: &'prepare SourceCheckpointInstruction<'candidate, 'stage, 'slot, 'session, B>,
    previous: Option<B::PreparedStep>,
}
impl<'prepare, 'candidate, 'stage, 'slot, 'session, B: SourceCheckpointDriverBackend>
    SourcePrepareMaintenanceInstruction<'prepare, 'candidate, 'stage, 'slot, 'session, B>
where
    B: 'session,
    'session: 'slot,
    'slot: 'stage,
    'stage: 'candidate,
{
    /// Readonly role/candidate/preimage views; no mutable backend escapes.
    #[must_use]
    pub fn instruction(
        &self,
    ) -> &SourceCheckpointInstruction<'candidate, 'stage, 'slot, 'session, B> {
        self.instruction
    }
    /// Moves the previous intact native owner only within this same candidate's
    /// scoped preparation. Native may retain only a completed physical Publish
    /// observation that validates against the new request's actual stage/epoch;
    /// all old intent/quota/history graphs must drop before current preparation.
    pub fn take_previous(
        &mut self,
        actual: &B::Brand,
        stage: &B::StageBrand,
    ) -> Result<Option<B::PreparedStep>, SourceDriverError> {
        self.check_before_prepare(actual, stage)?;
        Ok(self.previous.take())
    }
    /// Check actual source/stage before native reads, encoding or admission.
    pub fn check_before_prepare(
        &self,
        actual: &B::Brand,
        stage: &B::StageBrand,
    ) -> Result<(), SourceDriverError> {
        self.instruction.check_native_custody(actual, stage)
    }
    /// Attaches only the actual prepared owner while consuming this request.
    pub fn attach(
        self,
        actual: &B::Brand,
        stage: &B::StageBrand,
        prepared: B::PreparedStep,
    ) -> Result<SourcePreparedMaintenanceEvidence<'session, 'stage, B>, SourceDriverError> {
        self.check_before_prepare(actual, stage)?;
        Ok(SourcePreparedMaintenanceEvidence {
            brand: self.instruction.state.candidate.stage_brand.next(),
            prepared,
        })
    }
}
/// Non-reusable actual native preparation for its exact session/stage.
pub struct SourcePreparedMaintenanceEvidence<'session, 'stage, B: SourceCheckpointDriverBackend> {
    brand: SourceStageBrand<'session, 'stage, B>,
    prepared: B::PreparedStep,
}
/// Off-gate bounded frozen-row copy preparation for the same private candidate.
pub struct SourcePrepareCopyInstruction<
    'prepare,
    'session,
    'stage,
    B: SourceCheckpointDriverBackend,
> {
    brand: SourceStageBrand<'session, 'stage, B>,
    descriptor: &'prepare ProjectionSourceDescriptorV1,
    before: &'prepare ProjectionSourceMaintenanceV1,
    last_order_key: Option<&'prepare [u8]>,
}
impl<'prepare, 'session, 'stage, B: SourceCheckpointDriverBackend>
    SourcePrepareCopyInstruction<'prepare, 'session, 'stage, B>
{
    /// Exact immutable candidate descriptor for canonical row checks.
    #[must_use]
    pub fn descriptor(&self) -> &ProjectionSourceDescriptorV1 {
        self.descriptor
    }
    /// Exact durable before-progress, not a caller-selected resume watermark.
    #[must_use]
    pub fn before_progress(&self) -> &ProjectionSourceMaintenanceV1 {
        self.before
    }
    /// Existing canonical staging key of the last complete copied row, if any.
    #[must_use]
    pub fn last_order_key(&self) -> Option<&[u8]> {
        self.last_order_key
    }
    /// Check exact captured instance and physical frozen map before reading.
    pub fn check_before_prepare(
        &self,
        actual: &B::Brand,
        stage: &B::StageBrand,
    ) -> Result<(), SourceDriverError> {
        self.brand.check(actual, stage)
    }
    /// Attach actual charged canonical batch only through the consumed request.
    pub fn attach(
        self,
        actual: &B::Brand,
        stage: &B::StageBrand,
        batch: B::CopyBatch,
    ) -> Result<SourcePreparedCopyEvidence<'session, 'stage, B>, SourceDriverError> {
        self.check_before_prepare(actual, stage)?;
        Ok(SourcePreparedCopyEvidence {
            brand: self.brand,
            batch,
        })
    }
}
/// Exact stage-bound prepared batch; no durable progress or publication authority.
pub struct SourcePreparedCopyEvidence<'session, 'stage, B: SourceCheckpointDriverBackend> {
    brand: SourceStageBrand<'session, 'stage, B>,
    batch: B::CopyBatch,
}
/// Native receives the physical prepared owner by move and retains only this
/// sealed continuation for attaching actual definite/stale/published evidence.
pub struct SourceNativeStepContinuation<
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
    instruction: SourceCheckpointInstruction<'candidate, 'stage, 'slot, 'session, B>,
}
enum NativeResponse<B: SourceCheckpointDriverBackend> {
    Prepared(B::PreparationEvidence),
    Applied(B::StepEvidence),
    Published(B::StepEvidence, B::PublicationEvidence),
    Stale(B::PreSubmitRefusalEvidence),
}
/// Consumed exact native response. Its factories are tied to the matching role.
pub struct SourceCheckpointResponse<
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
    continuation: SourceNativeStepContinuation<'candidate, 'stage, 'slot, 'session, B>,
    response: NativeResponse<B>,
    pacing: Option<SourceGatePacingObservation>,
}
/// The only possible next fixed instruction or genuine native publication.
#[allow(clippy::large_enum_variant)]
pub enum SourceCheckpointContinuation<
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
    /// Exact monotone next operation, retaining the actual candidate.
    Instruction(SourceCheckpointInstruction<'candidate, 'stage, 'slot, 'session, B>),
    /// Exact native read-back attached to its consumed publication role.
    Published(SourceNativePublicationConfirmation<'session, B>),
}
impl<'candidate, 'stage, 'slot, 'session, B: SourceCheckpointDriverBackend>
    SourceCheckpointInstruction<'candidate, 'stage, 'slot, 'session, B>
where
    B: 'session,
    'session: 'slot,
    'slot: 'stage,
    'stage: 'candidate,
{
    /// Moves the actual prepared native owner without exposing raw mutable state.
    pub fn into_native_operation(
        mut self,
        actual: &B::Brand,
        stage: &B::StageBrand,
    ) -> Result<
        (
            Option<B::PreparedStep>,
            SourceNativeStepContinuation<'candidate, 'stage, 'slot, 'session, B>,
        ),
        SourceDriverError,
    > {
        self.check_native_custody(actual, stage)?;
        let prepared = self.state.prepared_step.take();
        if (self.operation == SourceCheckpointOperationKind::PrepareAllocation)
            != prepared.is_none()
        {
            return Err(SourceDriverError::InputChanged);
        }
        if self.operation == SourceCheckpointOperationKind::Allocate
            && self.state.assignment.is_none()
        {
            return Err(SourceDriverError::InputChanged);
        }
        match prepared.as_ref() {
            None if self.state.transaction_assignment.is_none() => {}
            Some(step)
                if self.state.transaction_assignment
                    == Some(step.proposed_transaction_binding()) => {}
            _ => return Err(SourceDriverError::InputChanged),
        }
        Ok((prepared, SourceNativeStepContinuation { instruction: self }))
    }
    fn prepare_native_step(&mut self) -> Result<(), SourceDriverError> {
        // Every current-head preparation, including a known stale retry, needs
        // a NEW consuming coordinator assignment. Old permission cannot carry.
        self.state.transaction_assignment = None;
        // The old intact owner moves only through this same candidate's scoped
        // native preparation. The actual Native owner drops old canonical
        // intent/quota/history before constructing a new current-head proposal.
        // Candidate/F/plan/input/verified rows remain unchanged across a stale head.
        let previous = self.state.prepared_step.take();
        let prepared = B::prepare_maintenance_step(
            self.state.candidate.brand.native(),
            SourcePrepareMaintenanceInstruction {
                instruction: self,
                previous,
            },
        )?;
        prepared.brand.check(
            self.state.candidate.brand.native(),
            self.state.candidate.stage_brand.native_stage(),
        )?;
        self.state.prepared_step = Some(prepared.prepared);
        Ok(())
    }
}
impl<'candidate, 'stage, 'slot, 'session, B: SourceCheckpointDriverBackend>
    SourceNativeStepContinuation<'candidate, 'stage, 'slot, 'session, B>
where
    B: 'session,
    'session: 'slot,
    'slot: 'stage,
    'stage: 'candidate,
{
    fn check(&self, actual: &B::Brand, stage: &B::StageBrand) -> Result<(), SourceDriverError> {
        self.instruction.check_native_custody(actual, stage)
    }
    /// Readonly view of the same consumed instruction during native apply.
    /// Its candidate/stage/input owners remain here; no prepared write or brand
    /// can be detached or reconstructed through this view.
    #[must_use]
    pub fn instruction(
        &self,
    ) -> &SourceCheckpointInstruction<'candidate, 'stage, 'slot, 'session, B> {
        &self.instruction
    }
    /// Attach a real native readonly allocation observation; no ordinal consumed.
    pub fn prepared(
        self,
        actual: &B::Brand,
        stage: &B::StageBrand,
        evidence: B::PreparationEvidence,
    ) -> Result<SourceCheckpointResponse<'candidate, 'stage, 'slot, 'session, B>, SourceDriverError>
    {
        self.check(actual, stage)?;
        if self.instruction.operation != SourceCheckpointOperationKind::PrepareAllocation {
            return Err(SourceDriverError::InputChanged);
        }
        Ok(SourceCheckpointResponse {
            continuation: self,
            response: NativeResponse::Prepared(evidence),
            pacing: None,
        })
    }
    /// Attach exact native definite read-back; plain commit Result is insufficient.
    pub fn applied(
        self,
        actual: &B::Brand,
        stage: &B::StageBrand,
        evidence: B::StepEvidence,
        pacing: SourceGatePacingObservation,
    ) -> Result<SourceCheckpointResponse<'candidate, 'stage, 'slot, 'session, B>, SourceDriverError>
    {
        self.check(actual, stage)?;
        if !matches!(
            self.instruction.operation,
            SourceCheckpointOperationKind::Allocate
                | SourceCheckpointOperationKind::Copy
                | SourceCheckpointOperationKind::Abort
                | SourceCheckpointOperationKind::Collect
        ) || evidence.operation() != self.instruction.operation
        {
            return Err(SourceDriverError::InputChanged);
        }
        Ok(SourceCheckpointResponse {
            continuation: self,
            response: NativeResponse::Applied(evidence),
            pacing: Some(pacing),
        })
    }
    /// Attach actual final SourceRoot/object reconciliation, never a nominal Ok.
    pub fn published(
        self,
        actual: &B::Brand,
        stage: &B::StageBrand,
        step: B::StepEvidence,
        evidence: B::PublicationEvidence,
        pacing: SourceGatePacingObservation,
    ) -> Result<SourceCheckpointResponse<'candidate, 'stage, 'slot, 'session, B>, SourceDriverError>
    {
        self.check(actual, stage)?;
        if self.instruction.operation != SourceCheckpointOperationKind::Publish
            || step.operation() != SourceCheckpointOperationKind::Publish
        {
            return Err(SourceDriverError::InputChanged);
        }
        Ok(SourceCheckpointResponse {
            continuation: self,
            response: NativeResponse::Published(step, evidence),
            pacing: Some(pacing),
        })
    }
    /// Only an actual known pre-submit proof may request same-candidate reprepare.
    /// Unknown apply never enters this factory or loses native input/quota custody.
    pub fn known_pre_submit_stale(
        mut self,
        actual: &B::Brand,
        stage: &B::StageBrand,
        evidence: B::PreSubmitRefusalEvidence,
        previous: Option<B::PreparedStep>,
        pacing: SourceGatePacingObservation,
    ) -> Result<SourceCheckpointResponse<'candidate, 'stage, 'slot, 'session, B>, SourceDriverError>
    {
        self.check(actual, stage)?;
        if self.instruction.operation == SourceCheckpointOperationKind::PrepareAllocation
            || self.instruction.state.prepared_step.is_some()
        {
            return Err(SourceDriverError::InputChanged);
        }
        let assigned = self
            .instruction
            .state
            .transaction_assignment
            .ok_or(SourceDriverError::InputChanged)?;
        if previous
            .as_ref()
            .is_some_and(|step| step.proposed_transaction_binding() != assigned)
        {
            return Err(SourceDriverError::InputChanged);
        }
        // Some is the actual whole owner refused BEFORE usage.begin. After a
        // raw abort/known-before resolution consumes usage and advances epoch,
        // Native returns None; it must never reconstitute that old owner.
        self.instruction.state.prepared_step = previous;
        Ok(SourceCheckpointResponse {
            continuation: self,
            response: NativeResponse::Stale(evidence),
            pacing: Some(pacing),
        })
    }
}

/// Maximum actual known-pre-submit stale preparations per one durable step.
/// This scheduling bound grants no keep-up claim and never converts uncertainty.
pub const MAX_SOURCE_STEP_REPREPARES: usize = 8;
impl<'candidate, 'stage, 'slot, 'session, B: SourceCheckpointDriverBackend>
    SourceCheckpointResponse<'candidate, 'stage, 'slot, 'session, B>
where
    B: 'session,
    'session: 'slot,
    'slot: 'stage,
    'stage: 'candidate,
{
    /// Every native gate exit is charged even if no success response is returned.
    #[must_use]
    pub const fn pacing_observation(&self) -> Option<SourceGatePacingObservation> {
        self.pacing
    }
    /// Only the actual native known-pre-submit proof enables this classification.
    #[must_use]
    pub fn is_known_pre_submit_stale(&self) -> bool {
        matches!(self.response, NativeResponse::Stale(_))
    }
    /// Constructs only the exact next bounded operation; all work here is off-gate.
    pub fn prepare_next(
        self,
    ) -> Result<
        SourceCheckpointContinuation<'candidate, 'stage, 'slot, 'session, B>,
        SourceDriverError,
    > {
        let mut instruction = self.continuation.instruction;
        match self.response {
            NativeResponse::Prepared(preparation) => {
                instruction.initialize_allocation(preparation)?;
                Ok(SourceCheckpointContinuation::Instruction(instruction))
            }
            NativeResponse::Stale(_proof) => {
                // No candidate/descriptor/progress/row/hash is recaptured or
                // re-evaluated. Only the transaction-current step binding changes.
                instruction.prepare_native_step()?;
                Ok(SourceCheckpointContinuation::Instruction(instruction))
            }
            NativeResponse::Applied(evidence) => {
                match instruction.operation {
                    SourceCheckpointOperationKind::Allocate
                    | SourceCheckpointOperationKind::Copy => {
                        if evidence.progress() != instruction.state.after.as_ref() {
                            return Err(SourceDriverError::InputChanged);
                        }
                        instruction.state.before = instruction.state.after.take();
                        drop(instruction.state.batch.take());
                        instruction.prepare_copy_or_publication()?;
                        Ok(SourceCheckpointContinuation::Instruction(instruction))
                    }
                    // Recovery/abort/GC use distinct actual native producers.
                    // The new-checkpoint entry cannot synthesize those roles.
                    _ => Err(SourceDriverError::InputChanged),
                }
            }
            NativeResponse::Published(_step, evidence) => {
                let descriptor = instruction
                    .state
                    .descriptor
                    .as_ref()
                    .ok_or(SourceDriverError::InputChanged)?;
                if evidence.descriptor() != descriptor
                    || evidence.root().reference() != descriptor.reference()
                {
                    return Err(SourceDriverError::InputChanged);
                }
                let candidate = &instruction.state.candidate;
                super::driver::check_publication(
                    evidence.descriptor(),
                    evidence.root(),
                    candidate.plan,
                    candidate.metadata,
                    candidate.summary,
                )?;
                let confirmation = SourceNativePublicationConfirmation {
                    brand: candidate.brand.next(),
                    evidence,
                };
                // Only actual final evidence permits destruction of the private
                // candidate chain. Native unknown/known-active slots are never
                // reconciled by these destructors or a numeric usage observation.
                drop(instruction);
                Ok(SourceCheckpointContinuation::Published(confirmation))
            }
        }
    }
}
impl<'candidate, 'stage, 'slot, 'session, B: SourceCheckpointDriverBackend>
    SourceCheckpointInstruction<'candidate, 'stage, 'slot, 'session, B>
where
    B: 'session,
    'session: 'slot,
    'slot: 'stage,
    'stage: 'candidate,
{
    fn initialize_allocation(
        &mut self,
        preparation: B::PreparationEvidence,
    ) -> Result<(), SourceDriverError> {
        if self.operation != SourceCheckpointOperationKind::PrepareAllocation
            || self.state.preparation.is_some()
        {
            return Err(SourceDriverError::InputChanged);
        }
        let candidate = &self.state.candidate;
        let locator = candidate.metadata.locator;
        let assignment = preparation
            .allocator()
            .allocate_one(
                locator.database_id(),
                locator.allocation_history_incarnation(),
            )
            .map_err(|_| SourceDriverError::SourceUnavailable)?;
        if assignment.locator() != locator {
            return Err(SourceDriverError::InputChanged);
        }
        let prior = preparation.prior_root();
        if prior.is_some_and(|root| root.reference().identity() != candidate.plan.identity()) {
            return Err(SourceDriverError::InputChanged);
        }
        let origin = riffdb_storage_api::projection_source_origin_commitment_v1(
            locator.database_id(),
            locator.allocation_history_incarnation(),
            candidate.metadata.origin,
        )
        .map_err(super::driver::invalid)?;
        let descriptor = ProjectionSourceDescriptorV1::new(ProjectionSourceDescriptorPartsV1 {
            identity: candidate.plan.identity().clone(),
            locator,
            applied_through: candidate.metadata.frontier,
            captured_source: candidate.metadata.history,
            origin,
            prior_source_digest: prior.map(|root| root.reference().digest()),
            row_count: candidate.summary.row_count,
            canonical_row_bytes: candidate.summary.canonical_row_bytes,
            row_root: candidate.summary.row_root,
        })
        .map_err(super::driver::invalid)?;
        let progress = ProjectionSourceMaintenanceV1::initial(descriptor.clone(), prior.cloned())
            .map_err(super::driver::invalid)?;
        let plan: &'session SourceDriverPlan<'session> = candidate.plan;
        let hash = crate::ProjectionSourceRowRootV1::new(
            plan.identity(),
            Some(locator),
            candidate.metadata.frontier,
            candidate.summary.row_count,
            candidate.summary.semantic_row_bytes,
        )
        .map_err(super::driver::invalid)?;
        self.state.descriptor = Some(descriptor);
        self.state.after = Some(progress);
        self.state.preparation = Some(preparation);
        self.state.hash = Some(hash);
        self.operation = SourceCheckpointOperationKind::Allocate;
        Ok(())
    }
    fn prepare_copy_or_publication(&mut self) -> Result<(), SourceDriverError> {
        let before = self
            .state
            .before
            .as_ref()
            .ok_or(SourceDriverError::InputChanged)?;
        if before.copy_complete() {
            let root = self
                .state
                .hash
                .take()
                .ok_or(SourceDriverError::InputChanged)?
                .finish()
                .map_err(super::driver::invalid)?;
            if root != self.state.candidate.summary.row_root {
                return Err(SourceDriverError::InputChanged);
            }
            self.state.after = None;
            self.operation = SourceCheckpointOperationKind::Publish;
            return self.prepare_native_step();
        }
        let descriptor = self
            .state
            .descriptor
            .as_ref()
            .ok_or(SourceDriverError::InputChanged)?;
        let candidate = &mut self.state.candidate;
        let batch = B::prepare_copy_batch(
            candidate.frozen,
            candidate.brand.native(),
            SourcePrepareCopyInstruction {
                brand: candidate.stage_brand.next(),
                descriptor,
                before,
                last_order_key: (!self.state.last_order_key.is_empty())
                    .then_some(self.state.last_order_key.as_slice()),
            },
        )?;
        batch.brand.check(
            candidate.brand.native(),
            candidate.stage_brand.native_stage(),
        )?;
        let rows = batch.batch.rows();
        if rows.is_empty()
            || rows.len() > usize::from(riffdb_storage_api::MAX_PROJECTION_SOURCE_STEP_ROWS)
        {
            return Err(SourceDriverError::InputChanged);
        }
        let mut canonical_pairs =
            [(&[][..], &[][..]); riffdb_storage_api::MAX_PROJECTION_SOURCE_STEP_ROWS as usize];
        let mut step_bytes = 0_u64;
        for (index, row) in rows.iter().enumerate() {
            let value = row.row();
            if value.identity() != descriptor.reference().identity()
                || value.locator() != descriptor.reference().locator()
                || value.last_changed_sequence() > descriptor.applied_through()
                || row.order_key().components() != value.group_values()
                || row.native_key_bytes() != row.native_key().len()
                || row.encoded_content_charge().get() != row.canonical_value().len()
            {
                return Err(SourceDriverError::InputChanged);
            }
            step_bytes = step_bytes
                .checked_add((row.native_key().len() + row.canonical_value().len()) as u64)
                .ok_or(SourceDriverError::InputChanged)?;
            if step_bytes > riffdb_storage_api::MAX_PROJECTION_SOURCE_STEP_BYTES {
                return Err(SourceDriverError::InputChanged);
            }
            let capacity = crate::projection_source_row_scratch_bytes_v1(value)
                .map_err(super::driver::invalid)?;
            let bound = riffdb_types::owned_allocation_capacity_bound_v1(
                capacity,
                std::mem::align_of::<u8>(),
            )
            .map_err(|_| SourceDriverError::SourceUnavailable)?;
            let _scratch_work = candidate
                .plan
                .originating_budget()
                .reserve(bound)
                .map_err(|_| SourceDriverError::SourceUnavailable)?;
            let mut scratch = Vec::with_capacity(capacity);
            self.state
                .hash
                .as_mut()
                .ok_or(SourceDriverError::InputChanged)?
                .append(row.order_key(), value, &mut scratch)
                .map_err(super::driver::invalid)?;
            canonical_pairs[index] = (row.native_key(), row.canonical_value());
        }
        let after = before
            .after_copy_step(before, &canonical_pairs[..rows.len()])
            .map_err(super::driver::invalid)?;
        if after.copied_rows() <= before.copied_rows()
            || after.copied_canonical_row_bytes() <= before.copied_canonical_row_bytes()
        {
            return Err(SourceDriverError::InputChanged);
        }
        let last = rows
            .last()
            .ok_or(SourceDriverError::InputChanged)?
            .order_key()
            .as_bytes();
        if last.len() > riffdb_types::MAX_KEY_BYTES {
            return Err(SourceDriverError::InputChanged);
        }
        self.state.last_order_key.clear();
        self.state.last_order_key.extend_from_slice(last);
        self.state.after = Some(after);
        self.state.batch = Some(batch.batch);
        self.operation = SourceCheckpointOperationKind::Copy;
        self.prepare_native_step()
    }
}
