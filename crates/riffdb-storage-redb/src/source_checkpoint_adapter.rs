//! Native opaque Source custody adapter. No projection evaluation or IR access.
use crate::{
    RedbOperationalPorts,
    source_evaluation_work::SourceStageSlotFactory,
    store::{
        OriginalPrefixRetentionLease, OriginalSourceCursor,
        source_usage_domain::{
            NativePreparedSourceUsage, PreparedSourceHistoryControls, SourceHistoryApplyFailure,
            SourceHistoryNoSubmitYield, SourceHistoryPreinstallFailure, SourceHistorySubmitted,
            prepare_native_source_usage,
        },
    },
};
use riffdb_projection::source_driver::{
    SourceCheckpointBackend, SourceCheckpointDriverBackend, SourceCheckpointInstruction,
    SourceCheckpointNativeMaintenancePort, SourceCheckpointOperationKind, SourceCheckpointResponse,
    SourceCheckpointSession, SourceCommandResponse, SourceDriverError, SourceDriverPlan,
    SourceGatePacingObservation, SourceNativePreSubmitRefusalEvidence,
    SourceNativePreparedStepEvidence, SourceNativePublicationEvidence,
    SourcePrepareCopyInstruction, SourcePrepareMaintenanceInstruction, SourcePreparedCopyEvidence,
    SourcePreparedMaintenanceEvidence, SourceReadInstruction, SourceReplayInputBackend,
    SourceRewindEvidence, SourceRewindInstruction,
};
use riffdb_storage_api::{
    AuthoritativeTransactionBindingV3, ChangelogHistoryStateV3, StorageErrorKind,
};
use std::{
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
    time::Instant,
};

mod copy;
pub(crate) mod intent;
pub(crate) mod maintenance;
mod preparation;
pub(crate) mod publication;
mod stage;

pub(crate) use intent::{NativeSourceStepIntent, NativeVerifiedSourceStep};
pub(crate) use maintenance::NativeSourceCheckpointRequest;

// No IDs-only or reusable proof constructor. This actual reference is never
// exposed through backend, instruction, candidate or coordinator getters.
#[doc(hidden)]
pub struct NativeSourceSessionBrand<'work> {
    ports: &'work RedbOperationalPorts,
    prefix: &'work OriginalPrefixRetentionLease,
    plan: &'work SourceDriverPlan<'work>,
    private_parent: &'work Path,
    _nonzero: u8,
}
#[doc(hidden)]
pub struct NativeSourceStageBrand {
    pass: u8,
}
#[doc(hidden)]
pub struct NativeSourceInput<'work> {
    cursor: Option<OriginalSourceCursor<'work>>,
    ports: &'work RedbOperationalPorts,
    prefix: &'work OriginalPrefixRetentionLease,
}
// Retain both actual input borrows even when no cursor is open or used again.
impl Drop for NativeSourceInput<'_> {
    fn drop(&mut self) {}
}

// Brand, input and one-slot factory are disjoint fields, avoiding a self-reference
// or a second worker claim. The projection driver borrows this whole owner once.
#[doc(hidden)]
pub struct NativeSourceCheckpointBackend<'work> {
    brand: NativeSourceSessionBrand<'work>,
    input: NativeSourceInput<'work>,
    stages: NativeSourceFactory<'work>,
    maintenance: NativeSourceMaintenanceState<'work>,
}
#[doc(hidden)]
pub struct NativeSourceFactory<'work> {
    inner: SourceStageSlotFactory<'work>,
}
struct NativeSourceMaintenanceState<'work> {
    // The closed Source usage producer/physical reconciler owns this state.
    // No public mutation or confirmation constructor is available here.
    prefix: &'work OriginalPrefixRetentionLease,
    cancellation: &'work AtomicBool,
}
#[doc(hidden)]
pub struct NativeSourceMaintenancePort<'session, 'work> {
    brand: &'session NativeSourceSessionBrand<'work>,
    state: &'session mut NativeSourceMaintenanceState<'work>,
}
#[doc(hidden)]
pub struct NativeSourcePreSubmitRefusalEvidence {
    _private: (),
}
impl SourceNativePreSubmitRefusalEvidence for NativeSourcePreSubmitRefusalEvidence {}

#[allow(clippy::large_enum_variant)]
enum PreparedStepKind<'work> {
    Progress(intent::step_evidence::NativePreparedStepProgress<'work>),
    Publish(intent::publication_recipe::NativePreparedPublication<'work>),
}
impl<'work> PreparedStepKind<'work> {
    fn validate_for<'request, 'candidate, 'stage, 'slot, 'session>(
        &self,
        request: &NativeSourceCheckpointRequest<
            'request,
            'candidate,
            'stage,
            'slot,
            'session,
            'work,
        >,
    ) -> Result<(), SourceDriverError>
    where
        'work: 'session,
        'session: 'slot,
        'slot: 'stage,
        'stage: 'candidate,
    {
        match self {
            Self::Progress(_) => Ok(()),
            Self::Publish(publication) => Ok(publication.validate_for(request)?),
        }
    }
}

#[doc(hidden)]
pub struct NativeSourcePreparedStep<'work> {
    intent: NativeSourceStepIntent,
    usage: NativePreparedSourceUsage<'work>,
    controls: PreparedSourceHistoryControls<'work>,
    step: PreparedStepKind<'work>,
}
impl SourceNativePreparedStepEvidence for NativeSourcePreparedStep<'_> {
    fn observed_history(&self) -> ChangelogHistoryStateV3 {
        self.intent.predecessor_history()
    }
    fn proposed_transaction_binding(&self) -> AuthoritativeTransactionBindingV3 {
        self.intent.receipt().binding()
    }
}

#[doc(hidden)]
pub struct NativeSourcePublicationEvidence {
    values: riffdb_types::ProjectionSourceWorkAllocationV1<
        intent::publication_recipe::NativeSourcePublicationValues,
    >,
}
impl SourceNativePublicationEvidence for NativeSourcePublicationEvidence {
    fn descriptor(&self) -> &riffdb_storage_api::ProjectionSourceDescriptorV1 {
        self.values.descriptor()
    }
    fn root(&self) -> &riffdb_storage_api::ProjectionSourceRootV1 {
        self.values.root()
    }
}
#[cfg(test)]
impl<'work> NativeSourceCheckpointBackend<'work> {
    pub(crate) fn capture(
        ports: &'work RedbOperationalPorts,
        work: &'work mut crate::source_evaluation_work::SourceEvaluationWorkOwner,
        plan: &'work SourceDriverPlan<'work>,
        private_parent: &'work Path,
    ) -> Result<Self, SourceDriverError> {
        let (prefix, stages) = work.split_session();
        if !prefix.belongs_to_ports(ports)
            || !plan.belongs_to(prefix.originating_budget())
            || prefix.captured_active_catalog_pointer() != Some(plan.catalog_pointer())
            || prefix.captured_native_origin().is_none()
            || prefix.tentative_source_locator().is_none()
        {
            return Err(SourceDriverError::SourceUnavailable);
        }
        let cancellation: &'work AtomicBool = stages.cancellation();
        Ok(Self {
            brand: NativeSourceSessionBrand {
                ports,
                prefix,
                plan,
                private_parent,
                _nonzero: 1,
            },
            input: NativeSourceInput {
                cursor: None,
                ports,
                prefix,
            },
            stages: NativeSourceFactory { inner: stages },
            maintenance: NativeSourceMaintenanceState {
                prefix,
                cancellation,
            },
        })
    }
}
impl<'work> SourceCheckpointBackend for NativeSourceCheckpointBackend<'work> {
    type Brand = NativeSourceSessionBrand<'work>;
    type StageBrand = NativeSourceStageBrand;
}
impl<'work> SourceReplayInputBackend for NativeSourceCheckpointBackend<'work> {
    type Input = NativeSourceInput<'work>;
    fn rewind<'session>(
        input: &mut Self::Input,
        actual: &Self::Brand,
        request: SourceRewindInstruction<'session, Self>,
    ) -> Result<SourceRewindEvidence<'session, Self>, SourceDriverError> {
        request.check_before_rewind(actual)?;
        check_input(input, actual)?;
        drop(input.cursor.take());
        input.cursor = Some(OriginalSourceCursor::open(input.ports, input.prefix)?);
        request.rewound(actual)
    }
    fn read_command<'command, 'session>(
        input: &'command mut Self::Input,
        actual: &Self::Brand,
        request: SourceReadInstruction<'session, Self>,
    ) -> Result<SourceCommandResponse<'command, 'session, Self>, SourceDriverError> {
        request.check_before_read(actual)?;
        check_input(input, actual)?;
        let cursor = input
            .cursor
            .as_mut()
            .ok_or(SourceDriverError::IncompleteInput)?;
        match cursor.next_commit()? {
            Some(command) => request.command(actual, command),
            None => request.exact_end(actual),
        }
    }
}
fn check_input(
    input: &NativeSourceInput<'_>,
    actual: &NativeSourceSessionBrand<'_>,
) -> Result<(), SourceDriverError> {
    if !std::ptr::eq(input.prefix, actual.prefix)
        || !std::ptr::eq(input.ports, actual.ports)
        || !input.prefix.belongs_to_ports(actual.ports)
    {
        return Err(SourceDriverError::ForeignCustody);
    }
    Ok(())
}

impl<'work> SourceCheckpointDriverBackend for NativeSourceCheckpointBackend<'work> {
    type MaintenancePort<'session>
        = NativeSourceMaintenancePort<'session, 'work>
    where
        Self: 'session;
    type PreparationEvidence = preparation::NativeSourceAllocationObservation<'work>;
    type StepEvidence = intent::step_evidence::NativeSourceAppliedStepEvidence;
    type PreSubmitRefusalEvidence = NativeSourcePreSubmitRefusalEvidence;
    type PreparedStep = NativeSourcePreparedStep<'work>;
    type CopyBatch = copy::NativeSourceCopyBatch;
    type PublicationEvidence = NativeSourcePublicationEvidence;

    fn prepare_maintenance_step<'prepare, 'candidate, 'stage, 'slot, 'session>(
        actual: &Self::Brand,
        mut request: SourcePrepareMaintenanceInstruction<
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
        'stage: 'candidate,
    {
        let stage = request.instruction().native_stage().brand;
        let previous = request.take_previous(actual, stage)?;
        let checked = maintenance::checked_request(actual, request.instruction())?;
        if checked.prepared_step().is_some() {
            return Err(SourceDriverError::InputChanged);
        }
        if let Some(previous) = previous {
            previous.step.validate_for(&checked)?;
            drop(previous);
        }
        let (intent, step) = match checked.operation() {
            SourceCheckpointOperationKind::Allocate => {
                let intent = intent::prepare_allocation_intent(&checked)?;
                let progress = intent::step_evidence::prepare_step_progress(&checked, &intent)?;
                (intent, PreparedStepKind::Progress(progress))
            }
            SourceCheckpointOperationKind::Copy => {
                let intent = intent::prepare_copy_intent(&checked)?;
                let progress = intent::step_evidence::prepare_step_progress(&checked, &intent)?;
                (intent, PreparedStepKind::Progress(progress))
            }
            SourceCheckpointOperationKind::Publish => {
                let observation = publication::verify_publication_copy(&checked)?;
                let publication =
                    intent::publication_recipe::prepare_publication_intent(&checked, observation)?;
                let intent = publication.intent().clone();
                (intent, PreparedStepKind::Publish(publication))
            }
            _ => return Err(SourceDriverError::InputChanged),
        };
        if intent.is_final_abort() {
            return Err(SourceDriverError::InputChanged);
        }
        let usage = prepare_native_source_usage(&checked, intent.clone())?;
        let controls = PreparedSourceHistoryControls::prepare(&checked, intent.clone())?;
        drop(checked);
        request.attach(
            actual,
            stage,
            NativeSourcePreparedStep {
                intent,
                usage,
                controls,
                step,
            },
        )
    }

    fn prepare_copy_batch<'prepare, 'session, 'stage, 'slot>(
        frozen: &mut Self::FrozenStage<'stage, 'slot>,
        actual: &Self::Brand,
        request: SourcePrepareCopyInstruction<'prepare, 'session, 'stage, Self>,
    ) -> Result<SourcePreparedCopyEvidence<'session, 'stage, Self>, SourceDriverError>
    where
        Self: 'session,
        'session: 'slot,
        'slot: 'stage,
    {
        copy::prepare_copy_batch(frozen, actual, request)
    }

    fn session(&mut self) -> Result<SourceCheckpointSession<'_, Self>, SourceDriverError> {
        if !std::ptr::eq(self.maintenance.prefix, self.brand.prefix) {
            return Err(SourceDriverError::ForeignCustody);
        }
        let prefix = self.brand.prefix;
        let metadata = (
            prefix.captured_history(),
            prefix
                .tentative_source_locator()
                .ok_or(SourceDriverError::SourceUnavailable)?,
            prefix
                .captured_native_origin()
                .ok_or(SourceDriverError::SourceUnavailable)?,
        );
        SourceCheckpointSession::attach(
            &self.brand,
            &mut self.input,
            &mut self.stages,
            self.brand.plan,
            NativeSourceMaintenancePort {
                brand: &self.brand,
                state: &mut self.maintenance,
            },
            metadata,
        )
    }
}

impl<'session, 'work>
    SourceCheckpointNativeMaintenancePort<'session, NativeSourceCheckpointBackend<'work>>
    for NativeSourceMaintenancePort<'session, 'work>
{
    fn cancellation_requested(&self) -> bool {
        self.state.cancellation.load(Ordering::Acquire)
            || self.brand.ports.shared.write_fenced.load(Ordering::Acquire)
            || !self.brand.ports.shared.is_source_mode()
    }

    fn apply<'candidate, 'stage, 'slot>(
        &mut self,
        instruction: SourceCheckpointInstruction<
            'candidate,
            'stage,
            'slot,
            'session,
            NativeSourceCheckpointBackend<'work>,
        >,
    ) -> Result<
        SourceCheckpointResponse<
            'candidate,
            'stage,
            'slot,
            'session,
            NativeSourceCheckpointBackend<'work>,
        >,
        SourceDriverError,
    >
    where
        NativeSourceCheckpointBackend<'work>: 'session,
        'session: 'slot,
        'slot: 'stage,
        'stage: 'candidate,
    {
        if self.cancellation_requested() {
            return Err(SourceDriverError::SourceUnavailable);
        }
        let stage = instruction.native_stage().brand;
        let (prepared, continuation) = instruction.into_native_operation(self.brand, stage)?;
        let checked = self.checked_request(continuation.instruction())?;
        if checked.prepared_step().is_some() {
            return Err(SourceDriverError::InputChanged);
        }
        match checked.operation() {
            SourceCheckpointOperationKind::PrepareAllocation => {
                if prepared.is_some() {
                    return Err(SourceDriverError::InputChanged);
                }
                let observation = preparation::observe(&checked)?;
                observation
                    .source_epoch()
                    .verify_for_native_allocation_observation(checked.actual_ports())?;
                if observation.observed_history().lineage() != checked.captured_history().lineage()
                {
                    return Err(SourceDriverError::InputChanged);
                }
                drop(checked);
                continuation.prepared(self.brand, stage, observation)
            }
            SourceCheckpointOperationKind::Allocate
            | SourceCheckpointOperationKind::Copy
            | SourceCheckpointOperationKind::Publish => {
                let NativeSourcePreparedStep {
                    intent,
                    usage,
                    controls,
                    step,
                } = prepared.ok_or(SourceDriverError::InputChanged)?;
                step.validate_for(&checked)?;
                let started = Instant::now();
                let lease = self
                    .brand
                    .ports
                    .shared
                    .mutation_gate
                    .acquire_source()?
                    .ok_or(SourceDriverError::SourceUnavailable)?;
                enum ApplyOutcome {
                    Submitted,
                    NoSubmission(riffdb_storage_api::StorageError),
                }
                enum GateTurnOutcome<'w> {
                    Yielded,
                    Submitted(crate::source_usage_domain::SourceUsageMutation<'w>),
                    NoSubmission(
                        crate::source_usage_domain::SourceUsageMutation<'w>,
                        riffdb_storage_api::StorageError,
                    ),
                }
                let turn_outcome = match controls.preinstall(&checked, &lease) {
                    Ok(preinstalled) => {
                        let mutation = usage.begin_under_source_lease(self.brand.ports, &lease)?;
                        let apply_outcome = match preinstalled.apply(&mutation) {
                            Ok(SourceHistorySubmitted { .. }) => Ok(ApplyOutcome::Submitted),
                            Err(SourceHistoryApplyFailure::NoSubmission { error, proof }) => {
                                let verified = intent::verified_before(proof)?;
                                self.brand
                                    .ports
                                    .shared
                                    .database
                                    .source_usage_domain()
                                    .reconcile_native_step(self.brand.ports, &lease, verified)?;
                                Ok(ApplyOutcome::NoSubmission(error))
                            }
                            Err(SourceHistoryApplyFailure::Uncertain(error)) => {
                                Err(SourceDriverError::from(error))
                            }
                        }?;
                        match apply_outcome {
                            ApplyOutcome::Submitted => Ok(GateTurnOutcome::Submitted(mutation)),
                            ApplyOutcome::NoSubmission(error) => {
                                Ok(GateTurnOutcome::NoSubmission(mutation, error))
                            }
                        }
                    }
                    Err(SourceHistoryPreinstallFailure::Yield(
                        SourceHistoryNoSubmitYield::Undrained
                        | SourceHistoryNoSubmitYield::HeadAdvanced,
                    )) => Ok(GateTurnOutcome::Yielded),
                    Err(SourceHistoryPreinstallFailure::Rejected(error)) => {
                        Err(SourceDriverError::from(error))
                    }
                }?;
                match turn_outcome {
                    GateTurnOutcome::Yielded => {
                        drop(lease);
                        let pacing = pacing_from(started)?;
                        drop(checked);
                        drop(step);
                        drop(intent);
                        continuation.known_pre_submit_stale(
                            self.brand,
                            stage,
                            NativeSourcePreSubmitRefusalEvidence { _private: () },
                            None,
                            pacing,
                        )
                    }
                    GateTurnOutcome::NoSubmission(mutation, error) => {
                        drop(lease);
                        let pacing = pacing_from(started)?;
                        drop(mutation);
                        drop(checked);
                        drop(step);
                        drop(intent);
                        if error.kind() == StorageErrorKind::Unavailable {
                            continuation.known_pre_submit_stale(
                                self.brand,
                                stage,
                                NativeSourcePreSubmitRefusalEvidence { _private: () },
                                None,
                                pacing,
                            )
                        } else {
                            Err(error.into())
                        }
                    }
                    GateTurnOutcome::Submitted(mutation) => {
                        drop(lease);
                        let gate_nanos = elapsed_nanos(started);
                        drop(checked);
                        let observed = intent::inspect_physical_source_image(
                            self.brand.ports,
                            &mutation,
                            &intent,
                        )?;
                        let reconcile_started = Instant::now();
                        let reconcile_lease = self
                            .brand
                            .ports
                            .shared
                            .mutation_gate
                            .acquire_source()?
                            .ok_or(SourceDriverError::SourceUnavailable)?;
                        let verified = observed.into_verified(&reconcile_lease)?;
                        let domain = self.brand.ports.shared.database.source_usage_domain();
                        match step {
                            PreparedStepKind::Progress(progress) => {
                                let applied = progress.confirm(&verified)?;
                                domain.reconcile_native_step(
                                    self.brand.ports,
                                    &reconcile_lease,
                                    verified,
                                )?;
                                drop(reconcile_lease);
                                let total_nanos =
                                    gate_nanos.saturating_add(elapsed_nanos(reconcile_started));
                                let pacing = SourceGatePacingObservation::from_native_gate_cost(
                                    total_nanos.min(u64::MAX / 19),
                                )?;
                                drop(mutation);
                                continuation.applied(self.brand, stage, applied, pacing)
                            }
                            PreparedStepKind::Publish(publication) => {
                                let (applied, published) = publication.confirm(&verified)?;
                                domain.reconcile_native_step(
                                    self.brand.ports,
                                    &reconcile_lease,
                                    verified,
                                )?;
                                drop(reconcile_lease);
                                let total_nanos =
                                    gate_nanos.saturating_add(elapsed_nanos(reconcile_started));
                                let pacing = SourceGatePacingObservation::from_native_gate_cost(
                                    total_nanos.min(u64::MAX / 19),
                                )?;
                                drop(mutation);
                                continuation
                                    .published(self.brand, stage, applied, published, pacing)
                            }
                        }
                    }
                }
            }
            _ => Err(SourceDriverError::InputChanged),
        }
    }

    fn await_maintenance_turn(
        &mut self,
        observation: SourceGatePacingObservation,
    ) -> Result<(), SourceDriverError> {
        if self.cancellation_requested() || !self.state.prefix.belongs_to_ports(self.brand.ports) {
            return Err(SourceDriverError::SourceUnavailable);
        }
        let _ = observation.gate_cost_nanos();
        let _ = observation.rest_nanos();
        let control = self.brand.ports.shared_source_control()?;
        let mut guard = control
            .lock()
            .map_err(|_| SourceDriverError::SourceUnavailable)?;
        guard.bind_source_scheduler();
        if !guard.source_work_wait().is_zero() {
            return Err(SourceDriverError::SourceUnavailable);
        }
        Ok(())
    }
}

fn elapsed_nanos(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX / 19)
}
fn pacing_from(started: Instant) -> Result<SourceGatePacingObservation, SourceDriverError> {
    SourceGatePacingObservation::from_native_gate_cost(elapsed_nanos(started).min(u64::MAX / 19))
}

impl Drop for NativeSourceCheckpointBackend<'_> {
    fn drop(&mut self) {}
}
impl Drop for NativeSourceMaintenancePort<'_, '_> {
    fn drop(&mut self) {}
}

#[cfg(test)]
impl<'work> NativeSourceCheckpointBackend<'work> {
    pub(crate) fn swap_input_for_test(&mut self, other: &mut Self) {
        std::mem::swap(&mut self.input, &mut other.input);
    }
}

#[cfg(test)]
impl<'session, 'work> NativeSourceMaintenancePort<'session, 'work> {
    pub(crate) fn brand_for_test(&self) -> &'session NativeSourceSessionBrand<'work> {
        self.brand
    }

    pub(crate) fn checked_request_for_test<'request, 'candidate, 'stage, 'slot>(
        &self,
        instruction: &'request SourceCheckpointInstruction<
            'candidate,
            'stage,
            'slot,
            'session,
            NativeSourceCheckpointBackend<'work>,
        >,
    ) -> Result<
        NativeSourceCheckpointRequest<'request, 'candidate, 'stage, 'slot, 'session, 'work>,
        SourceDriverError,
    >
    where
        'work: 'session,
        'session: 'slot,
        'slot: 'stage,
        'stage: 'candidate,
    {
        self.checked_request(instruction)
    }
}

#[cfg(test)]
impl<'work> NativeSourcePreparedStep<'work> {
    pub(crate) fn progress_for_test(
        &self,
    ) -> Option<
        &riffdb_types::ProjectionSourceWorkAllocationV1<
            riffdb_storage_api::ProjectionSourceMaintenanceV1,
        >,
    > {
        match &self.step {
            PreparedStepKind::Progress(progress) => progress.allocated_progress_for_test(),
            PreparedStepKind::Publish(_) => None,
        }
    }

    pub(crate) fn into_parts_for_test(
        self,
    ) -> (
        NativeSourceStepIntent,
        NativePreparedSourceUsage<'work>,
        PreparedSourceHistoryControls<'work>,
        Option<intent::step_evidence::NativePreparedStepProgress<'work>>,
        Option<intent::publication_recipe::NativePreparedPublication<'work>>,
    ) {
        match self.step {
            PreparedStepKind::Progress(progress) => {
                (self.intent, self.usage, self.controls, Some(progress), None)
            }
            PreparedStepKind::Publish(publication) => (
                self.intent,
                self.usage,
                self.controls,
                None,
                Some(publication),
            ),
        }
    }
}
