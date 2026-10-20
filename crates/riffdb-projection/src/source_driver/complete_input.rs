//! Nonpublishing complete-input interpretation for the exclusive offline cut.
//! This private draft is not integrated, compiled, or native authority evidence.
use super::{
    SourceCommandResponse, SourceDriverError, SourceReadInstruction, SourceReplayInputBackend,
    SourceRewindInstruction, seal::SourceBrand,
};
use riffdb_catalog::{ActiveCatalogSnapshot, ProjectionSourcePlanCaptureErrorV1};
use riffdb_storage_api::{ActiveCatalogPointerV1, ChangelogHistoryStateV3};
use riffdb_types::{
    CommitSequence, ProjectionIdentity, ProjectionSourceWorkReservationV1,
    ProjectionSourceWorkingBudgetV1,
};

/// One genuine offline input and its actual checked catalog, not a list of plans.
/// The native factory must retain the admitted catalog and Original custody,
/// bind the complete history and ledger, and keep its instance brand private.
/// No stage, source locator, allocator, or publication operation is accepted.
#[doc(hidden)]
pub struct SourceActivePlanInputSession<'session, B: SourceReplayInputBackend> {
    brand: SourceBrand<'session, B>,
    input: &'session mut B::Input,
    catalog: Option<&'session ActiveCatalogSnapshot>,
    history: &'session ChangelogHistoryStateV3,
    budget: &'session ProjectionSourceWorkingBudgetV1,
    // Actual fixed control admission survives every instruction and completion.
    _control: ProjectionSourceWorkReservationV1,
}
impl<'session, B: SourceReplayInputBackend> SourceActivePlanInputSession<'session, B> {
    /// Only a backend owning its actual private brand can attach native input.
    /// A generic test backend can construct only its own invariant protocol.
    pub fn attach(
        actual: &'session B::Brand,
        input: &'session mut B::Input,
        catalog: Option<&'session ActiveCatalogSnapshot>,
        history: &'session ChangelogHistoryStateV3,
        budget: &'session ProjectionSourceWorkingBudgetV1,
    ) -> Result<Self, SourceDriverError> {
        let bytes = std::mem::size_of::<(
            Self,
            SourceCompleteActivePlanInput<'session, B>,
            SourceReadInstruction<'session, B>,
            SourceCommandResponse<'session, 'session, B>,
            SourceRewindInstruction<'session, B>,
            ProjectionIdentity,
            SourceDriverError,
            [u64; 4],
        )>();
        let control = budget
            .reserve(u64::try_from(bytes).map_err(|_| SourceDriverError::SourceUnavailable)?)
            .map_err(|_| SourceDriverError::SourceUnavailable)?;
        Ok(Self {
            brand: SourceBrand::new(actual),
            input,
            catalog,
            history,
            budget,
            _control: control,
        })
    }
}
impl<B: SourceReplayInputBackend> Drop for SourceActivePlanInputSession<'_, B> {
    fn drop(&mut self) {}
}

/// Produced only after every actual active plan reaches exact Original end.
/// Retains the actual input/catalog/brand through physical destruction, even
/// when the caller never reads the measurements. It is not a checkpoint proof.
#[doc(hidden)]
pub struct SourceCompleteActivePlanInput<'session, B: SourceReplayInputBackend> {
    plans: u32,
    commands: u64,
    // Last: retain the admitted session/control through complete-value retirement.
    session: SourceActivePlanInputSession<'session, B>,
}
impl<B: SourceReplayInputBackend> SourceCompleteActivePlanInput<'_, B> {
    pub const fn checked_plan_count(&self) -> u32 {
        self.plans
    }
    /// Includes each complete command interpreted independently for each plan.
    pub const fn checked_command_count(&self) -> u64 {
        self.commands
    }

    /// The closed native cut constructor consumes this before transferring its
    /// still-live Original artifacts; it separately rechecks seals and authority.
    /// Equal IDs or a successful generic backend cannot satisfy native branding.
    pub fn consume_for_cut(
        self,
        actual: &B::Brand,
        history: &ChangelogHistoryStateV3,
        pointer: Option<&ActiveCatalogPointerV1>,
    ) -> Result<(), SourceDriverError> {
        self.session.brand.check(actual)?;
        if self.session.history != history
            || self.session.catalog.map(ActiveCatalogSnapshot::pointer) != pointer
        {
            return Err(SourceDriverError::InputChanged);
        }
        Ok(())
    }
}
impl<B: SourceReplayInputBackend> Drop for SourceCompleteActivePlanInput<'_, B> {
    fn drop(&mut self) {}
}

/// Interpret the entire actual active plan set over the same pinned input.
/// This never requires a nonzero application frontier and cannot enter normal
/// checkpoint staging or maintenance. No caller-selected plan list is accepted.
#[doc(hidden)]
pub fn validate_complete_active_plan_input<'session, B: SourceReplayInputBackend>(
    session: SourceActivePlanInputSession<'session, B>,
) -> Result<SourceCompleteActivePlanInput<'session, B>, SourceDriverError> {
    let mut plans = 0_u32;
    let mut commands = 0_u64;
    if let Some(catalog) = session.catalog {
        // Contract IR already bounds this immutable declaration collection.
        // Borrow it directly: no identity Vec, filtered caller subset or clone
        // of the complete plan set is constructed before Source admission.
        for plan in catalog.bundle().bundle().projections() {
            let identity = ProjectionIdentity::new(
                catalog.bundle().lineage().clone(),
                plan.projection_id(),
                plan.plan_hash(),
            );
            let captured = catalog
                .capture_projection_source_admitted_v1(&identity, session.budget)
                .map_err(|error| match error {
                    ProjectionSourcePlanCaptureErrorV1::Unavailable => {
                        SourceDriverError::SourceUnavailable
                    }
                    ProjectionSourcePlanCaptureErrorV1::Integrity => {
                        SourceDriverError::InputChanged
                    }
                })?;
            if !captured.belongs_to(session.budget)
                || captured.catalog_pointer() != catalog.pointer()
                || captured.schema().identity() != &identity
            {
                return Err(SourceDriverError::ForeignCustody);
            }
            let rewind = B::rewind(
                session.input,
                session.brand.native(),
                SourceRewindInstruction {
                    brand: session.brand.next(),
                },
            )?;
            session.brand.check_same(&rewind.brand)?;
            drop(rewind);
            let frontier = session.history.tail().frontier().application();
            let mut expected = Some(CommitSequence::first());
            loop {
                let response = B::read_command(
                    session.input,
                    session.brand.native(),
                    SourceReadInstruction::new(session.brand.next(), expected, frontier),
                )?;
                match response {
                    SourceCommandResponse::ExactEnd(end) => {
                        end.check(&session.brand)?;
                        break;
                    }
                    SourceCommandResponse::Command(command) => {
                        command.check(&session.brand)?;
                        let evaluated = crate::evaluate_projection_commit_source_admitted_v1(
                            captured.plan(),
                            captured.schema(),
                            command.command(),
                            session.budget,
                        )?;
                        // The closed evaluator takes this exact ledger and retains its
                        // reservation; no caller-supplied result or ledger is accepted.
                        if evaluated.sequence() != command.command().commit_sequence() {
                            return Err(SourceDriverError::InputChanged);
                        }
                        expected = evaluated.sequence().checked_next();
                        commands = commands
                            .checked_add(1)
                            .ok_or(SourceDriverError::SourceUnavailable)?;
                        // Both the interpreted delta graph and the original
                        // input response physically die before the next read.
                        drop(evaluated);
                        drop(command);
                    }
                }
            }
            plans = plans
                .checked_add(1)
                .ok_or(SourceDriverError::SourceUnavailable)?;
            drop(captured);
        }
    }
    Ok(SourceCompleteActivePlanInput {
        session,
        plans,
        commands,
    })
}

#[cfg(test)]
#[path = "complete_input_tests.rs"]
mod tests;
