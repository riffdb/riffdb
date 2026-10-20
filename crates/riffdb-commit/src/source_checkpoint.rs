//! Commit-owned bounded orchestration of one opaque Source checkpoint candidate.
//!
//! Physical native operations retain their actual Source admission and durable
//! reconciliation owners. This consumer alone authorizes ordinal assignment;
//! projection prepares each bounded continuation outside the writer gate.
use std::marker::PhantomData;

use riffdb_projection::source_driver::{
    MAX_SOURCE_CHECKPOINT_STEPS, MAX_SOURCE_STEP_REPREPARES, SourceCheckpointCandidate,
    SourceCheckpointContinuation, SourceCheckpointDriverBackend,
    SourceCheckpointMaintenanceCoordinator, SourceCheckpointNativeMaintenancePort,
    SourceDriverError, SourceNativePublicationConfirmation,
};

/// Fixed composition consumer. Applications receive no source-writing handle.
/// The server composition fixes the genuine backend and its existing joined
/// maintenance owner; it accepts no caller-selected coordinator or callback.
#[doc(hidden)]
pub struct SourceCheckpointCoordinator<B: SourceCheckpointDriverBackend> {
    backend: PhantomData<fn() -> B>,
}

impl<B: SourceCheckpointDriverBackend> SourceCheckpointCoordinator<B> {
    /// Constructs the coordinator role, without creating a native input,
    /// candidate, ordinal allocation, write capability or completion proof.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            backend: PhantomData,
        }
    }
}

impl<B: SourceCheckpointDriverBackend> Default for SourceCheckpointCoordinator<B> {
    fn default() -> Self {
        Self::new()
    }
}

impl<B: SourceCheckpointDriverBackend> SourceCheckpointMaintenanceCoordinator
    for SourceCheckpointCoordinator<B>
{
    type Backend = B;

    fn maintain<'candidate, 'stage, 'slot, 'session>(
        &mut self,
        candidate: SourceCheckpointCandidate<'candidate, 'stage, 'slot, 'session, B>,
        native: &mut B::MaintenancePort<'session>,
    ) -> Result<SourceNativePublicationConfirmation<'session, B>, SourceDriverError>
    where
        B: 'session,
        'session: 'slot,
        'slot: 'stage,
        'stage: 'candidate,
    {
        let mut instruction = candidate.into_prepare_instruction()?;
        let mut steps = 0;
        let mut reprepares = 0;
        loop {
            if steps == MAX_SOURCE_CHECKPOINT_STEPS {
                return Err(SourceDriverError::SourceUnavailable);
            }
            if native.cancellation_requested() {
                return Err(SourceDriverError::SourceUnavailable);
            }
            if let Some((allocator, database, incarnation)) = instruction.observed_ordinal() {
                let assignment = allocator
                    .allocate_one(database, incarnation)
                    .map_err(|_| SourceDriverError::SourceUnavailable)?;
                instruction = instruction.authorize_ordinal(assignment)?;
            }
            if let Some(history) = instruction.observed_transaction_history() {
                // Native preparation proposes bytes from one sealed observation;
                // only this commit owner assigns their transaction sequence.
                let sequence = history
                    .expected_allocator()
                    .allocate_one()
                    .map_err(|_| SourceDriverError::SourceUnavailable)?
                    .0;
                let assigned = riffdb_storage_api::AuthoritativeTransactionBindingV3 {
                    database_id: history.lineage().database_id(),
                    history_incarnation: history.lineage().history_incarnation(),
                    predecessor: Some(history.tail().sequence()),
                    sequence,
                    predecessor_frontier: history.tail().frontier(),
                    covered_frontier: history.tail().frontier(),
                    prior_history_hash: history.tail().history_hash(),
                };
                instruction = instruction.authorize_transaction_binding(assigned)?;
            }
            let response = native.apply(instruction)?;
            let observation = response.pacing_observation();
            let stale = response.is_known_pre_submit_stale();
            // Keep preparation failure until after the actual gate's mandatory
            // rest. Native also charges its existing owner on apply Err/unknown;
            // an absent success response cannot erase that obligation.
            let continuation = response.prepare_next();
            if let Some(observation) = observation {
                native.await_maintenance_turn(observation)?;
            }
            if stale {
                // Only the native consuming factory can certify no submission.
                // A retry retains this exact candidate, input fence and stage;
                // it cannot consume the finite durable-progress allowance.
                if reprepares == MAX_SOURCE_STEP_REPREPARES {
                    return Err(SourceDriverError::SourceUnavailable);
                }
                reprepares += 1;
            } else {
                steps += 1;
                reprepares = 0;
            }
            match continuation? {
                SourceCheckpointContinuation::Instruction(next) => instruction = next,
                SourceCheckpointContinuation::Published(confirmation) => return Ok(confirmation),
            }
        }
    }
}
