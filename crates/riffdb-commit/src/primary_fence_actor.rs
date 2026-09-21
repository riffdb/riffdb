//! Fence admission and ownership across the bounded coordinator barrier.
use super::*;
use crate::ControlPlaneExecutionErrorKind;

impl ControlPlaneExecutionCapacityPermit {
    /// Pauses new command/control submissions, waits for admitted senders, and
    /// enqueues one authorized fence in the existing sole-writer queue.
    /// Cancellation before enqueue releases only the nondurable pause. Once
    /// enqueued, dropping the receipt does not cancel fencing or reopen writes.
    pub async fn submit_primary_fence(
        self,
        preparation: AuthorizedPrimaryFencePreparation,
    ) -> Result<PrimaryFenceReceipt, ControlPlaneExecutionAdmissionError> {
        ensure_control_plane_accepting(&self.lifecycle)?;
        let pause = self
            .submission_gate
            .primary
            .pause_for_fence()
            .map_err(|refusal| match refusal {
                PrimaryAdmissionRefusal::Draining => ControlPlaneExecutionAdmissionError::Draining,
                PrimaryAdmissionRefusal::Fenced => {
                    ControlPlaneExecutionAdmissionError::PrimaryFenced
                }
            })?
            .drain()
            .await;
        // Do not hold a synchronous submission guard while awaiting prior
        // senders. Shutdown may close admission during drain; check again here.
        let (permit, submission) = self.into_audit_submission()?;
        let (completion, receiver) = oneshot::channel();
        let _sender = permit.send(CoordinatorMessage::PrimaryFence {
            preparation: Box::new(preparation),
            pause,
            completion,
        });
        drop(submission);
        Ok(PrimaryFenceReceipt { receiver })
    }
}

impl CommandWriter {
    pub(super) fn execute_primary_fence(
        &mut self,
        preparation: AuthorizedPrimaryFencePreparation,
        pause: DrainedPrimaryPause,
        completion: oneshot::Sender<
            Result<PrimaryFenceExecutionResult, ControlPlaneExecutionError>,
        >,
    ) {
        let result = self.operations.fence_primary(preparation);
        match &result {
            Ok(result)
                if matches!(
                    result.outcome(),
                    PrimaryFenceResultV1::Applied(_) | PrimaryFenceResultV1::Replayed(_)
                ) =>
            {
                pause.finish_fenced();
            }
            Err(error)
                if matches!(
                    error.kind(),
                    ControlPlaneExecutionErrorKind::OutcomeUnknown
                        | ControlPlaneExecutionErrorKind::CoordinatorFenced
                ) =>
            {
                // A durable fence may already exist. Neither cancellation nor
                // the eventual error response may restore local admission.
                self.lifecycle.fence();
                pause.finish_fenced();
            }
            _ => drop(pause),
        }
        let _receiver_may_be_dropped = completion.send(result);
    }
}
