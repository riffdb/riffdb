//! Closed actual native Source quota/input preparation and reconciliation.
//! Only this family derives prepared Usage from actual bounded reads and intent.
use super::*;
use crate::source_checkpoint_adapter::{NativeSourceStepIntent, NativeVerifiedSourceStep};
use crate::{RedbOperationalPorts, gate::ExclusiveLease, store::RetainedOriginalSourceInputs};
use riffdb_types::ProjectionSourceWorkReservationV1;

/// Off-gate prepared actual owner. No in-flight mutation starts at construction.
/// Canonical intent is an alias of the same Source-admitted physical allocation.
/// No plan/stage/foreign catalog graph is retained here or in the domain slot.
pub(crate) struct NativePreparedSourceUsage<'work> {
    actual: &'work RedbOperationalPorts,
    epoch: Option<SourceUsageEpoch<'work>>,
    before: Usage,
    after: Usage,
    before_candidate: Option<CandidateUsage>,
    after_candidate: Option<CandidateUsage>,
    intent: Option<NativeSourceStepIntent>,
    new_inputs: Option<RetainedOriginalSourceInputs>,
    control: Option<ProjectionSourceWorkReservationV1>,
}
impl Drop for NativePreparedSourceUsage<'_> {
    fn drop(&mut self) {}
}
impl<'work> NativePreparedSourceUsage<'work> {
    /// The actual fixed native producer invokes this only under its matched
    /// retirement-aware Source lease, before submitting any physical mutation.
    pub(crate) fn begin_under_source_lease(
        mut self,
        ports: &RedbOperationalPorts,
        lease: &ExclusiveLease,
    ) -> Result<SourceUsageMutation<'work>, StorageError> {
        if !std::sync::Arc::ptr_eq(&self.actual.shared, &ports.shared)
            || !lease.is_source_grant_for(&ports.shared.mutation_gate)
        {
            return Err(integrity());
        }
        if !ports.shared.is_source_mode()
            || ports
                .shared
                .write_fenced
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(unavailable());
        }
        let control = self.control.take().ok_or_else(integrity)?;
        let epoch = self.epoch.take().ok_or_else(integrity)?;
        let domain: &'work SourceUsageDomain = self.actual.shared.database.source_usage_domain();
        if !std::ptr::eq(domain, epoch.domain) {
            return Err(integrity());
        }
        let intent = self.intent.take().ok_or_else(integrity)?;
        {
            let state = domain.state.lock().map_err(|_| unavailable())?;
            if state.version != epoch.version
                || state.complete != Some(self.before)
                || state.complete_candidate != self.before_candidate
                || state.pending.is_some()
                || state.publication.is_some()
            {
                return Err(unavailable());
            }
            match (&state.input_obligation, &self.new_inputs) {
                (None, Some(inputs))
                    if inputs.matches_owner(&ports.shared.database)
                        && inputs.captured_locator() == intent.locator() => {}
                (Some(obligation), None)
                    if obligation.unresolved_intent.is_none()
                        && obligation.inputs.matches_owner(&ports.shared.database)
                        && obligation.inputs.captured_locator() == intent.locator() => {}
                _ => return Err(unavailable()),
            }
        }
        // This owner changes no physical bytes. Once pending is installed every
        // census/competing producer refuses; genuine inputs remain held locally
        // or in the existing slot until installation below completes.
        let mut mutation = domain.begin_prepared_change(PreparedChange {
            epoch,
            before: self.before,
            after: self.after,
        })?;
        let mut state = domain.state.lock().map_err(|_| unavailable())?;
        let pending = state.pending.as_mut().ok_or_else(unavailable)?;
        if pending.mutation_version != mutation.version || !pending.in_flight {
            return Err(unavailable());
        }
        pending.after_candidate = self.after_candidate;
        if let Some(inputs) = self.new_inputs.take() {
            if state.input_obligation.is_some() {
                return Err(integrity());
            }
            state.input_obligation = Some(SourceInputObligation {
                inputs,
                unresolved_intent: Some(intent),
            });
        } else {
            let obligation = state.input_obligation.as_mut().ok_or_else(unavailable)?;
            if obligation.unresolved_intent.is_some() {
                return Err(unavailable());
            }
            obligation.unresolved_intent = Some(intent);
        }
        drop(state);
        mutation._native_control = Some(control);
        // No Source write has been submitted before this successful return.
        // The surrounding NativePreparedSourceStep retains its own admitted
        // control/intent and the returned guard through exact read-back.
        Ok(mutation)
    }
}

impl SourceUsageDomain {
    /// Numeric census never calls this. Only constructor-private native exact
    /// physical AND logical read-back may atomically settle quota/input custody.
    pub(crate) fn reconcile_native_step(
        &self,
        ports: &RedbOperationalPorts,
        lease: &ExclusiveLease,
        verified: NativeVerifiedSourceStep<'_>,
    ) -> Result<(), StorageError> {
        if !std::ptr::eq(self, ports.shared.database.source_usage_domain())
            || !std::sync::Arc::ptr_eq(&ports.shared, &verified.actual_ports().shared)
            || !std::ptr::eq(lease, verified.source_lease())
        {
            return Err(integrity());
        }
        verified
            .mutation()
            .verify_for_native_readback(ports, lease)?;
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        if state.version != verified.source_version() || state.publication.is_some() {
            return Err(unavailable());
        }
        let pending = state.pending.ok_or_else(unavailable)?;
        if pending.kind != PendingKind::SourceTransition
            || !pending.in_flight
            || pending.mutation_version != verified.source_version()
        {
            return Err(unavailable());
        }
        let obligation = state.input_obligation.as_ref().ok_or_else(unavailable)?;
        if !obligation.inputs.matches_owner(&ports.shared.database)
            || obligation.inputs.captured_locator() != verified.locator()
            || obligation
                .unresolved_intent
                .as_ref()
                .is_none_or(|intent| !verified.matches_intent(intent))
        {
            return Err(integrity());
        }
        let (usage, candidate) = if verified.is_exact_before() {
            (pending.before, pending.before_candidate)
        } else {
            (pending.after, pending.after_candidate)
        };
        state.complete = Some(usage);
        state.complete_candidate = candidate;
        state.pending = None;
        // Take ownership under the mutex; potentially large canonical bytes and
        // final retention-pin destruction must occur after releasing it.
        let released_inputs = if verified.ends_input_obligation() {
            state.input_obligation.take()
        } else {
            None
        };
        let released_intent = if released_inputs.is_none() {
            state
                .input_obligation
                .as_mut()
                .and_then(|obligation| obligation.unresolved_intent.take())
        } else {
            None
        };
        drop(state);
        drop(released_intent);
        drop(released_inputs);
        // This method never clears the Shared write fence or authorizes writes.
        Ok(())
    }
}

#[path = "source_usage_native_quota.rs"]
mod quota;
pub(crate) use quota::prepare_native_source_usage;
