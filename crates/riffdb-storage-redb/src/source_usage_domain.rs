//! Private actual-engine Source namespace version and quota custody.
//!
//! This mechanical owner is not a table census or publication proof. Only the
//! closed native census/physical-transition child modules may mint its sealed
//! completion/change evidence. No application or DTO constructor can do so.
#![cfg_attr(not(test), allow(dead_code))]
use riffdb_storage_api::{ProjectionSourceTableUsageV1, StorageError, StorageErrorKind};
use std::sync::Mutex;

type Usage = ProjectionSourceTableUsageV1;

// Source usage changes are a closed inventory, not every ordinary V3 history
// receipt. Actual incarnation/role/reset changes invalidate separately.
pub(crate) const fn namespace_affects_source_usage(
    namespace: riffdb_storage_api::AuthoritativeNamespaceV3,
) -> bool {
    use riffdb_storage_api::AuthoritativeNamespaceV3 as N;
    match namespace {
        N::ProjectionSourceRows
        | N::ProjectionSourceObjects
        | N::ProjectionSourceRoots
        | N::ProjectionSourceMaintenance
        | N::ProjectionSourceAllocator => true,
        N::GenerationSourceBases | N::Existing(_) => false,
    }
}

pub(crate) struct SourceUsageDomain {
    state: Mutex<State>,
}
struct State {
    version: u64,
    complete: Option<Usage>,
    complete_candidate: Option<CandidateUsage>,
    input_obligation: Option<SourceInputObligation>,
    // Before/after remain separately retained on uncertainty: combining their
    // candidate/selected maxima into one DTO would invent an impossible state.
    pending: Option<Pending>,
    publication: Option<Publication>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CandidateUsage {
    locator: riffdb_types::ProjectionSourceLocatorV1,
    full: u64,
    actual: u64,
}
#[cfg(any(test, feature = "projection-source-driver"))]
struct SourceInputObligation {
    inputs: crate::store::RetainedOriginalSourceInputs,
    // None is known-active custody; Some retains the SAME admitted physical
    // intent through unresolved outcome. No semantic descriptor graph is kept.
    unresolved_intent: Option<crate::source_checkpoint_adapter::NativeSourceStepIntent>,
}
#[cfg(not(any(test, feature = "projection-source-driver")))]
struct SourceInputObligation {
    #[expect(
        dead_code,
        reason = "RAII custody retains original source inputs in non-source-driver builds"
    )]
    inputs: crate::store::RetainedOriginalSourceInputs,
    unresolved_intent: Option<()>,
}
#[derive(Clone, Copy)]
struct Publication {
    version: u64,
    in_flight: bool,
    prior: Option<Usage>,
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum PendingKind {
    SourceTransition,
    FreshInitialization,
}
#[derive(Clone, Copy)]
struct Pending {
    kind: PendingKind,
    in_flight: bool,
    mutation_version: u64,
    before: Usage,
    after: Usage,
    before_candidate: Option<CandidateUsage>,
    after_candidate: Option<CandidateUsage>,
}

/// Borrowed exact actual-engine version; durable DB-ID equality is insufficient.
pub(crate) struct SourceUsageEpoch<'engine> {
    domain: &'engine SourceUsageDomain,
    version: u64,
}
#[cfg(any(test, feature = "projection-source-driver"))]
impl SourceUsageEpoch<'_> {
    /// Closed native allocation observation, before/after its actual short read.
    /// No pin, state or mutable capability escapes this exact owner predicate.
    pub(crate) fn verify_for_native_allocation_observation(
        &self,
        ports: &crate::RedbOperationalPorts,
    ) -> Result<(), StorageError> {
        if !std::ptr::eq(self.domain, ports.shared.database.source_usage_domain()) {
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
        let state = self.domain.state.lock().map_err(|_| unavailable())?;
        if state.version != self.version
            || state.pending.is_some()
            || state.publication.is_some()
            || state.input_obligation.is_some()
            || state.complete_candidate.is_some()
            || state
                .complete
                .is_none_or(|usage| usage.candidate_reserved_bytes().is_some())
        {
            return Err(unavailable());
        }
        Ok(())
    }
}
#[cfg(any(test, feature = "projection-source-driver"))]
impl SourceUsageEpoch<'_> {
    /// Current preparation for a known-active candidate, not a detached prior
    /// allocation observation. Ordinary application appends leave this epoch.
    pub(crate) fn verify_for_native_step_observation(
        &self,
        ports: &crate::RedbOperationalPorts,
        locator: riffdb_types::ProjectionSourceLocatorV1,
    ) -> Result<(), StorageError> {
        if !std::ptr::eq(self.domain, ports.shared.database.source_usage_domain()) {
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
        let state = self.domain.state.lock().map_err(|_| unavailable())?;
        if state.version != self.version
            || state.pending.is_some()
            || state.publication.is_some()
            || state.complete.is_none()
            || state
                .complete_candidate
                .is_none_or(|candidate| candidate.locator != locator)
            || state.input_obligation.as_ref().is_none_or(|obligation| {
                obligation.unresolved_intent.is_some()
                    || obligation.inputs.captured_locator() != locator
                    || !obligation.inputs.matches_owner(&ports.shared.database)
            })
        {
            return Err(unavailable());
        }
        Ok(())
    }
}
impl Drop for SourceUsageEpoch<'_> {
    fn drop(&mut self) {
        // Keep the actual domain borrow through unused capability destruction.
    }
}

// Fields are private. Future native census code is a child module of this
// closed family and must mint this only after its real bounded physical walk.
struct CompletedCensus<'engine> {
    epoch: SourceUsageEpoch<'engine>,
    usage: Usage,
    candidate: Option<CandidateUsage>,
}
// A DTO is not a prepared native transition. Its producer must bind actual
// candidate, exact persisted before/after bytes, source quota and primary turn.
struct PreparedChange<'engine> {
    epoch: SourceUsageEpoch<'engine>,
    before: Usage,
    after: Usage,
}
// Minted by physical exact-receipt reconciliation, never from a mere Ok/Err.
enum VerifiedPhysicalOutcome {
    ExactBefore,
    ExactAfter,
}

impl SourceUsageDomain {
    pub(crate) fn new_unavailable() -> Self {
        Self {
            state: Mutex::new(State {
                version: 0,
                complete: None,
                complete_candidate: None,
                input_obligation: None,
                pending: None,
                publication: None,
            }),
        }
    }

    pub(crate) fn capture_epoch(&self) -> Result<SourceUsageEpoch<'_>, StorageError> {
        let state = self.state.lock().map_err(|_| unavailable())?;
        if state.pending.is_some_and(|pending| pending.in_flight)
            || state
                .publication
                .is_some_and(|publication| publication.in_flight)
        {
            return Err(unavailable());
        }
        Ok(SourceUsageEpoch {
            domain: self,
            version: state.version,
        })
    }

    fn require_epoch(&self, epoch: &SourceUsageEpoch<'_>) -> Result<(), StorageError> {
        if !std::ptr::eq(self, epoch.domain) {
            return Err(integrity());
        }
        let state = self.state.lock().map_err(|_| unavailable())?;
        if state.version != epoch.version
            || state.pending.is_some_and(|pending| pending.in_flight)
            || state
                .publication
                .is_some_and(|publication| publication.in_flight)
        {
            return Err(unavailable());
        }
        Ok(())
    }

    /// Bounded observation only. Reconstructing this DTO grants no admission.
    pub(crate) fn complete_observation(&self) -> Result<Usage, StorageError> {
        let state = self.state.lock().map_err(|_| unavailable())?;
        if state.pending.is_some() || state.publication.is_some() {
            return Err(unavailable());
        }
        state.complete.ok_or_else(unavailable)
    }

    fn install_completed_census(&self, completed: CompletedCensus<'_>) -> Result<(), StorageError> {
        if !std::ptr::eq(self, completed.epoch.domain) {
            return Err(integrity());
        }
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        if state.version != completed.epoch.version
            || state.pending.is_some_and(|pending| pending.in_flight)
            || state
                .publication
                .is_some_and(|publication| publication.in_flight)
        {
            return Err(unavailable());
        }
        state.complete = Some(completed.usage);
        state.complete_candidate = completed.candidate;
        // The sealed full physical census reconstructs Copying reservations as
        // well as actual bytes and may reconcile a released uncertain writer.
        // Numeric reconstruction never settles a retained native intent or its
        // Original closure. Preserve the exact before/after pair for recovery.
        if state
            .input_obligation
            .as_ref()
            .is_none_or(|obligation| obligation.unresolved_intent.is_none())
        {
            state.pending = None;
            state.publication = None;
        }
        Ok(())
    }

    fn begin_prepared_change(
        &self,
        prepared: PreparedChange<'_>,
    ) -> Result<SourceUsageMutation<'_>, StorageError> {
        if !std::ptr::eq(self, prepared.epoch.domain) {
            return Err(integrity());
        }
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        if state.version != prepared.epoch.version
            || state.pending.is_some()
            || state.publication.is_some()
            || state.complete != Some(prepared.before)
        {
            return Err(unavailable());
        }
        let version = state.version.checked_add(1).ok_or_else(limit)?;
        state.version = version;
        state.complete = None;
        let before_candidate = state.complete_candidate.take();
        state.pending = Some(Pending {
            kind: PendingKind::SourceTransition,
            in_flight: true,
            mutation_version: version,
            before: prepared.before,
            after: prepared.after,
            before_candidate,
            after_candidate: None,
        });
        Ok(SourceUsageMutation {
            domain: self,
            version,
            _native_control: None,
        })
    }

    fn observe_unreadable(&self, epoch: &SourceUsageEpoch<'_>) {
        // Keep a prior numerical proof from admitting Source work after a real
        // required-input failure. Resource-only refusal does not invoke this.
        if let Ok(mut state) = self.state.lock()
            && std::ptr::eq(self, epoch.domain)
            && state.version == epoch.version
            && !state.pending.is_some_and(|pending| pending.in_flight)
            && !state
                .publication
                .is_some_and(|publication| publication.in_flight)
        {
            state.complete_candidate = None;
            let prior = state
                .complete
                .take()
                .or_else(|| state.publication.and_then(|p| p.prior));
            state.publication = Some(Publication {
                version: state.version,
                in_flight: false,
                prior,
            });
        }
    }

    /// Actual source publication without prepared exact before/after accounting.
    /// The real logical/physical owner retains this guard through publication.
    /// It grants no Source mutation or capacity authority. All other writes
    /// bypass it; a released guard never restores completeness or proves rollback.
    pub(crate) fn begin_unaccounted_publication(
        &self,
    ) -> Result<SourceUsagePublication<'_>, StorageError> {
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        if state.input_obligation.is_some()
            || state.pending.is_some_and(|pending| pending.in_flight)
            || state
                .publication
                .is_some_and(|publication| publication.in_flight)
        {
            return Err(unavailable());
        }
        let version = state.version.checked_add(1).ok_or_else(limit)?;
        let prior = state
            .complete
            .or_else(|| state.publication.and_then(|publication| publication.prior));
        state.version = version;
        state.complete = None;
        state.complete_candidate = None;
        state.publication = Some(Publication {
            version,
            in_flight: true,
            prior,
        });
        Ok(SourceUsagePublication {
            domain: self,
            version,
        })
    }

    /// Uncertainty diagnostics retain maxima, never a selectable usage proof.
    pub(crate) fn retained_uncertain_bytes(&self) -> Result<Option<u64>, StorageError> {
        let state = self.state.lock().map_err(|_| unavailable())?;
        // An unaccounted physical transition has no exact after quota proof.
        // Keep the whole compiled Source quota unavailable until real census;
        // never invent an empty before/after DTO from a cold owner.
        if state.publication.is_some() {
            return Ok(Some(riffdb_storage_api::MAX_PROJECTION_SOURCE_TABLE_BYTES));
        }
        Ok(state.pending.map(|pending| {
            pending
                .before
                .total_bytes()
                .max(pending.after.total_bytes())
        }))
    }
}

pub(crate) struct SourceUsagePublication<'engine> {
    domain: &'engine SourceUsageDomain,
    version: u64,
}
impl SourceUsagePublication<'_> {
    pub(crate) fn require_for_shared(
        &self,
        shared: &crate::store::SharedRedb,
    ) -> Result<(), StorageError> {
        if !std::ptr::eq(self.domain, shared.database.source_usage_domain()) {
            return Err(integrity());
        }
        let state = self.domain.state.lock().map_err(|_| unavailable())?;
        if state.version != self.version
            || state.publication.is_none_or(|publication| {
                publication.version != self.version || !publication.in_flight
            })
        {
            return Err(unavailable());
        }
        Ok(())
    }
}
impl Drop for SourceUsagePublication<'_> {
    fn drop(&mut self) {
        // Scheduling only, after the actual owner ends its publication window.
        // A poisoned owner remains unavailable. Keep prior/full-quota custody;
        // neither success, failure nor destructor can mint physical completeness.
        if let Ok(mut state) = self.domain.state.lock()
            && let Some(publication) = state.publication.as_mut()
            && publication.version == self.version
        {
            publication.in_flight = false;
        }
    }
}

pub(crate) struct SourceUsageMutation<'engine> {
    domain: &'engine SourceUsageDomain,
    version: u64,
    _native_control: Option<riffdb_types::ProjectionSourceWorkReservationV1>,
}
impl<'engine> SourceUsageMutation<'engine> {
    /// Readonly scalar on the actual borrowed guard; never a detached proof.
    pub(crate) fn native_version(&self) -> u64 {
        self.version
    }

    /// Off-gate exact bounded observation may reconcile an already fenced writer.
    /// It grants no write admission or permission to clear the Shared write fence.
    pub(crate) fn verify_for_native_observation(
        &self,
        ports: &crate::RedbOperationalPorts,
    ) -> Result<(), StorageError> {
        if !std::ptr::eq(self.domain, ports.shared.database.source_usage_domain()) {
            return Err(integrity());
        }
        if !ports.shared.is_source_mode() {
            return Err(unavailable());
        }
        let state = self.domain.state.lock().map_err(|_| unavailable())?;
        if state.version != self.version
            || state.publication.is_some()
            || state.pending.is_none_or(|pending| {
                pending.kind != PendingKind::SourceTransition
                    || !pending.in_flight
                    || pending.mutation_version != self.version
            })
        {
            return Err(unavailable());
        }
        Ok(())
    }

    /// Consuming exact read-back evidence checks the SAME actual Source lease.
    /// A successful reconciliation does not unfence the engine or admit writes.
    pub(crate) fn verify_for_native_readback(
        &self,
        ports: &crate::RedbOperationalPorts,
        lease: &crate::gate::ExclusiveLease,
    ) -> Result<(), StorageError> {
        if !lease.is_source_grant_for(&ports.shared.mutation_gate) {
            return Err(integrity());
        }
        self.verify_for_native_observation(ports)
    }

    #[cfg(any(test, feature = "projection-source-driver"))]
    pub(crate) fn require_native_intent(
        &self,
        intent: &crate::source_checkpoint_adapter::NativeSourceStepIntent,
    ) -> Result<(), StorageError> {
        let state = self.domain.state.lock().map_err(|_| unavailable())?;
        if state.version != self.version
            || state.publication.is_some()
            || state.pending.is_none_or(|pending| {
                pending.kind != PendingKind::SourceTransition
                    || !pending.in_flight
                    || pending.mutation_version != self.version
            })
            || state.input_obligation.as_ref().is_none_or(|obligation| {
                obligation
                    .unresolved_intent
                    .as_ref()
                    .is_none_or(|pending| !std::ptr::eq(&**pending, &**intent))
            })
        {
            return Err(unavailable());
        }
        Ok(())
    }

    /// Borrow the actual prepared mutation; this grants no new Source write or
    /// outcome authority and never starts a second publication window.
    pub(crate) fn physical_commit_borrow<'guard>(
        &'guard self,
        ports: &'guard crate::RedbOperationalPorts,
        lease: &'guard crate::gate::ExclusiveLease,
    ) -> Result<SourceUsagePhysicalCommitBorrow<'guard, 'engine>, StorageError> {
        let borrowed = SourceUsagePhysicalCommitBorrow {
            mutation: self,
            shared: &ports.shared,
            lease,
        };
        borrowed.require_for_shared(&ports.shared)?;
        Ok(borrowed)
    }
    fn resolve_exact(self, outcome: VerifiedPhysicalOutcome) -> Result<(), StorageError> {
        let mut state = self.domain.state.lock().map_err(|_| unavailable())?;
        if state.version != self.version || state.publication.is_some() {
            return Err(unavailable());
        }
        let pending = state.pending.ok_or_else(unavailable)?;
        state.complete = Some(match outcome {
            VerifiedPhysicalOutcome::ExactBefore => pending.before,
            VerifiedPhysicalOutcome::ExactAfter => pending.after,
        });
        state.complete_candidate = match outcome {
            VerifiedPhysicalOutcome::ExactBefore => pending.before_candidate,
            VerifiedPhysicalOutcome::ExactAfter => pending.after_candidate,
        };
        state.pending = None;
        Ok(())
    }
}
/// Closed physical-commit seam. The genuine prepared owner retains its original
/// mutation through later logical publication/read-back; consuming this borrow
/// cannot resolve quota, classify rollback, or release Original inputs.
pub(crate) struct SourceUsagePhysicalCommitBorrow<'guard, 'engine> {
    mutation: &'guard SourceUsageMutation<'engine>,
    shared: &'guard crate::store::SharedRedb,
    lease: &'guard crate::gate::ExclusiveLease,
}
impl SourceUsagePhysicalCommitBorrow<'_, '_> {
    pub(crate) fn require_for_shared(
        &self,
        shared: &crate::store::SharedRedb,
    ) -> Result<(), StorageError> {
        if !std::ptr::eq(self.shared, shared)
            || !std::ptr::eq(self.mutation.domain, shared.database.source_usage_domain())
            || !self.lease.is_source_grant_for(&shared.mutation_gate)
        {
            return Err(integrity());
        }
        if !shared.is_source_mode()
            || shared
                .write_fenced
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(unavailable());
        }
        let state = self
            .mutation
            .domain
            .state
            .lock()
            .map_err(|_| unavailable())?;
        if state.version != self.mutation.version
            || state.publication.is_some()
            || state.pending.is_none_or(|pending| {
                pending.kind != PendingKind::SourceTransition
                    || !pending.in_flight
                    || pending.mutation_version != self.mutation.version
            })
        {
            return Err(unavailable());
        }
        Ok(())
    }
}
impl Drop for SourceUsagePhysicalCommitBorrow<'_, '_> {
    fn drop(&mut self) {
        // Explicit custody of the actual mutation and matching gate through even
        // an unused commit-borrow's destruction. No outcome follows from Drop.
    }
}

impl Drop for SourceUsageMutation<'_> {
    fn drop(&mut self) {
        // A mere error, cancellation or dropped writer must NOT release pending
        // quota, reinstall the before image or imply physical rollback. Pending
        // was already retained before mutation; future admission stays refused.
        // Scheduling state only: this does not prove rollback or publish usage.
        // A poisoned owner stays unavailable; never recover it into admission.
        if let Ok(mut state) = self.domain.state.lock()
            && let Some(pending) = state.pending.as_mut()
            && pending.mutation_version == self.version
        {
            pending.in_flight = false;
        }
    }
}

fn unavailable() -> StorageError {
    StorageError::new(StorageErrorKind::Unavailable, None)
}
fn integrity() -> StorageError {
    StorageError::new(StorageErrorKind::InvariantViolation, None)
}
fn limit() -> StorageError {
    StorageError::new(StorageErrorKind::LimitExceeded, None)
}

#[path = "source_usage_census.rs"]
mod census;
#[cfg(test)]
pub(crate) use census::SourceUsageCensus;
pub(crate) use census::SourceUsageReconstruction;

#[path = "source_usage_fresh.rs"]
mod fresh;
pub(crate) use fresh::{PreparedFreshSourceUsage, prepare_fresh_source_usage};

#[cfg(test)]
#[path = "source_usage_domain_tests.rs"]
mod tests;

#[cfg(any(test, feature = "projection-source-driver"))]
#[path = "source_usage_native.rs"]
mod native;
#[cfg(any(test, feature = "projection-source-driver"))]
pub(crate) use native::{NativePreparedSourceUsage, prepare_native_source_usage};

#[path = "source_usage_census_read.rs"]
mod read;

#[cfg(any(test, feature = "projection-source-driver"))]
#[path = "source_usage_history.rs"]
mod history;
#[cfg(any(test, feature = "projection-source-driver"))]
pub(crate) use history::{
    NativeSourceNoSubmission, PreparedSourceHistoryControls, SourceHistoryApplyFailure,
    SourceHistoryNoSubmitYield, SourceHistoryPreinstallFailure, SourceHistorySubmitted,
};
