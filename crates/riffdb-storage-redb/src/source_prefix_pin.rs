//! A worker's logical base-and-tail retention lease, independent of read roots.
//! Registration/final prune occur under the existing coordinator gate. This
//! domain grants retention protection only; it does not validate a plan, source
//! object, captured prefix or publication. The closed capture owner does that.
use riffdb_storage_api::{
    ChangelogHistoryPointV3, ChangelogHistoryStateV3, ChangelogLineageV3, StorageError,
    StorageErrorKind,
};
use riffdb_types::exact_provider_budget_v1::{
    PreparedSharedPhysicalOwnership, ProjectionSourceWorkReservationV1,
    ProjectionSourceWorkingBudgetV1, ProviderBudget, Reservation, SharedPhysicalOwnershipCell,
};
use riffdb_types::{CommitSequence, OwnedCapacitySharedV1};
use std::sync::Mutex;

#[derive(Clone, Copy)]
struct Position {
    token: u64,
    lineage: ChangelogLineageV3,
    base_through: u64,
    captured_through: CommitSequence,
    receipt_floor: Option<ChangelogHistoryPointV3>,
}
#[derive(Default)]
struct State {
    active: Option<Position>,
    next_token: u64,
    reserved_floor: Option<(u64, u64)>,
    reservation_token: u64,
    receipt_reserved_floor: Option<(ChangelogLineageV3, ChangelogHistoryPointV3)>,
    receipt_reservation_token: u64,
    refused: bool,
}

#[derive(Clone)]
pub(crate) struct SourcePrefixPins(OwnedCapacitySharedV1<SourcePrefixPinsInner>);
pub(crate) struct SourcePrefixPinsInner {
    state: Mutex<State>,
    budget: ProjectionSourceWorkingBudgetV1,
    provider: SharedPhysicalOwnershipCell,
    _control: ProjectionSourceWorkReservationV1,
}
impl std::ops::Deref for SourcePrefixPins {
    type Target = SourcePrefixPinsInner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl SourcePrefixPins {
    pub(crate) fn new(budget: &ProjectionSourceWorkingBudgetV1) -> Result<Self, StorageError> {
        let bytes = allocation_bound::<SourcePrefixPinsInner>()?;
        let control = budget.reserve(bytes).map_err(|_| unavailable())?;
        Ok(Self(OwnedCapacitySharedV1::new(SourcePrefixPinsInner {
            state: Mutex::new(State::default()),
            budget: budget.clone(),
            provider: SharedPhysicalOwnershipCell::new(),
            _control: control,
        })))
    }
    pub(crate) fn control_charge_bytes(&self) -> u64 {
        self._control.bytes()
    }
    pub(crate) fn provider_ownership_needed(
        &self,
        budget: &ProviderBudget,
    ) -> Result<u64, StorageError> {
        self.provider
            .provider_ownership_needed(self.control_charge_bytes(), budget)
            .map_err(|_| unavailable())
    }
    pub(crate) fn prepare_provider_ownership_from(
        &self,
        work: &mut Reservation,
        budget: &ProviderBudget,
    ) -> Result<PreparedSharedPhysicalOwnership<'_>, StorageError> {
        self.provider
            .prepare_provider_ownership_from(self.control_charge_bytes(), work, budget)
            .map_err(|_| unavailable())
    }
    #[cfg(test)]
    pub(crate) fn provider_charge_bytes(&self) -> u64 {
        self.provider.owned_bytes().unwrap()
    }

    // A single active worker lease follows ADR-0316's one-worker/database
    // bound. Generation/provider closures are a separate integration boundary.
    #[cfg(test)]
    pub(crate) fn try_register(
        &self,
        lineage: ChangelogLineageV3,
        base_through: u64,
        captured_through: CommitSequence,
    ) -> Result<OwnedCapacitySharedV1<SourcePrefixPin>, StorageError> {
        self.try_register_position(lineage, base_through, captured_through, None)
    }
    pub(crate) fn try_register_captured(
        &self,
        history: ChangelogHistoryStateV3,
        base_through: u64,
    ) -> Result<OwnedCapacitySharedV1<SourcePrefixPin>, StorageError> {
        let captured = history
            .tail()
            .frontier()
            .application()
            .ok_or_else(unavailable)?;
        self.try_register_position(
            history.lineage(),
            base_through,
            captured,
            Some(history.minimum_resume()),
        )
    }
    fn try_register_position(
        &self,
        lineage: ChangelogLineageV3,
        base_through: u64,
        captured_through: CommitSequence,
        receipt_floor: Option<ChangelogHistoryPointV3>,
    ) -> Result<OwnedCapacitySharedV1<SourcePrefixPin>, StorageError> {
        if base_through > captured_through.get() {
            return Err(unavailable());
        }
        // Reserve the actual fixed Arc/control allocation before construction.
        // Any separately retained semantic source metadata has its own already
        // admitted owner in the native capture, never an uncharged clone here.
        let control = self
            .budget
            .reserve(allocation_bound::<SourcePrefixPin>()?)
            .map_err(|_| unavailable())?;
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        if state.refused || state.active.is_some() {
            return Err(unavailable());
        }
        if let Some((owner, floor)) = state.reserved_floor {
            if owner != lineage.history_incarnation() {
                return Err(unavailable());
            }
            if base_through < floor {
                return Err(history_pruned());
            }
        }
        if let Some((owner, reserved)) = state.receipt_reserved_floor {
            if owner != lineage {
                return Err(unavailable());
            }
            if receipt_floor.is_some_and(|point| point.sequence() < reserved.sequence()) {
                return Err(history_pruned());
            }
        }
        let token = state.next_token.checked_add(1).ok_or_else(unavailable)?;
        let position = Position {
            token,
            lineage,
            base_through,
            captured_through,
            receipt_floor,
        };
        let pin = OwnedCapacitySharedV1::new(SourcePrefixPin {
            position,
            owner: self.clone(),
            _control: control,
        });
        state.next_token = token;
        state.active = Some(position);
        Ok(pin)
    }
    #[cfg(test)]
    pub(crate) fn application_floor(
        &self,
        lineage: ChangelogLineageV3,
    ) -> Result<Option<u64>, StorageError> {
        let state = self.state.lock().map_err(|_| unavailable())?;
        if state.refused {
            return Err(unavailable());
        }
        match state.active {
            Some(position) if position.lineage != lineage => Err(unavailable()),
            Some(position) => Ok(Some(position.base_through)),
            None => Ok(None),
        }
    }
    // Native retention already verifies this incarnation from the SAME final
    // gate read as the prune preimages. A process-local fence only denies
    // deletion; it grants no authority from a scalar incarnation or locator.
    pub(crate) fn application_floor_in_incarnation(
        &self,
        incarnation: u64,
    ) -> Result<Option<u64>, StorageError> {
        let state = self.state.lock().map_err(|_| unavailable())?;
        if state.refused {
            return Err(unavailable());
        }
        match state.active {
            Some(position) if position.lineage.history_incarnation() != incarnation => {
                Err(unavailable())
            }
            Some(position) => Ok(Some(position.base_through)),
            None => Ok(None),
        }
    }

    // The canonical retained receipt chain is independent of application
    // pruning. This compact logical fence retains no primary read transaction.
    pub(crate) fn receipt_floor(
        &self,
        lineage: ChangelogLineageV3,
    ) -> Result<Option<u64>, StorageError> {
        let state = self.state.lock().map_err(|_| unavailable())?;
        if state.refused {
            return Err(unavailable());
        }
        match state.active {
            Some(position) if position.lineage != lineage => Err(unavailable()),
            Some(position) => Ok(position.receipt_floor.map(|point| point.sequence().get())),
            None => Ok(None),
        }
    }
    // Invoked under the existing source gate before staging/submitting a cut.
    // Known pre-submit abort rolls back, unknown outcomes retain the floor.
    pub(crate) fn prepare_receipt_prune(
        &self,
        lineage: ChangelogLineageV3,
        through: ChangelogHistoryPointV3,
    ) -> Result<ReceiptPruneReservation, StorageError> {
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        if state.refused {
            return Err(unavailable());
        }
        if let Some(position) = state.active {
            if position.lineage != lineage {
                return Err(unavailable());
            }
            if position
                .receipt_floor
                .is_some_and(|point| through.sequence() > point.sequence())
            {
                return Err(history_pruned());
            }
        }
        let previous = state.receipt_reserved_floor;
        let floor = match previous {
            Some((owner, prior)) if owner == lineage => {
                if prior.sequence() > through.sequence() {
                    prior
                } else {
                    through
                }
            }
            Some(_) => return Err(unavailable()),
            None => through,
        };
        let token = state
            .receipt_reservation_token
            .checked_add(1)
            .ok_or_else(unavailable)?;
        state.receipt_reservation_token = token;
        state.receipt_reserved_floor = Some((lineage, floor));
        Ok(ReceiptPruneReservation {
            owner: self.clone(),
            token,
            previous,
            submitted: false,
        })
    }

    // Final gate admission, before destructive staging/submission. A capture
    // that follows submission observes the conservative floor even if the
    // published watermark has not advanced or the outcome becomes unknown.
    #[cfg(test)]
    pub(crate) fn prepare_prune(
        &self,
        lineage: ChangelogLineageV3,
        through: u64,
    ) -> Result<PruneReservation, StorageError> {
        self.prepare_prune_in_incarnation(lineage.history_incarnation(), through)
    }
    pub(crate) fn prepare_prune_in_incarnation(
        &self,
        incarnation: u64,
        through: u64,
    ) -> Result<PruneReservation, StorageError> {
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        if state.refused {
            return Err(unavailable());
        }
        if let Some(position) = state.active {
            if position.lineage.history_incarnation() != incarnation {
                return Err(unavailable());
            }
            if through > position.base_through {
                return Err(history_pruned());
            }
        }
        let previous = state.reserved_floor;
        let floor = match previous {
            Some((owner, prior)) if owner == incarnation => prior.max(through),
            Some(_) => return Err(unavailable()),
            None => through,
        };
        let token = state
            .reservation_token
            .checked_add(1)
            .ok_or_else(unavailable)?;
        state.reservation_token = token;
        state.reserved_floor = Some((incarnation, floor));
        Ok(PruneReservation {
            owner: self.clone(),
            token,
            previous,
            submitted: false,
        })
    }
}

pub(crate) struct SourcePrefixPin {
    position: Position,
    owner: SourcePrefixPins,
    _control: ProjectionSourceWorkReservationV1,
}
impl SourcePrefixPin {
    #[cfg(test)]
    pub(crate) fn control_charge_bytes_for_test(&self) -> u64 {
        self._control.bytes()
    }
    pub(crate) const fn captured_through(&self) -> CommitSequence {
        self.position.captured_through
    }
    pub(crate) fn belongs_to_owner(&self, owner: &SourcePrefixPins) -> bool {
        self.owner.0.same_allocation(&owner.0)
    }
}
impl Drop for SourcePrefixPin {
    fn drop(&mut self) {
        let Ok(mut state) = self.owner.state.lock() else {
            return;
        };
        if state
            .active
            .is_some_and(|position| position.token == self.position.token)
        {
            state.active = None;
        } else {
            state.refused = true;
        }
    }
}

pub(crate) struct PruneReservation {
    owner: SourcePrefixPins,
    token: u64,
    previous: Option<(u64, u64)>,
    submitted: bool,
}
impl PruneReservation {
    // Mark BEFORE invoking submit/commit: even a returned error can describe an
    // unknown durable outcome. Only a known pre-submission abort rolls back.
    pub(crate) fn submission_started(&mut self) {
        self.submitted = true;
    }
}
impl Drop for PruneReservation {
    fn drop(&mut self) {
        if self.submitted {
            return;
        }
        let Ok(mut state) = self.owner.state.lock() else {
            return;
        };
        if state.reservation_token == self.token {
            state.reserved_floor = self.previous;
        }
    }
}
pub(crate) struct ReceiptPruneReservation {
    owner: SourcePrefixPins,
    token: u64,
    previous: Option<(ChangelogLineageV3, ChangelogHistoryPointV3)>,
    submitted: bool,
}
impl ReceiptPruneReservation {
    pub(crate) fn submission_started(&mut self) {
        self.submitted = true;
    }
}
impl Drop for ReceiptPruneReservation {
    fn drop(&mut self) {
        if self.submitted {
            return;
        }
        let Ok(mut state) = self.owner.state.lock() else {
            return;
        };
        if state.receipt_reservation_token == self.token {
            state.receipt_reserved_floor = self.previous;
        }
    }
}

pub(crate) fn allocation_bound<T>() -> Result<u64, StorageError> {
    #[repr(C)]
    struct AllocationLayout<T> {
        counts: [usize; 2],
        value: T,
    }
    riffdb_types::owned_allocation_capacity_bound_v1(
        std::mem::size_of::<AllocationLayout<T>>(),
        std::mem::align_of::<AllocationLayout<T>>(),
    )
    .map_err(|_| unavailable())
}
fn unavailable() -> StorageError {
    StorageError::new(StorageErrorKind::Unavailable, None)
}
fn history_pruned() -> StorageError {
    StorageError::new(StorageErrorKind::HistoryPruned, None)
}

#[cfg(test)]
#[path = "source_prefix_pin_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "source_prefix_receipt_pin_tests.rs"]
mod receipt_tests;
