//! Destruction bookkeeping with closed control storage and scoped attachment.
// req: OQ-040

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Mutex;

use crate::{
    InspectOwnedCapacityV1, OwnedCapacityInspectionErrorV1, OwnedCapacityInspectionV1,
    OwnedCapacitySharedV1,
};

type AttachedOwner = crate::exact_provider_budget_v1::SharedOwnershipCharge;
struct State {
    live: bool,
    owner: Option<AttachedOwner>,
}
struct Control {
    state: Mutex<State>,
}
impl Control {
    fn attached_owner(&self) -> Result<Option<AttachedOwner>, OwnedCapacityInspectionErrorV1> {
        let state = self
            .state
            .lock()
            .map_err(|_| OwnedCapacityInspectionErrorV1)?;
        if !state.live {
            return Err(OwnedCapacityInspectionErrorV1);
        }
        Ok(state.owner.clone())
    }
    fn attach(
        &self,
        candidate: AttachedOwner,
    ) -> Result<AttachedOwner, OwnedCapacityInspectionErrorV1> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| OwnedCapacityInspectionErrorV1)?;
        if !state.live {
            return Err(OwnedCapacityInspectionErrorV1);
        }
        match state.owner.as_ref() {
            Some(existing) => Ok(existing.clone()),
            None => {
                state.owner = Some(candidate.clone());
                Ok(candidate)
            }
        }
    }
}

/// Store after the data it protects. Deep cloning creates a fresh control;
/// cloning a closed semantic owner retains that owner's existing lifecycle.
#[doc(hidden)]
pub struct OwnedCapacityLifecycleV1 {
    control: OwnedCapacitySharedV1<Control>,
}
impl Default for OwnedCapacityLifecycleV1 {
    fn default() -> Self {
        Self {
            control: OwnedCapacitySharedV1::new(Control {
                state: Mutex::new(State {
                    live: true,
                    owner: None,
                }),
            }),
        }
    }
}
impl Clone for OwnedCapacityLifecycleV1 {
    fn clone(&self) -> Self {
        Self::default()
    }
}
impl Drop for OwnedCapacityLifecycleV1 {
    fn drop(&mut self) {
        // Poison remains visible to admission. Destruction may recover solely
        // to mark dead; attached charge retires after the final physical control.
        let mut state = self
            .control
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.live = false;
    }
}
impl OwnedCapacityLifecycleV1 {
    /// Exact concrete control layout under the inspector's allocator envelope.
    pub fn control_allocation_upper_bound() -> Result<u64, OwnedCapacityInspectionErrorV1> {
        crate::owned_capacity::allocation_upper_bound(
            std::mem::size_of::<Control>() + 2 * std::mem::size_of::<usize>(),
            std::mem::align_of::<Control>(),
        )
    }
    /// First attachment can only borrow this actual live lifecycle. A report's
    /// carrier keeps its closed semantic owner alive through every such borrow.
    pub fn reference(&self) -> OwnedCapacityLifecycleReferenceV1<'_> {
        OwnedCapacityLifecycleReferenceV1(&self.control)
    }
    /// Persistent observers retain numerical control only. An unattached control
    /// cannot escape the construction reservation via this method.
    pub fn observer(
        &self,
    ) -> Result<OwnedCapacityLifecycleObserverV1, OwnedCapacityInspectionErrorV1> {
        let state = self
            .control
            .state
            .lock()
            .map_err(|_| OwnedCapacityInspectionErrorV1)?;
        if !state.live || state.owner.is_none() {
            return Err(OwnedCapacityInspectionErrorV1);
        }
        Ok(OwnedCapacityLifecycleObserverV1(self.control.clone()))
    }
}
impl fmt::Debug for OwnedCapacityLifecycleV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnedCapacityLifecycleV1([REDACTED])")
    }
}
impl PartialEq for OwnedCapacityLifecycleV1 {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for OwnedCapacityLifecycleV1 {}
impl PartialOrd for OwnedCapacityLifecycleV1 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OwnedCapacityLifecycleV1 {
    fn cmp(&self, _: &Self) -> Ordering {
        Ordering::Equal
    }
}
impl Hash for OwnedCapacityLifecycleV1 {
    fn hash<H: Hasher>(&self, _: &mut H) {}
}
impl InspectOwnedCapacityV1 for OwnedCapacityLifecycleV1 {
    fn inspect_owned_capacity(
        &self,
        inspection: &mut OwnedCapacityInspectionV1,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        inspection.allocation(
            std::mem::size_of::<Control>() + 2 * std::mem::size_of::<usize>(),
            std::mem::align_of::<Control>(),
        )
    }
}

/// A report-scoped attachment borrow, never a detached weak semantic shell.
#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct OwnedCapacityLifecycleReferenceV1<'owner>(&'owner Control);
impl fmt::Debug for OwnedCapacityLifecycleReferenceV1<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnedCapacityLifecycleReferenceV1([REDACTED])")
    }
}
impl OwnedCapacityLifecycleReferenceV1<'_> {
    pub fn attached_owner(&self) -> Result<Option<AttachedOwner>, OwnedCapacityInspectionErrorV1> {
        self.0.attached_owner()
    }
    pub fn attach(
        &self,
        candidate: AttachedOwner,
    ) -> Result<AttachedOwner, OwnedCapacityInspectionErrorV1> {
        self.0.attach(candidate)
    }
}

/// An already charged numerical/control observer; cannot retain or revive data.
#[doc(hidden)]
#[derive(Clone)]
pub struct OwnedCapacityLifecycleObserverV1(OwnedCapacitySharedV1<Control>);
impl fmt::Debug for OwnedCapacityLifecycleObserverV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnedCapacityLifecycleObserverV1([REDACTED])")
    }
}
impl OwnedCapacityLifecycleObserverV1 {
    /// Numerical control identity only; this grants no data access or upgrade.
    pub fn observes(&self, lifecycle: &OwnedCapacityLifecycleV1) -> bool {
        self.0.same_allocation(&lifecycle.control)
    }
    pub fn attached_owner(&self) -> Result<Option<AttachedOwner>, OwnedCapacityInspectionErrorV1> {
        self.0.attached_owner()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exact_provider_budget_v1::ProviderBudget;
    use std::sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering as AtomicOrdering},
    };

    struct Data {
        destroyed: Arc<AtomicBool>,
        budget: ProviderBudget,
    }
    impl Drop for Data {
        fn drop(&mut self) {
            assert_eq!(self.budget.observation().unwrap().owned_bytes, 128);
            self.destroyed.store(true, AtomicOrdering::SeqCst);
        }
    }
    struct Allocation {
        _data: Data,
        // The lifecycle must follow every charged data field.
        lifecycle: OwnedCapacityLifecycleV1,
    }

    #[test]
    fn shared_allocation_charge_survives_captures_and_releases_after_data_destruction() {
        let budget = ProviderBudget::new();
        let destroyed = Arc::new(AtomicBool::new(false));
        let allocation = OwnedCapacitySharedV1::new(Allocation {
            _data: Data {
                destroyed: destroyed.clone(),
                budget: budget.clone(),
            },
            lifecycle: OwnedCapacityLifecycleV1::default(),
        });
        let captured = allocation.clone();
        let charge = budget.reserve(128).unwrap().commit().unwrap();
        drop(
            allocation
                .lifecycle
                .reference()
                .attach(charge.clone())
                .unwrap(),
        );
        drop(charge);
        drop(allocation);
        assert!(!destroyed.load(AtomicOrdering::SeqCst));
        assert_eq!(budget.observation().unwrap().owned_bytes, 128);
        drop(captured);
        assert!(destroyed.load(AtomicOrdering::SeqCst));
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        assert_eq!(budget.observation().unwrap().released_allocations, 1);
    }

    #[test]
    fn lifecycle_reference_cannot_keep_data_alive_or_revive_destroyed_ownership() {
        let lifecycle = OwnedCapacityLifecycleV1::default();
        let budget = ProviderBudget::new();
        assert!(lifecycle.observer().is_err());
        lifecycle
            .reference()
            .attach(budget.reserve(128).unwrap().commit().unwrap())
            .unwrap();
        let observer = lifecycle.observer().unwrap();
        drop(lifecycle);
        assert!(observer.attached_owner().is_err());
        assert_eq!(budget.observation().unwrap().owned_bytes, 128);
        assert_eq!(
            format!("{observer:?}"),
            "OwnedCapacityLifecycleObserverV1([REDACTED])"
        );
        drop(observer);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }

    #[test]
    fn first_attachment_is_immutable_and_deep_clone_has_its_own_lifecycle() {
        let lifecycle = OwnedCapacityLifecycleV1::default();
        let budget = ProviderBudget::new();
        let first = budget.reserve(128).unwrap().commit().unwrap();
        let attached = lifecycle.reference().attach(first.clone()).unwrap();
        assert!(attached.same_allocation(&first));
        let retained = lifecycle
            .reference()
            .attach(budget.reserve(256).unwrap().commit().unwrap())
            .unwrap();
        assert!(retained.same_allocation(&first));
        let cloned = lifecycle.clone();
        let independent = cloned
            .reference()
            .attach(budget.reserve(256).unwrap().commit().unwrap())
            .unwrap();
        assert!(!independent.same_allocation(&first));
        assert_eq!(independent.bytes(), 256);
        assert_eq!(lifecycle, cloned);
    }

    #[test]
    fn concurrent_attachments_have_one_immutable_owner_and_cancel_only_the_loser() {
        let lifecycle = OwnedCapacityLifecycleV1::default();
        let budget = ProviderBudget::new();
        let start = Arc::new(Barrier::new(3));
        let owners = std::thread::scope(|scope| {
            let workers = (0..2)
                .map(|_| {
                    let reference = lifecycle.reference();
                    let start = start.clone();
                    let budget = budget.clone();
                    scope.spawn(move || {
                        let candidate = budget.reserve(128).unwrap().commit().unwrap();
                        start.wait();
                        reference.attach(candidate).unwrap()
                    })
                })
                .collect::<Vec<_>>();
            start.wait();
            workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert!(owners[0].same_allocation(&owners[1]));
        assert_eq!(budget.observation().unwrap().owned_bytes, 128);
        assert_eq!(budget.observation().unwrap().released_allocations, 1);
        drop(owners);
        assert_eq!(budget.observation().unwrap().owned_bytes, 128);
        drop(lifecycle);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        assert_eq!(budget.observation().unwrap().released_allocations, 2);
    }
}
