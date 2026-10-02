//! An allocation charge follows data destruction, independently of discovery.
// req: OQ-040

use std::any::Any;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, Weak};

use crate::{InspectOwnedCapacityV1, OwnedCapacityInspectionErrorV1, OwnedCapacityInspectionV1};

type AttachedOwner = Arc<crate::exact_provider_budget_v1::OwnershipCharge>;
struct Control {
    owner: Mutex<Option<AttachedOwner>>,
}

// Closed first-party implementation: opacity keeps ownership bookkeeping out of
// semantic key structure. No caller can provide an implementation or destructor.
trait LifecycleControl: Any + Send + Sync {
    fn attached_owner(&self) -> Result<Option<AttachedOwner>, OwnedCapacityInspectionErrorV1>;
    fn attach(
        &self,
        candidate: AttachedOwner,
    ) -> Result<AttachedOwner, OwnedCapacityInspectionErrorV1>;
}
impl LifecycleControl for Control {
    fn attached_owner(&self) -> Result<Option<AttachedOwner>, OwnedCapacityInspectionErrorV1> {
        let owner = self
            .owner
            .lock()
            .map_err(|_| OwnedCapacityInspectionErrorV1)?;
        Ok(owner.clone())
    }
    fn attach(
        &self,
        candidate: AttachedOwner,
    ) -> Result<AttachedOwner, OwnedCapacityInspectionErrorV1> {
        let mut owner = self
            .owner
            .lock()
            .map_err(|_| OwnedCapacityInspectionErrorV1)?;
        match owner.as_ref() {
            Some(existing) => Ok(existing.clone()),
            None => {
                *owner = Some(candidate.clone());
                Ok(candidate)
            }
        }
    }
}

/// Trusted destruction bookkeeping, excluded from semantic identity and codecs.
/// Store this after the data whose ownership it protects. A deep value clone
/// receives a fresh lifecycle; an Arc clone retains the existing lifecycle.
#[doc(hidden)]
pub struct OwnedCapacityLifecycleV1 {
    control: Arc<dyn LifecycleControl>,
}

impl Default for OwnedCapacityLifecycleV1 {
    fn default() -> Self {
        Self {
            control: Arc::new(Control {
                owner: Mutex::new(None),
            }),
        }
    }
}
impl Clone for OwnedCapacityLifecycleV1 {
    fn clone(&self) -> Self {
        Self::default()
    }
}
impl OwnedCapacityLifecycleV1 {
    /// A weak attachment reference never extends the charged data's lifetime.
    pub fn reference(&self) -> OwnedCapacityLifecycleReferenceV1 {
        OwnedCapacityLifecycleReferenceV1(Arc::downgrade(&self.control))
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
        // The attached numerical charge is accounting machinery, not another
        // semantic allocation to traverse. Its overhead belongs to its owner.
        inspection.allocation(
            std::mem::size_of::<Control>() + 2 * std::mem::size_of::<usize>(),
            std::mem::align_of::<Control>(),
        )
    }
}

/// Opaque weak reference to one allocation's destruction owner.
#[doc(hidden)]
#[derive(Clone)]
pub struct OwnedCapacityLifecycleReferenceV1(Weak<dyn LifecycleControl>);

impl fmt::Debug for OwnedCapacityLifecycleReferenceV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnedCapacityLifecycleReferenceV1([REDACTED])")
    }
}
impl OwnedCapacityLifecycleReferenceV1 {
    pub(crate) fn keepalive(
        &self,
    ) -> Result<Arc<dyn Any + Send + Sync>, OwnedCapacityInspectionErrorV1> {
        self.0
            .upgrade()
            .map(|control| control as Arc<dyn Any + Send + Sync>)
            .ok_or(OwnedCapacityInspectionErrorV1)
    }

    /// Read-only admission observation; the caller validates its concrete owner.
    pub fn attached_owner(&self) -> Result<Option<AttachedOwner>, OwnedCapacityInspectionErrorV1> {
        self.0
            .upgrade()
            .ok_or(OwnedCapacityInspectionErrorV1)?
            .attached_owner()
    }

    /// Keeps the first attachment immutable. The trusted budget caller must
    /// validate the returned owner's allocation and budget identity;
    /// a foreign attachment can only cause typed refusal, never skip a charge.
    pub fn attach(
        &self,
        candidate: AttachedOwner,
    ) -> Result<AttachedOwner, OwnedCapacityInspectionErrorV1> {
        self.0
            .upgrade()
            .ok_or(OwnedCapacityInspectionErrorV1)?
            .attach(candidate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exact_provider_budget_v1::ProviderBudget;
    use std::sync::{
        Barrier,
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
        let allocation = Arc::new(Allocation {
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
        let reference = lifecycle.reference();
        drop(lifecycle);
        let budget = ProviderBudget::new();
        assert!(
            reference
                .attach(budget.reserve(128).unwrap().commit().unwrap())
                .is_err()
        );
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        assert_eq!(
            format!("{reference:?}"),
            "OwnedCapacityLifecycleReferenceV1([REDACTED])"
        );
    }

    #[test]
    fn first_attachment_is_immutable_and_deep_clone_has_its_own_lifecycle() {
        let lifecycle = OwnedCapacityLifecycleV1::default();
        let budget = ProviderBudget::new();
        let first = budget.reserve(128).unwrap().commit().unwrap();
        let attached = lifecycle.reference().attach(first.clone()).unwrap();
        assert!(Arc::ptr_eq(&attached, &first));
        let retained = lifecycle
            .reference()
            .attach(budget.reserve(256).unwrap().commit().unwrap())
            .unwrap();
        assert!(Arc::ptr_eq(&retained, &first));
        let cloned = lifecycle.clone();
        let independent = cloned
            .reference()
            .attach(budget.reserve(256).unwrap().commit().unwrap())
            .unwrap();
        assert!(!Arc::ptr_eq(&independent, &first));
        assert_eq!(independent.bytes(), 256);
        assert_eq!(lifecycle, cloned);
    }

    #[test]
    fn concurrent_attachments_have_one_immutable_owner_and_cancel_only_the_loser() {
        let lifecycle = OwnedCapacityLifecycleV1::default();
        let budget = ProviderBudget::new();
        let start = Arc::new(Barrier::new(3));
        let workers = (0..2)
            .map(|_| {
                let reference = lifecycle.reference();
                let start = start.clone();
                let budget = budget.clone();
                std::thread::spawn(move || {
                    let candidate = budget.reserve(128).unwrap().commit().unwrap();
                    start.wait();
                    reference.attach(candidate).unwrap()
                })
            })
            .collect::<Vec<_>>();
        start.wait();
        let owners = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        assert!(Arc::ptr_eq(&owners[0], &owners[1]));
        assert_eq!(budget.observation().unwrap().owned_bytes, 128);
        assert_eq!(budget.observation().unwrap().released_allocations, 1);
        drop(owners);
        assert_eq!(budget.observation().unwrap().owned_bytes, 128);
        drop(lifecycle);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        assert_eq!(budget.observation().unwrap().released_allocations, 2);
    }
}
