// req: OQ-040
//! Registration admission over one compiled process graph budget.

use riffdb_types::exact_provider_budget_v1::{
    BudgetUnavailable, OwnershipCharge, ProviderBudget, Reservation,
};
use riffdb_types::{
    OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1, OwnedCapacityInspectionErrorV1,
    OwnedCapacityInspectionV1, OwnedCapacityLifecycleReferenceV1, SharedAllocationIdentityV1,
};
use std::sync::{Arc, OnceLock};

static PROCESS_GRAPH_BUDGET: OnceLock<ProviderBudget> = OnceLock::new();
const REGISTRY_METADATA_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ProviderAdmissionError {
    Capacity,
    Integrity,
}

pub(super) struct ProviderOwnershipGraph {
    pub(super) budget: ProviderBudget,
    // Last: registry data is destroyed before its fixed metadata allowance.
    _registry_metadata: Arc<OwnershipCharge>,
}
struct NeededCharge {
    identity: SharedAllocationIdentityV1,
    bytes: u64,
    lifecycle: OwnedCapacityLifecycleReferenceV1,
}
impl ProviderOwnershipGraph {
    pub(super) fn new() -> Result<Self, BudgetUnavailable> {
        let budget = PROCESS_GRAPH_BUDGET
            .get_or_init(ProviderBudget::new)
            .clone();
        let metadata = budget.reserve(REGISTRY_METADATA_BYTES)?.commit()?;
        Ok(Self {
            budget,
            _registry_metadata: metadata,
        })
    }
    #[cfg(test)]
    pub(crate) fn isolated_for_test() -> Self {
        let budget = ProviderBudget::new();
        let metadata = budget
            .reserve(REGISTRY_METADATA_BYTES)
            .unwrap()
            .commit()
            .unwrap();
        Self {
            budget,
            _registry_metadata: metadata,
        }
    }
    pub(super) fn metadata_owner(&self) -> Arc<OwnershipCharge> {
        self._registry_metadata.clone()
    }
    pub(super) fn admit(
        &self,
        additional_inline_bytes: u64,
        inspect: impl FnOnce(
            &mut OwnedCapacityInspectionV1,
        ) -> Result<(), OwnedCapacityInspectionErrorV1>,
    ) -> Result<Reservation, ProviderAdmissionError> {
        // Before both the observation census and its bounded admission plan.
        let scratch = self
            .budget
            .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
            .map_err(|_| self.reservation_failure())?;
        let report = OwnedCapacityInspectionV1::inspect_with(inspect)
            .map_err(|_| ProviderAdmissionError::Integrity)?;
        let root_bytes = report
            .unshared_bytes()
            .checked_add(additional_inline_bytes)
            .ok_or(ProviderAdmissionError::Integrity)?;
        let mut needed = Vec::with_capacity(report.shared_allocations().len());
        let mut total = root_bytes;
        for allocation in report.shared_allocations() {
            let lifecycle = allocation
                .lifecycle()
                .ok_or(ProviderAdmissionError::Integrity)?;
            match lifecycle
                .attached_owner()
                .map_err(|_| ProviderAdmissionError::Integrity)?
            {
                Some(owner) => self
                    .validate(&owner, allocation.identity(), allocation.bytes())
                    .map_err(|_| ProviderAdmissionError::Integrity)?,
                None => {
                    total = total
                        .checked_add(allocation.bytes())
                        .ok_or(ProviderAdmissionError::Integrity)?;
                    needed.push(NeededCharge {
                        identity: allocation.identity(),
                        bytes: allocation.bytes(),
                        lifecycle: lifecycle.clone(),
                    });
                }
            }
        }
        let mut admitted = self
            .budget
            .reserve(total)
            .map_err(|_| self.reservation_failure())?;
        for allocation in needed {
            let candidate = admitted
                .split(allocation.bytes)
                .map_err(|_| ProviderAdmissionError::Integrity)?
                .commit_for_allocation(allocation.identity)
                .map_err(|_| ProviderAdmissionError::Integrity)?;
            let owner = allocation
                .lifecycle
                .attach(candidate)
                .map_err(|_| ProviderAdmissionError::Integrity)?;
            // A concurrent winner or foreign attachment cannot skip validation.
            self.validate(&owner, allocation.identity, allocation.bytes)
                .map_err(|_| ProviderAdmissionError::Integrity)?;
        }
        // Original data references and matching sidecars drop before scratch.
        drop(report);
        drop(scratch);
        Ok(admitted)
    }
    fn reservation_failure(&self) -> ProviderAdmissionError {
        if self.budget.observation().is_ok() {
            ProviderAdmissionError::Capacity
        } else {
            ProviderAdmissionError::Integrity
        }
    }
    fn validate(
        &self,
        owner: &OwnershipCharge,
        identity: SharedAllocationIdentityV1,
        minimum: u64,
    ) -> Result<(), BudgetUnavailable> {
        if owner.belongs_to(&self.budget)
            && owner.allocation() == Some(identity)
            && owner.bytes() >= minimum
        {
            Ok(())
        } else {
            Err(BudgetUnavailable)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::{InspectOwnedCapacityV1, OwnedCapacityLifecycleV1};

    struct Compiled {
        bytes: String,
        lifecycle: OwnedCapacityLifecycleV1,
    }
    impl InspectOwnedCapacityV1 for Compiled {
        fn inspect_owned_capacity(
            &self,
            inspection: &mut OwnedCapacityInspectionV1,
        ) -> Result<(), OwnedCapacityInspectionErrorV1> {
            inspection.visit(&self.bytes)?;
            inspection.visit(&self.lifecycle)
        }
        fn allocation_lifecycle(&self) -> Option<OwnedCapacityLifecycleReferenceV1> {
            Some(self.lifecycle.reference())
        }
    }
    fn compiled() -> Arc<Compiled> {
        Arc::new(Compiled {
            bytes: String::with_capacity(4096),
            lifecycle: OwnedCapacityLifecycleV1::default(),
        })
    }
    fn graph() -> ProviderOwnershipGraph {
        let budget = ProviderBudget::new();
        let metadata = budget
            .reserve(REGISTRY_METADATA_BYTES)
            .unwrap()
            .commit()
            .unwrap();
        ProviderOwnershipGraph {
            budget,
            _registry_metadata: metadata,
        }
    }
    fn admit(graph: &ProviderOwnershipGraph, value: &Arc<Compiled>) -> Arc<OwnershipCharge> {
        graph
            .admit(1024, |inspection| inspection.visit(value))
            .unwrap()
            .commit()
            .unwrap()
    }

    #[test]
    fn shared_compilation_is_charged_once_until_its_actual_final_reference() {
        let graph = graph();
        let value = compiled();
        let first = admit(&graph, &value);
        let charged = graph.budget.observation().unwrap().owned_bytes;
        let second = admit(&graph, &value);
        assert_eq!(
            graph.budget.observation().unwrap().owned_bytes,
            charged + 1024
        );
        drop(first);
        drop(second);
        let captured = value.clone();
        drop(value);
        assert!(graph.budget.observation().unwrap().owned_bytes > REGISTRY_METADATA_BYTES);
        drop(captured);
        let observation = graph.budget.observation().unwrap();
        assert_eq!(
            (observation.owned_bytes, observation.reserved_bytes),
            (REGISTRY_METADATA_BYTES, 0)
        );
    }

    #[test]
    fn shared_allocation_without_a_destruction_proof_is_refused() {
        let graph = graph();
        let value = Arc::new(String::with_capacity(4096));
        assert!(
            graph
                .admit(1024, |inspection| inspection.visit(&value))
                .is_err()
        );
        let observation = graph.budget.observation().unwrap();
        assert_eq!(
            (observation.owned_bytes, observation.reserved_bytes),
            (REGISTRY_METADATA_BYTES, 0)
        );
    }

    #[test]
    fn foreign_budget_attachment_cannot_bypass_aggregate_admission() {
        let first = graph();
        let second = graph();
        let value = compiled();
        let root = admit(&first, &value);
        assert!(
            second
                .admit(1024, |inspection| inspection.visit(&value))
                .is_err()
        );
        assert_eq!(second.budget.observation().unwrap().reserved_bytes, 0);
        assert_eq!(
            second.budget.observation().unwrap().owned_bytes,
            REGISTRY_METADATA_BYTES
        );
        drop(root);
        drop(value);
        assert_eq!(
            first.budget.observation().unwrap().owned_bytes,
            REGISTRY_METADATA_BYTES
        );
    }

    #[test]
    fn saturation_refuses_before_attaching_any_shared_charge() {
        let graph = graph();
        let value = compiled();
        let observed = graph.budget.observation().unwrap();
        let remaining = riffdb_types::exact_provider_budget_v1::EXACT_PROVIDER_BUDGET_BYTES
            - observed.fixed_owner_bytes
            - observed.owned_bytes;
        let saturation = graph.budget.reserve(remaining).unwrap();
        assert!(
            graph
                .admit(1024, |inspection| inspection.visit(&value))
                .is_err()
        );
        assert!(
            value
                .lifecycle
                .reference()
                .attached_owner()
                .unwrap()
                .is_none()
        );
        drop(saturation);
        assert_eq!(graph.budget.observation().unwrap().reserved_bytes, 0);
        assert!(
            graph
                .admit(1024, |inspection| inspection.visit(&value))
                .is_ok()
        );
    }

    #[test]
    fn wrong_allocation_or_undersized_attached_charge_is_refused() {
        for wrong_identity in [false, true] {
            let graph = graph();
            let value = compiled();
            let other = compiled();
            let scratch = graph
                .budget
                .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
                .unwrap();
            let report =
                OwnedCapacityInspectionV1::inspect(if wrong_identity { &other } else { &value })
                    .unwrap();
            let identity = report.shared_allocations()[0].identity();
            let charge = graph
                .budget
                .reserve(128)
                .unwrap()
                .commit_for_allocation(identity)
                .unwrap();
            value.lifecycle.reference().attach(charge).unwrap();
            drop(report);
            drop(scratch);
            assert!(
                graph
                    .admit(1024, |inspection| inspection.visit(&value))
                    .is_err()
            );
            assert_eq!(graph.budget.observation().unwrap().reserved_bytes, 0);
            assert_eq!(
                graph.budget.observation().unwrap().owned_bytes,
                REGISTRY_METADATA_BYTES + 128
            );
            drop(value);
            drop(other);
            assert_eq!(
                graph.budget.observation().unwrap().owned_bytes,
                REGISTRY_METADATA_BYTES
            );
        }
    }

    #[test]
    fn simultaneous_admissions_attach_one_shared_charge_and_keep_both_roots() {
        use std::sync::Barrier;
        let graph = Arc::new(graph());
        let value = compiled();
        let barrier = Arc::new(Barrier::new(3));
        let tasks = (0..2)
            .map(|_| {
                let graph = graph.clone();
                let value = value.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    admit(&graph, &value)
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let roots = tasks
            .into_iter()
            .map(|task| task.join().unwrap())
            .collect::<Vec<_>>();
        let shared = value
            .lifecycle
            .reference()
            .attached_owner()
            .unwrap()
            .unwrap()
            .bytes();
        let observed = graph.budget.observation().unwrap();
        assert_eq!(
            (observed.owned_bytes, observed.reserved_bytes),
            (REGISTRY_METADATA_BYTES + shared + 2048, 0)
        );
        drop(roots);
        drop(value);
        assert_eq!(
            graph.budget.observation().unwrap().owned_bytes,
            REGISTRY_METADATA_BYTES
        );
    }
}
