//! Private candidate population admission and final source ownership.
use super::{MAX_PROJECTED_POLICY_CANDIDATES_V1, RebuildFailure};
use riffdb_types::{
    EntityKey, OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1,
    exact_provider_budget_v1::{ProviderAllocation, ProviderBudget, Reservation},
};
use std::{
    collections::BTreeSet,
    mem::{align_of, size_of},
    ops::Deref,
    sync::Arc,
};

pub(in crate::exact_text_adapter) type AdmittedCandidatePopulation =
    Arc<ProviderAllocation<BTreeSet<EntityKey>>>;

pub(in crate::exact_text_adapter) struct CandidatePopulation {
    keys: BTreeSet<EntityKey>,
    budget: ProviderBudget,
    // Last: private keys drop before their admitted construction capacity.
    work: Reservation,
}
impl CandidatePopulation {
    // Same full eleven-slot internal-node-per-entry bound as the native B-tree
    // inspector. Include one empty root in the initial allowance. Key buffers
    // are reserved separately before cloning; removals keep conservative work.
    const NODE_BYTES: u64 = (2
        * (11 * size_of::<EntityKey>() + 256 + align_of::<EntityKey>() + align_of::<()>()))
        as u64;
    pub(in crate::exact_text_adapter) fn new(
        budget: &ProviderBudget,
    ) -> Result<Self, RebuildFailure> {
        let work = budget
            .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 + 32 * 1024 + Self::NODE_BYTES)
            .map_err(|_| RebuildFailure::Transient)?;
        Ok(Self {
            keys: BTreeSet::new(),
            budget: budget.clone(),
            work,
        })
    }
    pub(in crate::exact_text_adapter) fn copy_from(
        budget: &ProviderBudget,
        keys: &BTreeSet<EntityKey>,
    ) -> Result<Self, RebuildFailure> {
        let mut population = Self::new(budget)?;
        for key in keys {
            population.insert(key)?;
        }
        Ok(population)
    }
    pub(in crate::exact_text_adapter) fn insert(
        &mut self,
        key: &EntityKey,
    ) -> Result<bool, RebuildFailure> {
        if self.keys.contains(key) {
            return Ok(false);
        }
        if self.keys.len() >= MAX_PROJECTED_POLICY_CANDIDATES_V1 {
            return Err(RebuildFailure::Integrity);
        }
        let bytes = key
            .as_bytes()
            .len()
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(2 * align_of::<u8>() + 256))
            .and_then(|bytes| u64::try_from(bytes).ok())
            .and_then(|bytes| bytes.checked_add(Self::NODE_BYTES))
            .ok_or(RebuildFailure::Transient)?;
        let incremental = self
            .budget
            .reserve(bytes)
            .map_err(|_| RebuildFailure::Transient)?;
        self.work
            .absorb(incremental)
            .map_err(|_| RebuildFailure::Transient)?;
        Ok(self.keys.insert(key.clone()))
    }
    pub(in crate::exact_text_adapter) fn remove(&mut self, key: &EntityKey) -> bool {
        self.keys.remove(key)
    }
    pub(in crate::exact_text_adapter) fn finish(
        self,
    ) -> Result<AdmittedCandidatePopulation, RebuildFailure> {
        self.work
            .settle_owned_value(self.keys)
            .map_err(|_| RebuildFailure::Transient)
    }
}
impl Deref for CandidatePopulation {
    type Target = BTreeSet<EntityKey>;
    fn deref(&self) -> &Self::Target {
        &self.keys
    }
}
