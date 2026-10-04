//! Candidate history ownership must survive provider and source aliases.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::{
    EntityKey, EntityKeyBuilder, EntityTypeId,
    exact_provider_budget_v1::{EXACT_PROVIDER_BUDGET_BYTES, ProviderBudget},
};

fn key(seed: u32) -> EntityKey {
    let mut key = EntityKeyBuilder::new(EntityTypeId::first());
    let mut uuid = [0; 16];
    uuid[12..].copy_from_slice(&seed.to_be_bytes());
    key.push_uuid(&uuid).unwrap();
    key.finish().unwrap()
}

#[test]
fn source_population_reserves_before_copy_and_keeps_final_capture_charged() {
    let budget = ProviderBudget::new();
    let mut population = CandidatePopulation::new(&budget).unwrap();
    assert!(population.insert(&key(1)).unwrap());
    let before = budget.observation().unwrap();
    assert!(!population.insert(&key(1)).unwrap());
    assert_eq!(budget.observation().unwrap(), before);
    let selected = population.finish().unwrap();
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    assert!(budget.observation().unwrap().owned_bytes > 0);
    let retained = selected.clone();
    let mut successor = CandidatePopulation::copy_from(&budget, &selected).unwrap();
    assert!(successor.remove(&key(1)));
    assert!(successor.insert(&key(2)).unwrap());
    let successor = successor.finish().unwrap();
    assert!(selected.contains(&key(1)));
    assert!(!selected.contains(&key(2)));
    assert!(successor.contains(&key(2)));
    drop(selected);
    drop(successor);
    assert!(budget.observation().unwrap().owned_bytes > 0);
    drop(retained);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

#[test]
fn source_population_pressure_refuses_before_cloning_or_mutating_keys() {
    let budget = ProviderBudget::new();
    let mut population = CandidatePopulation::new(&budget).unwrap();
    let observed = budget.observation().unwrap();
    let pressure = budget
        .reserve(
            EXACT_PROVIDER_BUDGET_BYTES
                - observed.fixed_owner_bytes
                - observed.reserved_bytes
                - observed.owned_bytes,
        )
        .unwrap();
    let before = budget.observation().unwrap();
    assert!(matches!(
        population.insert(&key(1)),
        Err(RebuildFailure::Transient)
    ));
    assert!(population.is_empty());
    assert_eq!(budget.observation().unwrap(), before);
    drop(population);
    drop(pressure);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}

#[test]
fn source_population_native_capacity_is_admitted_at_the_existing_population_ceiling() {
    let budget = ProviderBudget::new();
    let mut population = CandidatePopulation::new(&budget).unwrap();
    for seed in 0..MAX_PROJECTED_POLICY_CANDIDATES_V1 {
        assert!(
            population
                .insert(&key(u32::try_from(seed).unwrap()))
                .unwrap()
        );
    }
    let before = budget.observation().unwrap();
    let native = riffdb_types::OwnedCapacityInspectionV1::inspect(&*population).unwrap();
    assert!(native.shared_allocations().is_empty());
    assert!(native.unshared_bytes() < before.reserved_bytes);
    assert!(matches!(
        population.insert(&key(u32::MAX)),
        Err(RebuildFailure::Integrity)
    ));
    assert_eq!(budget.observation().unwrap(), before);
    let selected = population.finish().unwrap();
    assert_eq!(selected.len(), MAX_PROJECTED_POLICY_CANDIDATES_V1);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    assert!(budget.observation().unwrap().owned_bytes >= native.unshared_bytes());
    drop(selected);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}
