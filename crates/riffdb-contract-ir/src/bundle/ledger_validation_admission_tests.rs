//! Complete ledger checks must admit their native identity workspace first.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::exact_provider_budget_v1::{EXACT_PROVIDER_BUDGET_BYTES, ProviderBudget};

#[test]
fn admitted_ledger_validation_preserves_frozen_identities_and_releases_workspace() {
    for bytes in [
        include_bytes!("../../../../fixtures/compiler/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/reimport/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/text-index/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/production-embedding/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/cascade/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/vector-ann/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/secret/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/initialized-state-transition/bundle.bin")
            .as_slice(),
        include_bytes!("../../../../fixtures/compiler/secret-reveal/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/workflow-initial/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/indexed-selection/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/command-decision/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/aggregate-collection-budget/bundle.bin")
            .as_slice(),
    ] {
        let bundle = ContractBundle::decode(bytes).unwrap();
        let budget = ProviderBudget::new();
        bundle
            .validate_provider_ledger_admitted_v1(&budget)
            .unwrap();
        assert_eq!(bundle.canonical_bytes(), bytes);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
}

#[test]
fn admitted_ledger_validation_native_capacity_witnesses_cover_overlapping_forests() {
    for bytes in [
        include_bytes!("../../../../fixtures/compiler/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/text-index/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/aggregate-collection-budget/bundle.bin")
            .as_slice(),
    ] {
        let mut bundle = ContractBundle::decode(bytes).unwrap();
        // Historical aliases add private identity copies to the final set.
        let entry = bundle
            .ledger
            .allocations
            .iter()
            .flat_map(|allocation| &allocation.entries)
            .find(|entry| entry.state == LineageEntryState::Active)
            .unwrap();
        bundle.ledger.aliases = vec![LineageAlias {
            identity: StableIdentity::new(entry.identity.namespace.clone(), "HistoricalName")
                .unwrap(),
            id: entry.id,
        }];
        bundle.ledger.version = LINEAGE_LEDGER_VERSION_V2;
        let budget = ProviderBudget::new();
        let bound = ledger_validation_admission::LedgerValidationBound::inspect(&bundle).unwrap();
        let mut work = budget
            .reserve(
                bound.maximum().unwrap() + riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1,
            )
            .unwrap();
        let _scratch = work
            .split(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
            .unwrap();
        let required = required_lineage_allocation_namespaces(
            bundle.schema(),
            bundle.commands(),
            bundle.projections(),
        )
        .unwrap();
        let mut expected = Vec::new();
        visit_expected_lineage_identities(
            &bundle.ledger,
            bundle.schema(),
            bundle.commands(),
            bundle.projections(),
            |tag, kind, owners, name, id| {
                expected.push((
                    StableIdentity::new(StableIdNamespace::new(tag, kind, owners.to_vec())?, name)?,
                    id,
                ));
                Ok::<_, IrValidationError>(())
            },
        )
        .unwrap();
        let identities = bundle
            .ledger
            .allocations
            .iter()
            .flat_map(|value| &value.entries)
            .map(|value| value.identity.clone())
            .chain(
                bundle
                    .ledger
                    .aliases
                    .iter()
                    .map(|value| value.identity.clone()),
            )
            .collect::<BTreeSet<_>>();
        assert!(
            riffdb_types::OwnedCapacityInspectionV1::inspect(&required)
                .unwrap()
                .unshared_bytes()
                <= bound.required
        );
        assert!(
            riffdb_types::OwnedCapacityInspectionV1::inspect(&expected)
                .unwrap()
                .unshared_bytes()
                <= bound.expected
        );
        assert!(
            riffdb_types::OwnedCapacityInspectionV1::inspect(&identities)
                .unwrap()
                .unshared_bytes()
                <= bound.identities
        );
        for identity in &identities {
            let temporary = identity.namespace.allocation_namespace();
            assert!(
                riffdb_types::OwnedCapacityInspectionV1::inspect(&temporary)
                    .unwrap()
                    .unshared_bytes()
                    <= bound.temporary
            );
        }
        drop(identities);
        drop(expected);
        drop(required);
        drop(_scratch);
        drop(work);
        bundle
            .validate_provider_ledger_admitted_v1(&budget)
            .unwrap();
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }
}

#[test]
fn admitted_ledger_validation_preserves_required_history_active_identity_and_alias_errors() {
    let original =
        ContractBundle::decode(include_bytes!("../../../../fixtures/compiler/bundle.bin")).unwrap();
    let mut missing = original.clone();
    missing.ledger.allocations.remove(0);
    let mut ordering = original.clone();
    ordering.ledger.allocations.swap(0, 1);
    let mut active = original.clone();
    active
        .ledger
        .allocations
        .iter_mut()
        .flat_map(|value| &mut value.entries)
        .find(|entry| entry.state == LineageEntryState::Active)
        .unwrap()
        .identity
        .name = "ReplacementName".to_owned();
    let mut history = original.clone();
    history
        .ledger
        .allocations
        .iter_mut()
        .find(|value| !value.entries.is_empty())
        .unwrap()
        .max_allocated += 1;
    let mut alias = original.clone();
    let target = alias
        .ledger
        .allocations
        .iter()
        .flat_map(|value| &value.entries)
        .next()
        .unwrap();
    alias.ledger.version = LINEAGE_LEDGER_VERSION_V2;
    alias.ledger.aliases = vec![LineageAlias {
        identity: StableIdentity::new(target.identity.namespace.clone(), "HistoricalName").unwrap(),
        id: u32::MAX,
    }];
    let mut collision = alias.clone();
    collision.ledger.aliases[0].identity = target.identity.clone();
    let budget = ProviderBudget::new();
    for bundle in [missing, ordering, active, history, alias, collision] {
        let expected = validate_ledger(
            &bundle.ledger,
            bundle.schema(),
            bundle.commands(),
            bundle.projections(),
        )
        .unwrap_err();
        assert_eq!(
            bundle.validate_provider_ledger_admitted_v1(&budget),
            Err(ProviderBundleEncodingErrorV1::Invalid(expected))
        );
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
    let bound = ledger_validation_admission::LedgerValidationBound {
        required: u64::MAX,
        expected: 1,
        identities: 0,
        temporary: 0,
    };
    assert_eq!(
        bound.maximum(),
        Err(riffdb_types::exact_provider_budget_v1::BudgetUnavailable)
    );
}

#[test]
fn admitted_ledger_validation_refuses_before_construction_under_pressure() {
    let bundle =
        ContractBundle::decode(include_bytes!("../../../../fixtures/compiler/bundle.bin")).unwrap();
    for remaining in [128, 32 * 1024] {
        let budget = ProviderBudget::new();
        let fixed = budget.observation().unwrap().fixed_owner_bytes;
        let pressure = budget
            .reserve(EXACT_PROVIDER_BUDGET_BYTES - fixed - remaining)
            .unwrap();
        let before = budget.observation().unwrap();
        assert_eq!(
            bundle.validate_provider_ledger_admitted_v1(&budget),
            Err(ProviderBundleEncodingErrorV1::Unavailable)
        );
        assert_eq!(budget.observation().unwrap(), before);
        drop(pressure);
    }
}
