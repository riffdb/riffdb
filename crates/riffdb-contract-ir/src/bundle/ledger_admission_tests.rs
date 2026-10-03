//! Existing lineage section parsing must be admitted before native allocation.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::exact_provider_budget_v1::{EXACT_PROVIDER_BUDGET_BYTES, ProviderBudget};

fn section(ledger: &LineageLedgerV1) -> Vec<u8> {
    let mut writer = Writer::new(MAX_BUNDLE_BYTES);
    encode_ledger(&mut writer, ledger).unwrap();
    writer.finish()
}

#[test]
fn admitted_ledger_parser_preserves_frozen_sections_and_final_capture_charge() {
    for bytes in [
        include_bytes!("../../../../fixtures/compiler/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/reimport/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/workflow-initial/bundle.bin").as_slice(),
    ] {
        let bundle = ContractBundle::decode(bytes).unwrap();
        let bytes = section(&bundle.ledger);
        let budget = ProviderBudget::new();
        let output = decode_provider_ledger_section_admitted_v1(&bytes, &budget).unwrap();
        assert_eq!(**output, bundle.ledger);
        validate_ledger(
            &output,
            bundle.schema(),
            bundle.commands(),
            bundle.projections(),
        )
        .unwrap();
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        let capture = output.clone();
        drop(output);
        assert!(budget.observation().unwrap().owned_bytes > 0);
        drop(capture);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
}

#[test]
fn admitted_ledger_parser_refuses_before_construction_under_pressure() {
    let bundle =
        ContractBundle::decode(include_bytes!("../../../../fixtures/compiler/bundle.bin")).unwrap();
    let bytes = section(&bundle.ledger);
    for remaining in [128, 64 * 1024] {
        let budget = ProviderBudget::new();
        let fixed = budget.observation().unwrap().fixed_owner_bytes;
        let pressure = budget
            .reserve(EXACT_PROVIDER_BUDGET_BYTES - fixed - remaining)
            .unwrap();
        let before = budget.observation().unwrap();
        assert!(matches!(
            decode_provider_ledger_section_admitted_v1(&bytes, &budget),
            Err(ProviderBundleEncodingErrorV1::Unavailable)
        ));
        assert_eq!(budget.observation().unwrap(), before);
        drop(pressure);
    }
}

fn scoped_ledger(version: u32, count: usize) -> LineageLedgerV1 {
    let namespace = StableIdAllocationNamespace::scoped(
        StableIdNamespaceTag::Field,
        record_owner_tag::COMMAND_OUTCOME,
        vec![1, 2],
    )
    .unwrap();
    let entries = (1..=count)
        .map(|id| LineageEntry {
            id: id as u32,
            identity: StableIdentity::new(
                StableIdNamespace::new(
                    StableIdNamespaceTag::Field,
                    record_owner_tag::COMMAND_OUTCOME,
                    vec![1, 2],
                )
                .unwrap(),
                format!("Member{id}"),
            )
            .unwrap(),
            state: if id % 2 == 0 {
                LineageEntryState::Tombstone
            } else {
                LineageEntryState::Active
            },
        })
        .collect();
    let mut aliases = if version == LINEAGE_LEDGER_VERSION_V2 {
        (1..=count)
            .map(|id| LineageAlias {
                identity: StableIdentity::new(
                    StableIdNamespace::new(
                        StableIdNamespaceTag::Field,
                        record_owner_tag::COMMAND_OUTCOME,
                        vec![1, 2],
                    )
                    .unwrap(),
                    format!("Historical{id}"),
                )
                .unwrap(),
                id: id as u32,
            })
            .collect::<Vec<_>>()
    } else {
        vec![]
    };
    aliases.sort_unstable_by(|left, right| left.identity.cmp(&right.identity));
    LineageLedgerV1 {
        version,
        allocations: vec![LineageAllocation {
            namespace,
            max_allocated: count as u32,
            entries,
        }],
        aliases,
    }
}

#[test]
fn admitted_ledger_native_capacity_witnesses_cover_versions_scopes_tombstones_and_aliases() {
    for (version, count) in [
        (LINEAGE_LEDGER_VERSION_V1, 0),
        (LINEAGE_LEDGER_VERSION_V1, 1),
        (LINEAGE_LEDGER_VERSION_V1, 5),
        (LINEAGE_LEDGER_VERSION_V1, MAX_LINEAGE_LEDGER_ENTRIES),
        (LINEAGE_LEDGER_VERSION_V2, 4_096),
    ] {
        let ledger = scoped_ledger(version, count);
        let bytes = section(&ledger);
        let bound = ledger_admission::LedgerSectionBound::inspect(&bytes).unwrap();
        let budget = ProviderBudget::new();
        {
            let mut work = budget.reserve(bound.maximum().unwrap()).unwrap();
            let _scratch = work
                .split(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
                .unwrap();
            let decoded = decode_ledger(&mut Reader::new(&bytes)).unwrap();
            let inventory = riffdb_types::OwnedCapacityInspectionV1::inspect(&decoded).unwrap();
            assert!(inventory.shared_allocations().is_empty());
            assert!(inventory.unshared_bytes() <= bound.heap);
            if let Some(entry) = decoded.allocations()[0].entries().first() {
                let temporary = entry.identity().namespace().allocation_namespace();
                let inspection =
                    riffdb_types::OwnedCapacityInspectionV1::inspect(&temporary).unwrap();
                assert!(inspection.unshared_bytes() <= bound.constructors);
            }
            let mut writer = Writer::with_capacity(bytes.len(), bytes.len());
            encode_ledger(&mut writer, &decoded).unwrap();
            let canonical = writer.finish();
            let inspection = riffdb_types::OwnedCapacityInspectionV1::inspect(&canonical).unwrap();
            assert!(inspection.unshared_bytes() <= bound.canonical);
            assert_eq!(canonical, bytes);
        }
        let output = decode_provider_ledger_section_admitted_v1(&bytes, &budget).unwrap();
        assert_eq!(**output, ledger);
        drop(output);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
}

fn raw_ledger_entry(
    allocation_owners: &[u32],
    identity_owners: &[u32],
    id: u32,
    max_allocated: u32,
    state: u8,
    name: &str,
) -> Vec<u8> {
    let mut writer = Writer::new(MAX_BUNDLE_BYTES);
    writer.u32(LINEAGE_LEDGER_VERSION_V1).unwrap();
    writer.u32(1).unwrap();
    writer.u8(StableIdNamespaceTag::Field as u8).unwrap();
    writer.u8(record_owner_tag::COMMAND_OUTCOME).unwrap();
    writer.u8(allocation_owners.len() as u8).unwrap();
    for id in allocation_owners {
        writer.u32(*id).unwrap();
    }
    writer.u32(max_allocated).unwrap();
    writer.u32(1).unwrap();
    writer.u32(id).unwrap();
    writer.u8(record_owner_tag::COMMAND_OUTCOME).unwrap();
    writer.u8(identity_owners.len() as u8).unwrap();
    for id in identity_owners {
        writer.u32(*id).unwrap();
    }
    writer.string(name).unwrap();
    writer.u8(state).unwrap();
    writer.finish()
}

#[test]
fn admitted_ledger_parser_preserves_structural_errors_and_releases_every_rejection() {
    let bytes = section(&scoped_ledger(LINEAGE_LEDGER_VERSION_V2, 2));
    let budget = ProviderBudget::new();
    for end in 0..bytes.len() {
        assert!(decode_provider_ledger_section_admitted_v1(&bytes[..end], &budget).is_err());
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
    let state = lineage_state_tag::ACTIVE;
    let valid = raw_ledger_entry(&[1, 2], &[1, 2], 1, 1, state, "Member");
    let mut unsupported = valid.clone();
    unsupported[..4].copy_from_slice(&u32::MAX.to_le_bytes());
    let mut hostile = valid.clone();
    hostile[4..8].copy_from_slice(&(MAX_LINEAGE_ALLOCATION_STATES as u32 + 1).to_le_bytes());
    let mut hostile_entries = valid.clone();
    hostile_entries[23..27].copy_from_slice(&(MAX_LINEAGE_LEDGER_ENTRIES as u32 + 1).to_le_bytes());
    let mut unknown_namespace = valid.clone();
    unknown_namespace[8] = 255;
    for bytes in [
        unsupported,
        hostile,
        hostile_entries,
        unknown_namespace,
        raw_ledger_entry(&vec![1; 255], &[1, 2], 1, 1, state, "Member"),
        raw_ledger_entry(&[1, 2], &vec![1; 255], 1, 1, state, "Member"),
        raw_ledger_entry(&[1, 2], &[1, 3], 1, 1, state, "Member"),
        raw_ledger_entry(&[1, 2], &[1, 2], 2, 1, state, "Member"),
        raw_ledger_entry(&[1, 2], &[1, 2], 1, 2, state, "Member"),
        raw_ledger_entry(&[1, 2], &[1, 2], 1, 1, 255, "Member"),
        raw_ledger_entry(&[1, 2], &[1, 2], 1, 1, state, "tx"),
    ] {
        let expected = decode_ledger(&mut Reader::new(&bytes)).unwrap_err();
        assert!(
            matches!(decode_provider_ledger_section_admitted_v1(&bytes, &budget),
            Err(ProviderBundleEncodingErrorV1::Invalid(error)) if error == expected)
        );
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
    let mut trailing = valid;
    trailing.push(0);
    assert!(matches!(
        decode_provider_ledger_section_admitted_v1(&trailing, &budget),
        Err(ProviderBundleEncodingErrorV1::Invalid(
            IrValidationError::TrailingBytes
        ))
    ));
    let bound = ledger_admission::LedgerSectionBound {
        heap: u64::MAX,
        constructors: 1,
        canonical: 0,
    };
    assert_eq!(
        bound.maximum(),
        Err(riffdb_types::exact_provider_budget_v1::BudgetUnavailable)
    );
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

#[test]
fn admitted_ledger_parser_preserves_aggregate_history_alias_and_state_order_limits() {
    let mut oversized = scoped_ledger(LINEAGE_LEDGER_VERSION_V1, MAX_LINEAGE_LEDGER_ENTRIES);
    let mut extra = scoped_ledger(LINEAGE_LEDGER_VERSION_V1, 1)
        .allocations
        .remove(0);
    extra.namespace.owner_ids[1] = 3;
    extra.entries[0].identity.namespace.owner_ids[1] = 3;
    oversized.allocations.push(extra);
    let over_total = section(&oversized);
    let mut duplicate_states = scoped_ledger(LINEAGE_LEDGER_VERSION_V1, 0);
    duplicate_states
        .allocations
        .push(duplicate_states.allocations[0].clone());
    let duplicate_states = section(&duplicate_states);
    let mut writer = Writer::new(MAX_BUNDLE_BYTES);
    writer.u32(LINEAGE_LEDGER_VERSION_V2).unwrap();
    writer.u32(0).unwrap();
    writer.u32(MAX_LINEAGE_LEDGER_ENTRIES as u32 + 1).unwrap();
    let over_aliases = writer.finish();
    let budget = ProviderBudget::new();
    for bytes in [over_total, duplicate_states, over_aliases] {
        let expected = decode_ledger(&mut Reader::new(&bytes)).unwrap_err();
        assert!(
            matches!(decode_provider_ledger_section_admitted_v1(&bytes, &budget),
            Err(ProviderBundleEncodingErrorV1::Invalid(error)) if error == expected)
        );
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
}
