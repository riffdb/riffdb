//! Registry construction and escaped lineage ownership require admission.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::exact_provider_budget_v1::{EXACT_PROVIDER_BUDGET_BYTES, ProviderBudget};

fn section(registry: &McpCommandNameRegistryV2) -> Vec<u8> {
    let mut writer = Writer::new(MAX_BUNDLE_BYTES);
    encode_mcp_registry(&mut writer, registry).unwrap();
    writer.finish()
}

#[test]
fn borrowed_mcp_registry_binding_validation_preserves_completeness_and_reimport_exclusion() {
    let bundle = ContractBundle::decode(include_bytes!(
        "../../../../fixtures/compiler/reimport/bundle.bin"
    ))
    .unwrap();
    assert!(bundle.commands().iter().any(CommandPlan::is_reimport));
    let registry = &bundle.mcp_command_names;
    validate_mcp_registry(bundle.lineage(), bundle.commands(), registry).unwrap();
    let bundle =
        ContractBundle::decode(include_bytes!("../../../../fixtures/compiler/bundle.bin")).unwrap();
    let registry = &bundle.mcp_command_names;
    assert!(!registry.entries().is_empty());
    let incomplete = McpCommandNameRegistryV2::new(
        bundle.lineage().clone(),
        registry.source_contract_name(),
        vec![],
    )
    .unwrap();
    assert_eq!(
        validate_mcp_registry(bundle.lineage(), bundle.commands(), &incomplete),
        Err(IrValidationError::InvalidMcpName {
            reason: "MCP registry lineage or completeness mismatch"
        })
    );
    assert_eq!(
        validate_mcp_registry(
            &ContractLineage::new("Other").unwrap(),
            bundle.commands(),
            registry
        ),
        Err(IrValidationError::InvalidMcpName {
            reason: "MCP registry lineage or completeness mismatch"
        })
    );
    let rebound = McpCommandNameRegistryV2::new(
        bundle.lineage().clone(),
        registry.source_contract_name(),
        registry
            .entries()
            .iter()
            .map(|entry| {
                McpCommandNameEntryV2::new(
                    CommandId::new(entry.command_id().get() + 100_000).unwrap(),
                    registry.source_contract_name(),
                    entry.source_command_name(),
                    entry.tool_name().as_str(),
                )
                .unwrap()
            })
            .collect(),
    )
    .unwrap();
    assert_eq!(
        validate_mcp_registry(bundle.lineage(), bundle.commands(), &rebound),
        Err(IrValidationError::InvalidMcpName {
            reason: "MCP registry command binding mismatch"
        })
    );
    let bytes = section(&incomplete);
    let budget = ProviderBudget::new();
    let admitted = decode_provider_mcp_registry_section_admitted_v1(&bytes, &budget).unwrap();
    assert_eq!(**admitted, incomplete);
    drop(admitted);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

#[test]
fn admitted_mcp_registry_parser_preserves_frozen_names_and_escaped_lineage_charge() {
    for bytes in [
        include_bytes!("../../../../fixtures/compiler/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/reimport/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/workflow-initial/bundle.bin").as_slice(),
    ] {
        let bundle = ContractBundle::decode(bytes).unwrap();
        let bytes = section(&bundle.mcp_command_names);
        let budget = ProviderBudget::new();
        let output = decode_provider_mcp_registry_section_admitted_v1(&bytes, &budget).unwrap();
        assert_eq!(**output, bundle.mcp_command_names);
        validate_mcp_registry(bundle.lineage(), bundle.commands(), &output).unwrap();
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        let escaped = output.lineage().clone();
        drop(output);
        assert!(budget.observation().unwrap().owned_bytes > 0);
        drop(escaped);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
}

#[test]
fn admitted_mcp_registry_parser_refuses_before_construction_under_pressure() {
    let bundle =
        ContractBundle::decode(include_bytes!("../../../../fixtures/compiler/bundle.bin")).unwrap();
    let bytes = section(&bundle.mcp_command_names);
    for remaining in [128, 64 * 1024] {
        let budget = ProviderBudget::new();
        let fixed = budget.observation().unwrap().fixed_owner_bytes;
        let pressure = budget
            .reserve(EXACT_PROVIDER_BUDGET_BYTES - fixed - remaining)
            .unwrap();
        let before = budget.observation().unwrap();
        assert!(matches!(
            decode_provider_mcp_registry_section_admitted_v1(&bytes, &budget),
            Err(ProviderBundleEncodingErrorV1::Unavailable)
        ));
        assert_eq!(budget.observation().unwrap(), before);
        drop(pressure);
    }
}

#[test]
fn admitted_mcp_registry_native_capacity_witnesses_cover_maximum_entries_and_names() {
    let registry = McpCommandNameRegistryV2::new(
        ContractLineage::new("Registry").unwrap(),
        "Registry",
        (1..=4_096)
            .map(|id| {
                McpCommandNameEntryV2::new(
                    CommandId::new(id).unwrap(),
                    "Registry",
                    format!("Command{id}"),
                    format!("riffdb_cmd_registry_command{id}"),
                )
                .unwrap()
            })
            .collect(),
    )
    .unwrap();
    let bytes = section(&registry);
    let bound = mcp_admission::McpSectionBound::inspect(&bytes).unwrap();
    let budget = ProviderBudget::new();
    let mut work = budget.reserve(bound.maximum().unwrap()).unwrap();
    let _scratch = work
        .split(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let decoded = decode_mcp_registry(&mut Reader::new(&bytes)).unwrap();
    let inventory = riffdb_types::OwnedCapacityInspectionV1::inspect(&decoded).unwrap();
    assert!(inventory.unshared_bytes() <= bound.heap);
    assert_eq!(inventory.shared_allocations().len(), 1);
    assert!(inventory.shared_allocations()[0].bytes() <= bound.shared);
    // Two machine words have the native key size/alignment of a borrowed str.
    // This independent tree census proves the collision-set layout allowance
    // without treating the borrowed names as owned allocations.
    assert_eq!(
        std::mem::size_of::<[usize; 2]>(),
        std::mem::size_of::<&str>()
    );
    let keys = (0..registry.entries().len())
        .map(|index| [index, 0])
        .collect::<BTreeSet<_>>();
    let collision = riffdb_types::OwnedCapacityInspectionV1::inspect(&keys).unwrap();
    assert!(
        collision.unshared_bytes() <= plan_hash_admission::tree_bound::<&str>(keys.len()).unwrap()
    );
    for (contract, command, tool) in [(8, 10, 30), (1, 1, 13), (256, 256, 128)] {
        let contract = "C".repeat(contract).to_ascii_lowercase();
        let command = "D".repeat(command).to_ascii_lowercase();
        let normalized = format!("riffdb_cmd_{contract}_{command}");
        let cloned_tool = "t".repeat(tool);
        let mut observed = 0u64;
        for value in [&contract, &command, &normalized, &cloned_tool] {
            observed += riffdb_types::OwnedCapacityInspectionV1::inspect(value)
                .unwrap()
                .unshared_bytes();
        }
        assert!(
            observed <= mcp_admission::name_workspace(contract.len(), command.len(), tool).unwrap()
        );
    }
    let mut writer = Writer::with_capacity(bytes.len(), bytes.len());
    encode_mcp_registry(&mut writer, &decoded).unwrap();
    let canonical = writer.finish();
    assert!(
        riffdb_types::OwnedCapacityInspectionV1::inspect(&canonical)
            .unwrap()
            .unshared_bytes()
            <= bound.canonical
    );
    assert_eq!(canonical, bytes);
    drop(canonical);
    drop(keys);
    drop(inventory);
    drop(decoded);
    drop(_scratch);
    drop(work);
    let output = decode_provider_mcp_registry_section_admitted_v1(&bytes, &budget).unwrap();
    assert_eq!(**output, registry);
    drop(output);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

fn raw_registry(contract: &str, entries: &[(u32, &str, &str)]) -> Vec<u8> {
    let mut writer = Writer::new(MAX_BUNDLE_BYTES);
    writer
        .u32(crate::MCP_COMMAND_NAME_REGISTRY_VERSION_V2)
        .unwrap();
    writer.string(contract).unwrap();
    writer.string(contract).unwrap();
    writer.u32(entries.len() as u32).unwrap();
    for (id, command, tool) in entries {
        writer.u32(*id).unwrap();
        writer.string(command).unwrap();
        writer.string(tool).unwrap();
    }
    writer.finish()
}

#[test]
fn admitted_mcp_registry_rejects_malformed_collision_and_noncanonical_sections() {
    let bytes = raw_registry(
        "Test",
        &[
            (1, "Foo", "riffdb_cmd_test_foo"),
            (2, "Bar", "riffdb_cmd_test_bar"),
        ],
    );
    let budget = ProviderBudget::new();
    for end in 0..bytes.len() {
        assert!(decode_provider_mcp_registry_section_admitted_v1(&bytes[..end], &budget).is_err());
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
    let mut wrong_version = bytes.clone();
    wrong_version[..4].copy_from_slice(&u32::MAX.to_le_bytes());
    let mut hostile = bytes.clone();
    hostile[20..24].copy_from_slice(&4_097u32.to_le_bytes());
    let long_contract = "X".repeat(256);
    let long_command = "Y".repeat(256);
    for bytes in [
        wrong_version,
        hostile,
        raw_registry("Test", &[(0, "Foo", "riffdb_cmd_test_foo")]),
        raw_registry("Test", &[(1, "Foo", "riffdb_cmd_test_wrong")]),
        raw_registry(
            "Test",
            &[
                (1, "Foo", "riffdb_cmd_test_foo"),
                (1, "Bar", "riffdb_cmd_test_bar"),
            ],
        ),
        raw_registry(
            "Test",
            &[
                (1, "Foo", "riffdb_cmd_test_foo"),
                (2, "foo", "riffdb_cmd_test_foo"),
            ],
        ),
        raw_registry(&long_contract, &[(1, &long_command, "short")]),
    ] {
        let expected = decode_mcp_registry(&mut Reader::new(&bytes)).unwrap_err();
        assert!(matches!(
            decode_provider_mcp_registry_section_admitted_v1(&bytes, &budget),
            Err(ProviderBundleEncodingErrorV1::Invalid(error)) if error == expected
        ));
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
    let reversed = raw_registry(
        "Test",
        &[
            (2, "Bar", "riffdb_cmd_test_bar"),
            (1, "Foo", "riffdb_cmd_test_foo"),
        ],
    );
    assert_eq!(
        decode_mcp_registry(&mut Reader::new(&reversed)).unwrap(),
        decode_mcp_registry(&mut Reader::new(&bytes)).unwrap()
    );
    assert!(matches!(
        decode_provider_mcp_registry_section_admitted_v1(&reversed, &budget),
        Err(ProviderBundleEncodingErrorV1::Invalid(
            IrValidationError::NonCanonicalOrder {
                kind: "canonical contract bundle bytes"
            }
        ))
    ));
    let mut trailing = bytes;
    trailing.push(0);
    assert!(matches!(
        decode_provider_mcp_registry_section_admitted_v1(&trailing, &budget),
        Err(ProviderBundleEncodingErrorV1::Invalid(
            IrValidationError::TrailingBytes
        ))
    ));
    let bound = mcp_admission::McpSectionBound {
        heap: u64::MAX,
        shared: 1,
        constructors: 0,
        canonical: 0,
    };
    assert_eq!(
        bound.maximum(),
        Err(riffdb_types::exact_provider_budget_v1::BudgetUnavailable)
    );
    assert!(mcp_admission::name_workspace(usize::MAX, 1, 1).is_err());
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}
