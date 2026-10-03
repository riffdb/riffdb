//! Existing workflow bytes require admission before parser and constructor work.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::exact_provider_budget_v1::{EXACT_PROVIDER_BUDGET_BYTES, ProviderBudget};

fn section(bundle: &ContractBundle) -> Vec<u8> {
    let mut writer = Writer::new(MAX_BUNDLE_BYTES);
    encode_workflows(&mut writer, &bundle.workflows, bundle.ir_version()).unwrap();
    writer.finish()
}

#[test]
fn admitted_workflow_parser_preserves_frozen_sections_and_final_capture_charge() {
    for bytes in [
        include_bytes!("../../../../fixtures/compiler/initialized-state-transition/bundle.bin")
            .as_slice(),
        include_bytes!("../../../../fixtures/compiler/workflow-initial/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/reimport/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/secret-reveal/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/aggregate-collection-budget/bundle.bin")
            .as_slice(),
    ] {
        let bundle = ContractBundle::decode(bytes).unwrap();
        assert!(bundle.ir_version() >= EXECUTABLE_IR_VERSION_V2);
        let bytes = section(&bundle);
        {
            let budget = ProviderBudget::new();
            let sizing = budget.reserve(32 * 1024).unwrap();
            let bound = workflow_admission::WorkflowSectionBound::inspect(
                &bytes,
                bundle.schema(),
                bundle.ir_version(),
            )
            .unwrap();
            let mut work = budget.reserve(bound.maximum().unwrap()).unwrap();
            drop(sizing);
            let _scratch = work
                .split(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
                .unwrap();
            let mut reader = Reader::new(&bytes);
            let data = decode_workflows(&mut reader, bundle.schema(), bundle.ir_version()).unwrap();
            reader.finish().unwrap();
            let inventory = riffdb_types::OwnedCapacityInspectionV1::inspect(&data).unwrap();
            assert!(inventory.shared_allocations().is_empty());
            assert!(inventory.unshared_bytes() <= bound.heap);
            for workflow in data.workflows() {
                let edges = workflow
                    .transitions()
                    .iter()
                    .flat_map(|transition| {
                        transition
                            .source_states()
                            .iter()
                            .map(|source| (*source, transition.destination()))
                    })
                    .collect::<BTreeSet<_>>();
                let transient = riffdb_types::OwnedCapacityInspectionV1::inspect(&edges).unwrap();
                assert!(transient.unshared_bytes() <= bound.constructors);
            }
            {
                let entities = data
                    .workflows()
                    .iter()
                    .map(WorkflowSchema::entity)
                    .collect::<BTreeSet<_>>();
                for workflow in data.workflows() {
                    let variants = bundle
                        .schema()
                        .enumeration(workflow.state_enum())
                        .unwrap()
                        .variants()
                        .iter()
                        .map(crate::EnumVariantSchema::id)
                        .collect::<BTreeSet<_>>();
                    let transient =
                        riffdb_types::OwnedCapacityInspectionV1::inspect_with(|inspection| {
                            inspection.visit(&entities)?;
                            inspection.visit(&variants)
                        })
                        .unwrap();
                    assert!(transient.unshared_bytes() <= bound.constructors);
                }
            }
            let mut writer = Writer::with_capacity(bytes.len(), bytes.len());
            encode_workflows(&mut writer, &data, bundle.ir_version()).unwrap();
            let encoded = writer.finish();
            assert!(2 * encoded.capacity() as u64 + 258 <= bound.canonical);
            assert_eq!(encoded, bytes);
        }
        let budget = ProviderBudget::new();
        let workflows = decode_provider_workflow_section_admitted_v1(
            &bytes,
            bundle.schema(),
            bundle.ir_version(),
            &budget,
        )
        .unwrap();
        assert_eq!(&**workflows, &bundle.workflows);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        assert!(budget.observation().unwrap().owned_bytes > 0);
        let captured = workflows.clone();
        drop(workflows);
        assert!(budget.observation().unwrap().owned_bytes > 0);
        drop(captured);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
}

#[test]
fn admitted_workflow_parser_refuses_pressure_and_releases_invalid_candidate() {
    let bundle = ContractBundle::decode(include_bytes!(
        "../../../../fixtures/compiler/workflow-initial/bundle.bin"
    ))
    .unwrap();
    let bytes = section(&bundle);
    for remaining in [128, 64 * 1024] {
        let budget = ProviderBudget::new();
        let fixed = budget.observation().unwrap().fixed_owner_bytes;
        let pressure = budget
            .reserve(EXACT_PROVIDER_BUDGET_BYTES - fixed - remaining)
            .unwrap();
        let before = budget.observation().unwrap();
        assert!(matches!(
            decode_provider_workflow_section_admitted_v1(
                &bytes,
                bundle.schema(),
                bundle.ir_version(),
                &budget
            ),
            Err(ProviderBundleEncodingErrorV1::Unavailable)
        ));
        assert_eq!(budget.observation().unwrap(), before);
        drop(pressure);
    }
    for cut in 0..bytes.len() {
        let budget = ProviderBudget::new();
        assert!(
            decode_provider_workflow_section_admitted_v1(
                &bytes[..cut],
                bundle.schema(),
                bundle.ir_version(),
                &budget
            )
            .is_err()
        );
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    let budget = ProviderBudget::new();
    assert!(matches!(
        decode_provider_workflow_section_admitted_v1(
            &trailing,
            bundle.schema(),
            bundle.ir_version(),
            &budget
        ),
        Err(ProviderBundleEncodingErrorV1::Invalid(
            IrValidationError::TrailingBytes
        ))
    ));
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}

#[test]
fn admitted_workflow_parser_preserves_semantic_errors_and_rejects_noncanonical_sections() {
    let bundle = ContractBundle::decode(include_bytes!(
        "../../../../fixtures/compiler/reimport/bundle.bin"
    ))
    .unwrap();
    let original = bundle.workflows.workflows().first().unwrap();
    let transition = original.transitions().first().unwrap();
    let returning = WorkflowTransitionSchema::new(
        "Alternate",
        vec![transition.destination()],
        transition.source_states()[0],
    )
    .unwrap();
    let workflow = WorkflowSchema::new(
        original.name(),
        original.entity(),
        original.state_field(),
        original.state_enum(),
        original.initial_state(),
        vec![transition.clone(), returning],
        original.lease().cloned(),
    )
    .unwrap();
    let catalog = WorkflowCatalog::new(vec![workflow], bundle.schema()).unwrap();
    let mut writer = Writer::new(MAX_BUNDLE_BYTES);
    encode_workflows(&mut writer, &catalog, bundle.ir_version()).unwrap();
    let bytes = writer.finish();
    let mut reader = Reader::new(&bytes);
    assert_eq!(reader.u32().unwrap(), 1);
    reader.bytes(256).unwrap();
    reader.read(12).unwrap();
    if bundle.ir_version() >= EXECUTABLE_IR_VERSION_V9 && reader.bool().unwrap() {
        reader.u32().unwrap();
    }
    assert_eq!(reader.u32().unwrap(), 2);
    let first = bytes.len() - reader.remaining();
    reader.bytes(256).unwrap();
    let states = reader.u32().unwrap() as usize;
    reader.read(states * 4 + 4).unwrap();
    let middle = bytes.len() - reader.remaining();
    reader.bytes(256).unwrap();
    let states = reader.u32().unwrap() as usize;
    reader.read(states * 4 + 4).unwrap();
    let last = bytes.len() - reader.remaining();
    let mut reversed = bytes[..first].to_vec();
    reversed.extend_from_slice(&bytes[middle..last]);
    reversed.extend_from_slice(&bytes[first..middle]);
    reversed.extend_from_slice(&bytes[last..]);
    let ordinary = decode_workflows(
        &mut Reader::new(&reversed),
        bundle.schema(),
        bundle.ir_version(),
    )
    .unwrap();
    assert_eq!(ordinary, catalog);
    let budget = ProviderBudget::new();
    assert!(matches!(
        decode_provider_workflow_section_admitted_v1(
            &reversed,
            bundle.schema(),
            bundle.ir_version(),
            &budget
        ),
        Err(ProviderBundleEncodingErrorV1::Invalid(
            IrValidationError::NonCanonicalOrder {
                kind: "canonical contract bundle bytes"
            }
        ))
    ));
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    // The lease constructor must still reject a zero minimum duration after
    // parser allocation has been admitted, and release the partial candidate.
    assert!(reader.bool().unwrap());
    reader.bytes(256).unwrap();
    reader.read(12).unwrap();
    if reader.bool().unwrap() {
        reader.u32().unwrap();
    }
    let minimum = bytes.len() - reader.remaining();
    let mut invalid = bytes.clone();
    invalid[minimum..minimum + 8].copy_from_slice(&0u64.to_be_bytes());
    let expected = decode_workflows(
        &mut Reader::new(&invalid),
        bundle.schema(),
        bundle.ir_version(),
    )
    .unwrap_err();
    assert_eq!(
        decode_provider_workflow_section_admitted_v1(
            &invalid,
            bundle.schema(),
            bundle.ir_version(),
            &budget
        )
        .err(),
        Some(ProviderBundleEncodingErrorV1::Invalid(expected))
    );
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    let hostile = (crate::MAX_DECLARATIONS_PER_KIND as u32 + 1).to_be_bytes();
    let expected = decode_workflows(
        &mut Reader::new(&hostile),
        bundle.schema(),
        bundle.ir_version(),
    )
    .unwrap_err();
    assert_eq!(
        decode_provider_workflow_section_admitted_v1(
            &hostile,
            bundle.schema(),
            bundle.ir_version(),
            &budget
        )
        .err(),
        Some(ProviderBundleEncodingErrorV1::Invalid(expected))
    );
}

#[test]
fn admitted_workflow_parser_preserves_pre_initial_state_versions_and_refuses_unknown_ir() {
    let bundle = ContractBundle::decode(include_bytes!(
        "../../../../fixtures/compiler/reimport/bundle.bin"
    ))
    .unwrap();
    let original = bundle.workflows.workflows().first().unwrap();
    for version in [EXECUTABLE_IR_VERSION_V2, EXECUTABLE_IR_VERSION_V3] {
        let lease = if version == EXECUTABLE_IR_VERSION_V3 {
            original.lease().cloned()
        } else {
            None
        };
        let workflow = WorkflowSchema::new(
            original.name(),
            original.entity(),
            original.state_field(),
            original.state_enum(),
            None,
            original.transitions().to_vec(),
            lease,
        )
        .unwrap();
        let catalog = WorkflowCatalog::new(vec![workflow], bundle.schema()).unwrap();
        let mut writer = Writer::new(MAX_BUNDLE_BYTES);
        encode_workflows(&mut writer, &catalog, version).unwrap();
        let bytes = writer.finish();
        let budget = ProviderBudget::new();
        let decoded =
            decode_provider_workflow_section_admitted_v1(&bytes, bundle.schema(), version, &budget)
                .unwrap();
        assert_eq!(&**decoded, &catalog);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        drop(decoded);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        for unknown in [0, EXECUTABLE_IR_VERSION_V1, u32::MAX] {
            assert!(
                matches!(decode_provider_workflow_section_admitted_v1(&bytes, bundle.schema(), unknown, &budget), Err(ProviderBundleEncodingErrorV1::Invalid(IrValidationError::UnsupportedVersion { kind: "workflow executable IR", value })) if value == unknown)
            );
            assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        }
    }
}
