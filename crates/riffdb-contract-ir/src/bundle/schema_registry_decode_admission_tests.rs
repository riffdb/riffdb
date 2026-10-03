//! Schema registry decoding admits retained output and generation together.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::exact_provider_budget_v1::{EXACT_PROVIDER_BUDGET_BYTES, ProviderBudget};

fn section(bundle: &ContractBundle) -> Vec<u8> {
    let mut writer = Writer::new(MAX_BUNDLE_BYTES);
    writer.u32(bundle.schema_artifacts.len() as u32).unwrap();
    for artifact in &bundle.schema_artifacts {
        writer.raw(&artifact.key().to_bytes()).unwrap();
        writer.bytes(artifact.canonical_json().as_bytes()).unwrap();
        writer.raw(artifact.hash().as_bytes()).unwrap();
    }
    writer.finish()
}

#[test]
fn admitted_schema_registry_decode_preserves_frozen_artifacts_and_final_capture_charge() {
    for bytes in [
        include_bytes!("../../../../fixtures/compiler/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/reimport/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/workflow-initial/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/production-embedding/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/cascade/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/vector-ann/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/secret/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/text-index/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/initialized-state-transition/bundle.bin")
            .as_slice(),
        include_bytes!("../../../../fixtures/compiler/secret-reveal/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/indexed-selection/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/command-decision/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/aggregate-collection-budget/bundle.bin")
            .as_slice(),
    ] {
        let bundle = ContractBundle::decode(bytes).unwrap();
        let bytes = section(&bundle);
        let budget = ProviderBudget::new();
        let output = decode_provider_schema_registry_section_admitted_v1(
            &bytes,
            bundle.schema(),
            bundle.commands(),
            bundle.projections(),
            &budget,
        )
        .unwrap();
        assert_eq!(**output, bundle.schema_artifacts);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        let capture = output.clone();
        drop(output);
        assert!(budget.observation().unwrap().owned_bytes > 0);
        drop(capture);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
}

#[test]
fn admitted_schema_registry_decode_native_capacity_covers_retained_vector_and_peak_generation() {
    let bundle =
        ContractBundle::decode(include_bytes!("../../../../fixtures/compiler/bundle.bin")).unwrap();
    let bytes = section(&bundle);
    let budget = ProviderBudget::new();
    let sizing = budget.reserve(32 * 1024).unwrap();
    let bound = schema_registry_decode_admission::SchemaRegistrySectionBound::inspect(
        &bytes,
        bundle.schema(),
        bundle.commands(),
        bundle.projections(),
    )
    .unwrap();
    let mut work = budget.reserve(bound.maximum().unwrap()).unwrap();
    drop(sizing);
    let _scratch = work
        .split(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let mut reader = Reader::new(&bytes);
    let artifacts = decode_schema_artifacts(
        &mut reader,
        bundle.schema(),
        bundle.commands(),
        bundle.projections(),
    )
    .unwrap();
    reader.finish().unwrap();
    assert_eq!(artifacts.capacity(), artifacts.len());
    let inventory = riffdb_types::OwnedCapacityInspectionV1::inspect(&artifacts).unwrap();
    assert!(inventory.shared_allocations().is_empty());
    assert!(inventory.unshared_bytes() <= bound.retained);
    let mut largest = 0;
    visit_schema_artifact_sources(
        bundle.schema(),
        bundle.commands(),
        bundle.projections(),
        |source| {
            let expected = source.expected_size(bundle.schema())?;
            largest = largest.max(crate::json_schema::json_workspace_bound(expected).unwrap());
            Ok::<_, IrValidationError>(())
        },
    )
    .unwrap();
    // The native JSON-tree/render witnesses live with the common bound in
    // json_schema/admission_tests. This checks registry composition retains
    // the complete earlier output in addition to that peak generation work.
    assert_eq!(bound.generation, largest);
    assert!(
        bound.maximum().unwrap()
            >= inventory.unshared_bytes()
                + largest
                + riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1
    );
    let overflowing = schema_registry_decode_admission::SchemaRegistrySectionBound {
        retained: u64::MAX,
        generation: 1,
    };
    assert_eq!(
        overflowing.maximum(),
        Err(riffdb_types::exact_provider_budget_v1::BudgetUnavailable)
    );
}

#[test]
fn admitted_schema_registry_decode_preserves_mismatch_errors_and_releases_partial_output() {
    let bundle =
        ContractBundle::decode(include_bytes!("../../../../fixtures/compiler/bundle.bin")).unwrap();
    let bytes = section(&bundle);
    let budget = ProviderBudget::new();
    for end in 0..bytes.len() {
        assert!(
            decode_provider_schema_registry_section_admitted_v1(
                &bytes[..end],
                bundle.schema(),
                bundle.commands(),
                bundle.projections(),
                &budget,
            )
            .is_err()
        );
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
    let mut key = bytes.clone();
    key[4] ^= 1;
    let mut json = bytes.clone();
    json[13] = 255;
    let mut late_hash = bytes.clone();
    *late_hash.last_mut().unwrap() ^= 1;
    let mut count = bytes.clone();
    count[..4].copy_from_slice(&(bundle.schema_artifacts.len() as u32 - 1).to_le_bytes());
    let mut hostile_count = bytes.clone();
    hostile_count[..4].copy_from_slice(&20_481u32.to_le_bytes());
    let mut reordered = bundle.clone();
    reordered.schema_artifacts.swap(0, 1);
    for bytes in [
        key,
        json,
        late_hash,
        count,
        hostile_count,
        section(&reordered),
    ] {
        let expected = decode_schema_artifacts(
            &mut Reader::new(&bytes),
            bundle.schema(),
            bundle.commands(),
            bundle.projections(),
        )
        .unwrap_err();
        assert!(
            matches!(decode_provider_schema_registry_section_admitted_v1(
            &bytes, bundle.schema(), bundle.commands(), bundle.projections(), &budget,
        ), Err(ProviderBundleEncodingErrorV1::Invalid(error)) if error == expected)
        );
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
    let mut trailing = bytes;
    trailing.push(0);
    assert!(matches!(
        decode_provider_schema_registry_section_admitted_v1(
            &trailing,
            bundle.schema(),
            bundle.commands(),
            bundle.projections(),
            &budget,
        ),
        Err(ProviderBundleEncodingErrorV1::Invalid(
            IrValidationError::TrailingBytes
        ))
    ));
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

#[test]
fn admitted_schema_registry_decode_refuses_before_generation_under_pressure() {
    let bundle =
        ContractBundle::decode(include_bytes!("../../../../fixtures/compiler/bundle.bin")).unwrap();
    let bytes = section(&bundle);
    for remaining in [128, 64 * 1024] {
        let budget = ProviderBudget::new();
        let fixed = budget.observation().unwrap().fixed_owner_bytes;
        let pressure = budget
            .reserve(EXACT_PROVIDER_BUDGET_BYTES - fixed - remaining)
            .unwrap();
        let before = budget.observation().unwrap();
        assert!(matches!(
            decode_provider_schema_registry_section_admitted_v1(
                &bytes,
                bundle.schema(),
                bundle.commands(),
                bundle.projections(),
                &budget,
            ),
            Err(ProviderBundleEncodingErrorV1::Unavailable)
        ));
        assert_eq!(budget.observation().unwrap(), before);
        drop(pressure);
    }
}
