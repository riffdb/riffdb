//! Provider parsing must admit complete projection construction before allocation.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::exact_provider_budget_v1::{EXACT_PROVIDER_BUDGET_BYTES, ProviderBudget};

fn section(bundle: &ContractBundle) -> Vec<u8> {
    let mut writer = Writer::new(MAX_BUNDLE_BYTES);
    writer.u32(bundle.projections().len() as u32).unwrap();
    for projection in bundle.projections() {
        encode_projection_bundle_entry(&mut writer, projection).unwrap();
    }
    writer.finish()
}

#[test]
fn admitted_projection_parser_preserves_frozen_plans_and_final_capture_charge() {
    let mut count = 0;
    for bytes in [
        include_bytes!("../../../../fixtures/compiler/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/production-embedding/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/reimport/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/secret/bundle.bin").as_slice(),
    ] {
        let bundle = ContractBundle::decode(bytes).unwrap();
        count += bundle.projections().len();
        let budget = ProviderBudget::new();
        let bytes = section(&bundle);
        let bound =
            projection_decode_admission::ProjectionSectionBound::inspect(&bytes, bundle.schema())
                .unwrap();
        let _inspection = budget
            .reserve(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
            .unwrap();
        let mut reader = Reader::new(&bytes);
        let ordinary = decode_projections(&mut reader, bundle.schema()).unwrap();
        reader.finish().unwrap();
        let native = riffdb_types::OwnedCapacityInspectionV1::inspect(&ordinary).unwrap();
        assert!(native.shared_allocations().is_empty());
        assert!(native.unshared_bytes() <= bound.heap);
        for projection in &ordinary {
            let source = bundle.schema().event(projection.source_event()).unwrap();
            let forest =
                collect_projection_enum_forest(projection, source, bundle.schema()).unwrap();
            let native = riffdb_types::OwnedCapacityInspectionV1::inspect_with(|inspection| {
                inspection.visit(&forest.enums)?;
                inspection.visit(&forest.records)
            })
            .unwrap();
            assert!(native.unshared_bytes() <= bound.constructors);
        }
        drop(ordinary);
        drop(_inspection);
        let output =
            decode_provider_projection_section_admitted_v1(&bytes, bundle.schema(), &budget)
                .unwrap();
        assert_eq!(output.as_slice(), bundle.projections());
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        let retained = output.clone();
        drop(output);
        assert!(budget.observation().unwrap().owned_bytes > 0);
        drop(retained);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
    assert!(count > 0);
}

#[test]
fn admitted_projection_parser_refuses_before_construction_and_releases_invalid_work() {
    let bundle =
        ContractBundle::decode(include_bytes!("../../../../fixtures/compiler/bundle.bin")).unwrap();
    let bytes = section(&bundle);
    assert!(!bundle.projections().is_empty());
    let budget = ProviderBudget::new();
    let pressure = budget
        .reserve(
            EXACT_PROVIDER_BUDGET_BYTES
                - budget.observation().unwrap().fixed_owner_bytes
                - 32 * 1024,
        )
        .unwrap();
    let before = budget.observation().unwrap();
    assert!(matches!(
        decode_provider_projection_section_admitted_v1(&bytes, bundle.schema(), &budget),
        Err(ProviderBundleEncodingErrorV1::Unavailable)
    ));
    let after = budget.observation().unwrap();
    assert_eq!(after.owned_bytes, before.owned_bytes);
    assert_eq!(after.reserved_bytes, before.reserved_bytes);
    drop(pressure);
    for end in 0..bytes.len() {
        assert!(
            decode_provider_projection_section_admitted_v1(&bytes[..end], bundle.schema(), &budget)
                .is_err()
        );
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }
    let mut noncanonical = bytes.clone();
    noncanonical.push(0);
    assert!(
        decode_provider_projection_section_admitted_v1(&noncanonical, bundle.schema(), &budget)
            .is_err()
    );
    let mut bad_hash = bytes;
    // Count, projection ID, framed name, then the frozen hash.
    let name_length = u32::from_be_bytes(bad_hash[8..12].try_into().unwrap()) as usize;
    bad_hash[12 + name_length] ^= 1;
    let ordinary = decode_projections(&mut Reader::new(&bad_hash), bundle.schema()).unwrap_err();
    assert_eq!(
        decode_provider_projection_section_admitted_v1(&bad_hash, bundle.schema(), &budget)
            .err()
            .unwrap(),
        ProviderBundleEncodingErrorV1::Invalid(ordinary)
    );
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}

#[test]
fn admitted_projection_native_bound_covers_large_measure_records_and_plan_vectors() {
    let bundle =
        ContractBundle::decode(include_bytes!("../../../../fixtures/compiler/bundle.bin")).unwrap();
    let original = &bundle.projections()[0];
    let mut fields = original.group_schema().measures().fields().to_vec();
    let mut measures = original.measures().to_vec();
    let first = fields.iter().map(|field| field.id().get()).max().unwrap() + 1;
    for id in first..first + 1024 {
        let field = FieldSchema::new(
            FieldId::new(id).unwrap(),
            format!("measure_{id}"),
            crate::ValueType::u64(),
        )
        .unwrap();
        measures.push(ProjectionMeasurePlan::count(field.clone()).unwrap());
        fields.push(field);
    }
    let group = ProjectionGroupSchema::new(
        original.projection_id(),
        original.group_schema().group_components().to_vec(),
        RecordSchema::new(
            RecordTypeRef::ProjectionResult(original.projection_id()),
            fields,
        )
        .unwrap(),
    )
    .unwrap();
    let projection = ProjectionPlan::new(
        original.projection_id(),
        "p".repeat(256),
        original.source_event(),
        original.expressions().clone(),
        original.filter(),
        original.key_expressions().to_vec(),
        measures,
        original.frontier(),
        group,
        bundle.schema(),
    )
    .unwrap();
    let mut writer = Writer::new(MAX_BUNDLE_BYTES);
    writer.u32(64).unwrap();
    for _ in 0..64 {
        encode_projection_bundle_entry(&mut writer, &projection).unwrap();
    }
    let bytes = writer.finish();
    let budget = ProviderBudget::new();
    let bound =
        projection_decode_admission::ProjectionSectionBound::inspect(&bytes, bundle.schema())
            .unwrap();
    let work = budget.reserve(bound.maximum().unwrap()).unwrap();
    let mut reader = Reader::new(&bytes);
    let ordinary = decode_projections(&mut reader, bundle.schema()).unwrap();
    reader.finish().unwrap();
    let native = riffdb_types::OwnedCapacityInspectionV1::inspect(&ordinary).unwrap();
    assert!(native.shared_allocations().is_empty());
    assert!(native.unshared_bytes() <= bound.heap);
    drop(ordinary);
    drop(work);
    let output =
        decode_provider_projection_section_admitted_v1(&bytes, bundle.schema(), &budget).unwrap();
    assert_eq!(output.len(), 64);
    assert!(output.iter().all(|plan| *plan == projection));
    drop(output);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}
