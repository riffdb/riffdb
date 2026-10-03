//! Compatibility parser admission preserves existing report semantics.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::exact_provider_budget_v1::{EXACT_PROVIDER_BUDGET_BYTES, ProviderBudget};

fn section(report: &CompatibilityReport) -> Vec<u8> {
    let mut writer = Writer::new(MAX_BUNDLE_BYTES);
    encode_compatibility(&mut writer, report).unwrap();
    writer.finish()
}

#[test]
fn admitted_compatibility_parser_preserves_reports_and_final_capture_charge() {
    let mut reports = vec![
        CompatibilityReport::genesis(),
        CompatibilityReport::successor(vec![]).unwrap(),
        CompatibilityReport::successor(vec![
            CompatibilityEntry::new(CompatibilityCode::AddedCommand, "command:2").unwrap(),
            CompatibilityEntry::new(CompatibilityCode::AddedOptionalField, "entity:1/field:2")
                .unwrap(),
        ])
        .unwrap(),
    ];
    reports.push(
        CompatibilityReport::successor(
            [
                "aggregate:1/invariant:2",
                "entity:1/delete-policy",
                "enum:1/variant:2",
                "event:1/field:2",
                "projection:1/field:2",
                "command:1/input/field:2",
                "command:1/outcome:2",
                "command:1/outcome:2/field:3",
            ]
            .into_iter()
            .map(|path| CompatibilityEntry::new(CompatibilityCode::AddedCommand, path).unwrap())
            .collect(),
        )
        .unwrap(),
    );
    for bytes in [
        include_bytes!("../../../../fixtures/compiler/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/reimport/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/workflow-initial/bundle.bin").as_slice(),
    ] {
        reports.push(
            ContractBundle::decode(bytes)
                .unwrap()
                .compatibility()
                .clone(),
        );
    }
    for report in reports {
        let bytes = section(&report);
        let budget = ProviderBudget::new();
        let output = decode_provider_compatibility_section_admitted_v1(
            &bytes,
            !report.entries().is_empty(),
            &budget,
        )
        .unwrap();
        assert_eq!(**output, report);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        let captured = output.clone();
        drop(output);
        assert!(budget.observation().unwrap().owned_bytes > 0);
        drop(captured);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
}

#[test]
fn admitted_compatibility_parser_refuses_before_construction_under_pressure() {
    let bytes = section(&CompatibilityReport::genesis());
    for remaining in [128, 64 * 1024] {
        let budget = ProviderBudget::new();
        let fixed = budget.observation().unwrap().fixed_owner_bytes;
        let pressure = budget
            .reserve(EXACT_PROVIDER_BUDGET_BYTES - fixed - remaining)
            .unwrap();
        let before = budget.observation().unwrap();
        assert!(matches!(
            decode_provider_compatibility_section_admitted_v1(&bytes, false, &budget),
            Err(ProviderBundleEncodingErrorV1::Unavailable)
        ));
        assert_eq!(budget.observation().unwrap(), before);
        drop(pressure);
    }
}

#[test]
fn admitted_compatibility_parser_native_capacity_witnesses_cover_large_reports_and_paths() {
    let report = CompatibilityReport::successor(
        (1..=4_096)
            .map(|id| {
                CompatibilityEntry::new(
                    CompatibilityCode::AddedOptionalOutcomeField,
                    format!("command:{id}/outcome:4294967295/field:4294967295"),
                )
                .unwrap()
            })
            .collect(),
    )
    .unwrap();
    for report in [CompatibilityReport::genesis(), report] {
        let bytes = section(&report);
        let has_parent = !report.entries().is_empty();
        let bound = compatibility_admission::CompatibilitySectionBound::inspect(&bytes, has_parent)
            .unwrap();
        let budget = ProviderBudget::new();
        let mut work = budget.reserve(bound.maximum().unwrap()).unwrap();
        let _scratch = work
            .split(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
            .unwrap();
        let decoded = decode_compatibility(&mut Reader::new(&bytes), has_parent).unwrap();
        let inventory = riffdb_types::OwnedCapacityInspectionV1::inspect(&decoded).unwrap();
        assert!(inventory.shared_allocations().is_empty());
        assert!(inventory.unshared_bytes() <= bound.heap);
        for entry in decoded.entries() {
            let parts = entry.affected_path().split('/').collect::<Vec<_>>();
            let native = 2 * parts.capacity() * std::mem::size_of::<&str>()
                + 2 * std::mem::align_of::<&str>()
                + 256;
            assert!(native as u64 <= bound.constructors);
        }
        let mut writer = Writer::with_capacity(bytes.len(), bytes.len());
        encode_compatibility(&mut writer, &decoded).unwrap();
        let canonical = writer.finish();
        let inventory = riffdb_types::OwnedCapacityInspectionV1::inspect(&canonical).unwrap();
        assert!(inventory.unshared_bytes() <= bound.canonical);
        assert_eq!(canonical, bytes);
    }
    // Invalid slash-dense paths still collect all parts before validation.
    let mut writer = Writer::new(MAX_BUNDLE_BYTES);
    writer.u8(CompatibilityClass::Compatible as u8).unwrap();
    writer.u32(1).unwrap();
    writer
        .string(CompatibilityCode::AddedCommand.as_str())
        .unwrap();
    let path = "/".repeat(1_024);
    writer.string(&path).unwrap();
    let bytes = writer.finish();
    let bound = compatibility_admission::CompatibilitySectionBound::inspect(&bytes, true).unwrap();
    let budget = ProviderBudget::new();
    let mut work = budget.reserve(bound.maximum().unwrap()).unwrap();
    let _scratch = work
        .split(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let parts = path.split('/').collect::<Vec<_>>();
    let native =
        2 * parts.capacity() * std::mem::size_of::<&str>() + 2 * std::mem::align_of::<&str>() + 256;
    assert!(native as u64 <= bound.constructors);
    drop(parts);
    drop(_scratch);
    drop(work);
    let expected = decode_compatibility(&mut Reader::new(&bytes), true).unwrap_err();
    assert!(matches!(
        decode_provider_compatibility_section_admitted_v1(&bytes, true, &budget),
        Err(ProviderBundleEncodingErrorV1::Invalid(error)) if error == expected
    ));
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}

#[test]
fn admitted_compatibility_parser_rejects_truncation_noncanonical_and_invalid_reports() {
    let report = CompatibilityReport::successor(vec![
        CompatibilityEntry::new(CompatibilityCode::AddedCommand, "command:1").unwrap(),
        CompatibilityEntry::new(CompatibilityCode::AddedCommand, "command:2").unwrap(),
    ])
    .unwrap();
    let bytes = section(&report);
    let budget = ProviderBudget::new();
    for end in 0..bytes.len() {
        assert!(
            decode_provider_compatibility_section_admitted_v1(&bytes[..end], true, &budget)
                .is_err()
        );
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
    let mut writer = Writer::new(MAX_BUNDLE_BYTES);
    writer.u8(report.overall() as u8).unwrap();
    writer.u32(2).unwrap();
    for entry in report.entries().iter().rev() {
        writer.string(entry.code().as_str()).unwrap();
        writer.string(entry.affected_path()).unwrap();
    }
    let reversed = writer.finish();
    assert_eq!(
        decode_compatibility(&mut Reader::new(&reversed), true).unwrap(),
        report
    );
    for bytes in [reversed, section(&CompatibilityReport::genesis())] {
        assert!(matches!(
            decode_provider_compatibility_section_admitted_v1(&bytes, true, &budget),
            Err(ProviderBundleEncodingErrorV1::Invalid(
                IrValidationError::NonCanonicalOrder {
                    kind: "canonical contract bundle bytes"
                }
            ))
        ));
    }
    let mut incorrect_class = bytes.clone();
    incorrect_class[0] = CompatibilityClass::Incompatible as u8;
    let mut hostile_count = bytes.clone();
    hostile_count[1..5].copy_from_slice(&4_097u32.to_le_bytes());
    let mut trailing = bytes.clone();
    trailing.push(0);
    for (bytes, parent) in [
        (incorrect_class, true),
        (hostile_count, true),
        (bytes, false),
    ] {
        let expected = decode_compatibility(&mut Reader::new(&bytes), parent).unwrap_err();
        assert!(matches!(
            decode_provider_compatibility_section_admitted_v1(&bytes, parent, &budget),
            Err(ProviderBundleEncodingErrorV1::Invalid(error)) if error == expected
        ));
    }
    assert!(matches!(
        decode_provider_compatibility_section_admitted_v1(&trailing, true, &budget),
        Err(ProviderBundleEncodingErrorV1::Invalid(
            IrValidationError::TrailingBytes
        ))
    ));
    let bound = compatibility_admission::CompatibilitySectionBound {
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
