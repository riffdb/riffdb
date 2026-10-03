//! Policy decoding must admit its two fact maps and reconstruction workspace.
// req: OQ-040, PRJ-004
use super::*;
use crate::row_policy::*;
use riffdb_types::exact_provider_budget_v1::{EXACT_PROVIDER_BUDGET_BYTES, ProviderBudget};

fn section(catalog: &RowPolicyCatalogV1) -> Vec<u8> {
    let mut writer = Writer::new(MAX_BUNDLE_BYTES);
    encode_row_policy_catalog(&mut writer, catalog).unwrap();
    writer.finish()
}

#[test]
fn admitted_row_policy_parser_preserves_frozen_catalogs_and_final_capture_charge() {
    for bytes in [
        include_bytes!("../../../../fixtures/compiler/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/reimport/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/secret/bundle.bin").as_slice(),
    ] {
        let bundle = ContractBundle::decode(bytes).unwrap();
        let bytes = section(&bundle.row_policies);
        let budget = ProviderBudget::new();
        let output =
            decode_provider_row_policy_section_admitted_v1(&bytes, bundle.schema(), &budget)
                .unwrap();
        assert_eq!(**output, bundle.row_policies);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        let capture = output.clone();
        drop(output);
        assert!(budget.observation().unwrap().owned_bytes > 0);
        drop(capture);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
}

#[test]
fn admitted_row_policy_parser_refuses_before_construction_under_pressure() {
    let bytes = section(&RowPolicyCatalogV1::empty());
    let bundle =
        ContractBundle::decode(include_bytes!("../../../../fixtures/compiler/bundle.bin")).unwrap();
    for remaining in [128, 64 * 1024] {
        let budget = ProviderBudget::new();
        let fixed = budget.observation().unwrap().fixed_owner_bytes;
        let pressure = budget
            .reserve(EXACT_PROVIDER_BUDGET_BYTES - fixed - remaining)
            .unwrap();
        let before = budget.observation().unwrap();
        assert!(matches!(
            decode_provider_row_policy_section_admitted_v1(&bytes, bundle.schema(), &budget),
            Err(ProviderBundleEncodingErrorV1::Unavailable)
        ));
        assert_eq!(budget.observation().unwrap(), before);
        drop(pressure);
    }
}

fn populated(schema: &SchemaIr) -> RowPolicyCatalogV1 {
    let entity = schema.entities()[0].id();
    let facts = vec![
        PrincipalFactSchemaV1::new("Enabled", ValueType::bool()).unwrap(),
        PrincipalFactSchemaV1::new(
            "Groups",
            ValueType::list(ValueType::string(32).unwrap(), 16).unwrap(),
        )
        .unwrap(),
    ];
    let fact_map = facts
        .iter()
        .map(|fact| (fact.name().to_owned(), fact.value_type().clone()))
        .collect::<BTreeMap<_, _>>();
    let read = RowPolicyRuleV1::new(
        RowPolicyOperationV1::Read,
        vec![RowPolicyExpressionNodeV1::Operand(RowPolicyOperandV1::new(
            RowPolicyValueSourceV1::PrincipalFact("Enabled".to_owned()),
            ValueType::bool(),
        ))],
        0,
        entity,
        schema,
        &fact_map,
    )
    .unwrap();
    let update = RowPolicyRuleV1::new(
        RowPolicyOperationV1::Update,
        vec![
            RowPolicyExpressionNodeV1::Operand(RowPolicyOperandV1::new(
                RowPolicyValueSourceV1::Constant(
                    riffdb_types::CanonicalValue::list(vec![
                        riffdb_types::CanonicalValue::string("member").unwrap(),
                        riffdb_types::CanonicalValue::string("reviewer").unwrap(),
                    ])
                    .unwrap(),
                ),
                ValueType::list(ValueType::string(32).unwrap(), 16).unwrap(),
            )),
            RowPolicyExpressionNodeV1::Operand(RowPolicyOperandV1::new(
                RowPolicyValueSourceV1::Constant(
                    riffdb_types::CanonicalValue::string("member").unwrap(),
                ),
                ValueType::string(32).unwrap(),
            )),
            RowPolicyExpressionNodeV1::In {
                needle: 1,
                haystack: 0,
            },
        ],
        2,
        entity,
        schema,
        &fact_map,
    )
    .unwrap();
    RowPolicyCatalogV1::new(
        facts,
        vec![RowPolicyPlanV1::new("Access", entity, vec![read, update], schema).unwrap()],
        schema,
    )
    .unwrap()
}

#[test]
fn admitted_row_policy_native_capacity_covers_both_maps_replacement_vectors_and_type_clones() {
    let bundle =
        ContractBundle::decode(include_bytes!("../../../../fixtures/compiler/bundle.bin")).unwrap();
    let policy = populated(bundle.schema());
    let bytes = section(&policy);
    let budget = ProviderBudget::new();
    let bound = row_policy_admission::RowPolicySectionBound::inspect(&bytes).unwrap();
    {
        let mut work = budget.reserve(bound.maximum().unwrap()).unwrap();
        let _scratch = work
            .split(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
            .unwrap();
        let mut reader = Reader::new(&bytes);
        let decoded = decode_row_policy_catalog(&mut reader, bundle.schema()).unwrap();
        reader.finish().unwrap();
        let inventory = riffdb_types::OwnedCapacityInspectionV1::inspect(&decoded).unwrap();
        assert!(inventory.shared_allocations().is_empty());
        assert!(inventory.unshared_bytes() <= bound.heap);
        let first = decoded
            .facts()
            .iter()
            .map(|fact| (fact.name().to_owned(), fact.value_type().clone()))
            .collect::<BTreeMap<_, _>>();
        let second = first.clone();
        let first_bytes = riffdb_types::OwnedCapacityInspectionV1::inspect(&first)
            .unwrap()
            .unshared_bytes();
        let second_bytes = riffdb_types::OwnedCapacityInspectionV1::inspect(&second)
            .unwrap()
            .unshared_bytes();
        assert!(first_bytes <= bound.fact_map);
        assert!(second_bytes <= bound.fact_map);
        let pair_inputs =
            Vec::<(String, ValueType)>::with_capacity((decoded.facts().len() * 2).max(4));
        let input_bytes = riffdb_types::OwnedCapacityInspectionV1::inspect(&pair_inputs)
            .unwrap()
            .unshared_bytes();
        assert!(first_bytes + input_bytes <= bound.fact_map);
        let extra_policies = Vec::<RowPolicyPlanV1>::with_capacity(decoded.policies().len());
        for policy in decoded.policies() {
            let extra_rules = Vec::<RowPolicyRuleV1>::with_capacity(policy.rules().len());
            let policy_copy_bytes =
                riffdb_types::OwnedCapacityInspectionV1::inspect(&extra_policies)
                    .unwrap()
                    .unshared_bytes();
            let rule_copy_bytes = riffdb_types::OwnedCapacityInspectionV1::inspect(&extra_rules)
                .unwrap()
                .unshared_bytes();
            assert!(policy_copy_bytes + rule_copy_bytes <= bound.replacement_vectors);
            for rule in policy.rules() {
                let types = rule
                    .nodes()
                    .iter()
                    .map(|node| match node {
                        RowPolicyExpressionNodeV1::Operand(operand) => operand.value_type().clone(),
                        _ => ValueType::bool(),
                    })
                    .collect::<Vec<_>>();
                let widths = vec![1usize; rule.nodes().len()];
                let scratch =
                    riffdb_types::OwnedCapacityInspectionV1::inspect(&(types, widths)).unwrap();
                assert!(scratch.unshared_bytes() <= bound.rule_scratch);
                assert!(
                    inventory.unshared_bytes()
                        + first_bytes
                        + second_bytes
                        + scratch.unshared_bytes()
                        <= bound.heap + bound.constructors().unwrap()
                );
            }
        }
        assert!(
            riffdb_types::OwnedCapacityInspectionV1::inspect(&extra_policies)
                .unwrap()
                .unshared_bytes()
                <= bound.replacement_vectors
        );
        drop(first);
        drop(second);
        drop(pair_inputs);
        drop(extra_policies);
        let mut writer = Writer::with_capacity(bytes.len(), bytes.len());
        encode_row_policy_catalog(&mut writer, &decoded).unwrap();
        let canonical = writer.finish();
        assert!(
            riffdb_types::OwnedCapacityInspectionV1::inspect(&canonical)
                .unwrap()
                .unshared_bytes()
                <= bound.canonical
        );
        assert_eq!(canonical, bytes);
    }
    let output =
        decode_provider_row_policy_section_admitted_v1(&bytes, bundle.schema(), &budget).unwrap();
    assert_eq!(**output, policy);
    drop(output);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

#[test]
fn borrowed_policy_type_capacity_covers_every_tag_and_deep_owned_boxes() {
    let mut values = vec![
        ValueType::bool(),
        ValueType::i64(),
        ValueType::u64(),
        ValueType::decimal(DecimalSpec::new(12, 2).unwrap()),
        ValueType::money(CurrencyCode::new("USD").unwrap()),
        ValueType::string(64).unwrap(),
        ValueType::bytes(64).unwrap(),
        ValueType::timestamp(),
        ValueType::date(),
        ValueType::uuid(),
        ValueType::enumeration(EnumTypeId::new(1).unwrap()),
        ValueType::optional(ValueType::string(64).unwrap()).unwrap(),
        ValueType::list(ValueType::optional(ValueType::u64()).unwrap(), 16).unwrap(),
        ValueType::vector(riffdb_types::VectorDimension::new(3).unwrap()),
    ];
    for owner in [
        RecordTypeRef::Entity(EntityTypeId::new(1).unwrap()),
        RecordTypeRef::Event(EventTypeId::new(1).unwrap()),
        RecordTypeRef::CommandInput(CommandId::new(1).unwrap()),
        RecordTypeRef::CommandOutcome {
            command_id: CommandId::new(1).unwrap(),
            outcome_id: OutcomeId::new(1).unwrap(),
        },
        RecordTypeRef::ProjectionResult(ProjectionId::new(1).unwrap()),
    ] {
        values.push(ValueType::record(owner));
    }
    let mut deep = ValueType::bool();
    for _ in 0..31 {
        deep = ValueType::list(deep, 1).unwrap();
    }
    values.push(deep);
    let budget = ProviderBudget::new();
    let _scratch = budget
        .reserve(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    for value in values {
        let mut writer = Writer::new(MAX_BUNDLE_BYTES);
        encode_value_type(&mut writer, &value).unwrap();
        let bytes = writer.finish();
        let mut reader = Reader::new(&bytes);
        let bound = row_policy_admission::type_heap(&mut reader, 0).unwrap();
        reader.finish().unwrap();
        let _work = budget.reserve(bound + 32 * 1024).unwrap();
        let decoded = decode_value_type(&mut Reader::new(&bytes), 0).unwrap();
        assert_eq!(decoded, value);
        let inventory = riffdb_types::OwnedCapacityInspectionV1::inspect(&decoded).unwrap();
        assert!(inventory.shared_allocations().is_empty());
        assert!(inventory.unshared_bytes() <= bound);
    }
    let mut too_deep = vec![value_type_tag::LIST; 32];
    too_deep.push(value_type_tag::BOOL);
    for _ in 0..32 {
        too_deep.extend_from_slice(&1u32.to_le_bytes());
    }
    let expected = decode_value_type(&mut Reader::new(&too_deep), 0).unwrap_err();
    assert_eq!(
        row_policy_admission::type_heap(&mut Reader::new(&too_deep), 0),
        Err(ProviderBundleEncodingErrorV1::Invalid(expected))
    );
}

#[test]
fn admitted_row_policy_parser_rejects_truncation_noncanonical_order_and_invalid_rules() {
    let bundle =
        ContractBundle::decode(include_bytes!("../../../../fixtures/compiler/bundle.bin")).unwrap();
    let policy = populated(bundle.schema());
    let bytes = section(&policy);
    let budget = ProviderBudget::new();
    for end in 0..bytes.len() {
        assert!(
            decode_provider_row_policy_section_admitted_v1(&bytes[..end], bundle.schema(), &budget)
                .is_err()
        );
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
    let mut unsupported = bytes.clone();
    unsupported[..4].copy_from_slice(&u32::MAX.to_le_bytes());
    let mut hostile_facts = bytes.clone();
    hostile_facts[4..8].copy_from_slice(&(MAX_PRINCIPAL_FACT_SCHEMAS_V1 as u32 + 1).to_le_bytes());
    let mut writer = Writer::new(MAX_BUNDLE_BYTES);
    writer.u32(ROW_POLICY_CATALOG_VERSION_V1).unwrap();
    writer.u32(0).unwrap();
    writer.u32(1).unwrap();
    writer.string("InvalidRule").unwrap();
    writer
        .u32(bundle.schema().entities()[0].id().get())
        .unwrap();
    writer.u32(1).unwrap();
    writer.u8(1).unwrap(); // existing Read operation tag
    writer.u32(0).unwrap();
    writer.u32(1).unwrap();
    writer.u8(1).unwrap(); // operand
    writer.u8(5).unwrap(); // constant
    writer.bytes(&[1, 255]).unwrap(); // unsupported canonical value tag
    encode_value_type(&mut writer, &ValueType::bool()).unwrap();
    let invalid_constant = writer.finish();
    let mut writer = Writer::new(MAX_BUNDLE_BYTES);
    writer.u32(ROW_POLICY_CATALOG_VERSION_V1).unwrap();
    writer.u32(1).unwrap();
    writer.string("BrokenType").unwrap();
    writer.u8(value_type_tag::STRING).unwrap();
    writer.u32(0).unwrap();
    writer.u32(0).unwrap();
    let invalid_type = writer.finish();
    for bytes in [unsupported, hostile_facts, invalid_constant, invalid_type] {
        let expected =
            decode_row_policy_catalog(&mut Reader::new(&bytes), bundle.schema()).unwrap_err();
        assert!(
            matches!(decode_provider_row_policy_section_admitted_v1(&bytes, bundle.schema(), &budget),
            Err(ProviderBundleEncodingErrorV1::Invalid(error)) if error == expected)
        );
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
    let mut reader = Reader::new(&bytes);
    reader.read(8).unwrap();
    let first_start = bytes.len() - reader.remaining();
    reader.bytes(256).unwrap();
    row_policy_admission::type_heap(&mut reader, 0).unwrap();
    let second_start = bytes.len() - reader.remaining();
    reader.bytes(256).unwrap();
    row_policy_admission::type_heap(&mut reader, 0).unwrap();
    let end = bytes.len() - reader.remaining();
    let mut reversed = bytes[..first_start].to_vec();
    reversed.extend_from_slice(&bytes[second_start..end]);
    reversed.extend_from_slice(&bytes[first_start..second_start]);
    reversed.extend_from_slice(&bytes[end..]);
    assert_eq!(
        decode_row_policy_catalog(&mut Reader::new(&reversed), bundle.schema()).unwrap(),
        policy
    );
    assert!(matches!(
        decode_provider_row_policy_section_admitted_v1(&reversed, bundle.schema(), &budget),
        Err(ProviderBundleEncodingErrorV1::Invalid(
            IrValidationError::NonCanonicalOrder {
                kind: "canonical contract bundle bytes"
            }
        ))
    ));
    let mut trailing = bytes;
    trailing.push(0);
    assert!(matches!(
        decode_provider_row_policy_section_admitted_v1(&trailing, bundle.schema(), &budget),
        Err(ProviderBundleEncodingErrorV1::Invalid(
            IrValidationError::TrailingBytes
        ))
    ));
    let bound = row_policy_admission::RowPolicySectionBound {
        heap: u64::MAX,
        fact_map: 1,
        replacement_vectors: 0,
        rule_scratch: 0,
        canonical: 0,
    };
    assert_eq!(
        bound.maximum(),
        Err(riffdb_types::exact_provider_budget_v1::BudgetUnavailable)
    );
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}
