//! Structural schema decoding must admit retained and validation ownership.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::exact_provider_budget_v1::{EXACT_PROVIDER_BUDGET_BYTES, ProviderBudget};

fn section(schema: &SchemaIr) -> Vec<u8> {
    let mut writer = Writer::new(MAX_BUNDLE_BYTES);
    encode_schema(&mut writer, schema).unwrap();
    writer.finish()
}
fn frozen() -> [&'static [u8]; 13] {
    [
        include_bytes!("../../../../fixtures/compiler/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/production-embedding/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/reimport/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/cascade/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/vector-ann/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/secret/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/text-index/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/initialized-state-transition/bundle.bin")
            .as_slice(),
        include_bytes!("../../../../fixtures/compiler/secret-reveal/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/workflow-initial/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/indexed-selection/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/command-decision/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/aggregate-collection-budget/bundle.bin")
            .as_slice(),
    ]
}
#[test]
fn admitted_structural_schema_preserves_frozen_state_and_last_capture() {
    for bytes in frozen() {
        let bundle = ContractBundle::decode(bytes).unwrap();
        let bytes = section(bundle.schema());
        let budget = ProviderBudget::new();
        let output = decode_provider_structural_schema_admitted_v1(&bytes, &budget).unwrap();
        assert_eq!(**output, *bundle.schema());
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        let capture = output.clone();
        drop(output);
        assert!(budget.observation().unwrap().owned_bytes > 0);
        drop(capture);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
}

#[test]
fn admitted_structural_schema_native_inventory_covers_retained_and_global_forests() {
    for bytes in frozen() {
        let bundle = ContractBundle::decode(bytes).unwrap();
        let schema = bundle.schema();
        let bytes = section(schema);
        let budget = ProviderBudget::new();
        let bound = schema_decode_admission::SchemaSectionBound::inspect(&bytes).unwrap();
        let _work = budget.reserve(bound.maximum(bytes.len()).unwrap()).unwrap();
        fn native<T: riffdb_types::InspectOwnedCapacityV1>(value: &T) -> u64 {
            let inventory = riffdb_types::OwnedCapacityInspectionV1::inspect(value).unwrap();
            assert!(inventory.shared_allocations().is_empty());
            inventory.unshared_bytes()
        }
        assert!(native(schema) <= bound.heap);
        let indexes = schema
            .entities()
            .iter()
            .flat_map(EntitySchema::indexes)
            .map(IndexSchema::id)
            .collect::<BTreeSet<_>>();
        let invariants = schema
            .entities()
            .iter()
            .flat_map(EntitySchema::invariants)
            .chain(
                schema
                    .aggregates()
                    .iter()
                    .flat_map(AggregateSchema::invariants),
            )
            .map(InvariantPlan::id)
            .collect::<BTreeSet<_>>();
        // Native references have the same inline layout as the borrowed keys
        // and values; inspect layouts without traversing independently owned IR.
        let entities = schema
            .entities()
            .iter()
            .map(|entity| (entity.id(), 0_usize))
            .collect::<BTreeMap<_, _>>();
        let ownership = schema
            .aggregates()
            .iter()
            .flat_map(|aggregate| {
                std::iter::once(aggregate.root())
                    .chain(aggregate.children().iter().copied())
                    .map(move |entity| (entity, aggregate.id()))
            })
            .collect::<BTreeMap<_, _>>();
        let builder_pairs =
            Vec::<(EntityTypeId, usize)>::with_capacity((schema.entities().len() * 2).max(4));
        let mut forests = native(&indexes)
            + native(&invariants)
            + native(&entities)
            + native(&ownership)
            + native(&builder_pairs);
        for entity in schema.entities() {
            let fields = entity
                .primary_key_fields()
                .iter()
                .copied()
                .collect::<BTreeSet<_>>();
            forests += native(&fields);
            for index in entity.indexes() {
                let fields = index
                    .fields()
                    .iter()
                    .chain(index.cover_fields())
                    .copied()
                    .collect::<BTreeSet<_>>();
                assert!(
                    native(&fields)
                        <= plan_hash_admission::tree_bound::<FieldId>(index.fields().len())
                            .unwrap()
                            + plan_hash_admission::tree_bound::<FieldId>(
                                index.cover_fields().len()
                            )
                            .unwrap()
                );
            }
        }
        assert!(forests <= bound.constructors);
        assert!(native(schema) + forests <= bound.maximum(bytes.len()).unwrap());
        drop((indexes, invariants, entities, ownership, builder_pairs));
        let mut canonical = Writer::with_capacity(bytes.len(), bytes.len());
        encode_schema(&mut canonical, schema).unwrap();
        assert!(
            native(schema) + native(&canonical.finish()) <= bound.maximum(bytes.len()).unwrap()
        );
    }
}

#[test]
fn admitted_structural_schema_rejects_truncation_invalid_names_order_and_trailing_bytes() {
    let bundle = ContractBundle::decode(frozen()[0]).unwrap();
    let bytes = section(bundle.schema());
    let budget = ProviderBudget::new();
    for end in 0..bytes.len() {
        assert!(decode_provider_structural_schema_admitted_v1(&bytes[..end], &budget).is_err());
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }
    let mut invalid_name = bytes.clone();
    // entity count + first entity ID + name length precede its source name.
    invalid_name[12] = b'!';
    let ordinary = decode_schema(&mut Reader::new(&invalid_name)).unwrap_err();
    assert_eq!(
        decode_provider_structural_schema_admitted_v1(&invalid_name, &budget)
            .err()
            .unwrap(),
        ProviderBundleEncodingErrorV1::Invalid(ordinary)
    );
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert_eq!(
        decode_provider_structural_schema_admitted_v1(&trailing, &budget)
            .err()
            .unwrap(),
        ProviderBundleEncodingErrorV1::Invalid(IrValidationError::TrailingBytes)
    );
    let mut ceiling = bytes;
    ceiling[..4].copy_from_slice(&(crate::MAX_DECLARATIONS_PER_KIND as u32 + 1).to_be_bytes());
    assert!(matches!(
        decode_provider_structural_schema_admitted_v1(&ceiling, &budget),
        Err(ProviderBundleEncodingErrorV1::Invalid(
            IrValidationError::LimitExceeded {
                kind: "entities",
                ..
            }
        ))
    ));
    assert!(
        schema_decode_admission::SchemaSectionBound {
            heap: u64::MAX,
            constructors: 1
        }
        .maximum(1)
        .is_err()
    );
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

#[test]
fn admitted_structural_schema_native_large_records_registries_and_top_level_vectors() {
    use riffdb_types::OwnedCapacityInspectionV1;
    for (entity_count, field_count, variant_count) in [(1, 4096, 4096), (4096, 1, 1)] {
        let mut entities = Vec::with_capacity(entity_count);
        for index in 1..=entity_count {
            let id = EntityTypeId::new(index as u32).unwrap();
            let fields = (1..=field_count)
                .map(|field| {
                    FieldSchema::new(
                        FieldId::new(field as u32).unwrap(),
                        format!("field{field}"),
                        ValueType::u64(),
                    )
                    .unwrap()
                })
                .collect();
            let record = RecordSchema::new(RecordTypeRef::Entity(id), fields).unwrap();
            let key = KeySchema::new(
                KeyPurpose::Entity(id),
                vec![KeyComponentSchema::new(ValueType::u64(), vec![]).unwrap()],
            )
            .unwrap();
            entities.push(
                EntitySchema::new(
                    id,
                    format!("Entity{index}"),
                    record,
                    vec![FieldId::first()],
                    key,
                    vec![],
                    vec![],
                )
                .unwrap(),
            );
        }
        let enumeration = EnumSchema::new(
            EnumTypeId::first(),
            "State",
            (1..=variant_count)
                .map(|variant| {
                    EnumVariantSchema::new(
                        EnumVariantId::new(variant as u32).unwrap(),
                        format!("Variant{variant}"),
                    )
                    .unwrap()
                })
                .collect(),
        )
        .unwrap();
        let schema = SchemaIr::new(entities, vec![], vec![enumeration], vec![]).unwrap();
        let bytes = section(&schema);
        let bound = schema_decode_admission::SchemaSectionBound::inspect(&bytes).unwrap();
        let budget = ProviderBudget::new();
        let _work = budget.reserve(bound.maximum(bytes.len()).unwrap()).unwrap();
        let decoded = decode_schema(&mut Reader::new(&bytes)).unwrap();
        assert_eq!(decoded, schema);
        assert!(
            OwnedCapacityInspectionV1::inspect(&decoded)
                .unwrap()
                .unshared_bytes()
                <= bound.heap
        );
        let field_names = (0..field_count)
            .map(|value| [value, value])
            .collect::<BTreeSet<_>>();
        let variants = (0..variant_count)
            .map(|value| [value, value])
            .collect::<BTreeSet<_>>();
        let entity_names = (0..entity_count)
            .map(|value| [value, value])
            .collect::<BTreeSet<_>>();
        let enum_ids = Vec::<EnumVariantId>::with_capacity(variant_count);
        let constructors = OwnedCapacityInspectionV1::inspect(&field_names)
            .unwrap()
            .unshared_bytes()
            + OwnedCapacityInspectionV1::inspect(&variants)
                .unwrap()
                .unshared_bytes()
            + OwnedCapacityInspectionV1::inspect(&entity_names)
                .unwrap()
                .unshared_bytes()
            + OwnedCapacityInspectionV1::inspect(&enum_ids)
                .unwrap()
                .unshared_bytes();
        assert!(constructors <= bound.constructors);
        let output = decode_provider_structural_schema_admitted_v1(&bytes, &budget).unwrap();
        assert_eq!(**output, schema);
        drop(output);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        if field_count > 1 {
            let mut reordered = bytes;
            let mut reader = Reader::new(&reordered);
            reader.read(8).unwrap();
            reader.bytes(256).unwrap();
            decode_record_ref(&mut reader).unwrap();
            assert_eq!(reader.u32().unwrap(), field_count as u32);
            let first = reordered.len() - reader.remaining();
            reader.u32().unwrap();
            reader.bytes(256).unwrap();
            row_policy_admission::type_heap(&mut reader, 0).unwrap();
            let second = reordered.len() - reader.remaining();
            reader.u32().unwrap();
            reader.bytes(256).unwrap();
            row_policy_admission::type_heap(&mut reader, 0).unwrap();
            let end = reordered.len() - reader.remaining();
            let swapped = [&reordered[second..end], &reordered[first..second]].concat();
            reordered[first..end].copy_from_slice(&swapped);
            assert_eq!(decode_schema(&mut Reader::new(&reordered)).unwrap(), schema);
            assert!(matches!(
                decode_provider_structural_schema_admitted_v1(&reordered, &budget),
                Err(ProviderBundleEncodingErrorV1::Invalid(
                    IrValidationError::NonCanonicalOrder { .. }
                ))
            ));
        }
    }
}

#[test]
fn admitted_structural_schema_graph_workspace_covers_relationship_pair_and_route_forests() {
    let bundle = ContractBundle::decode(frozen()[3]).unwrap();
    let schema = bundle.schema();
    assert!(!schema.relationships().is_empty());
    let bytes = section(schema);
    let bound = schema_decode_admission::SchemaSectionBound::inspect(&bytes).unwrap();
    let budget = ProviderBudget::new();
    let _work = budget.reserve(bound.maximum(bytes.len()).unwrap()).unwrap();
    let nodes = schema
        .aggregates()
        .iter()
        .map(|aggregate| aggregate.keys().expressions().len())
        .max()
        .unwrap();
    let pairs = nodes
        .checked_mul(nodes)
        .unwrap()
        .min(crate::MAX_EXPRESSION_NODES + 1);
    let visited = (0..pairs)
        .map(|value| (ExprId::new(value as u32), ExprId::new(value as u32)))
        .collect::<BTreeSet<_>>();
    let pending = Vec::<(ExprId, ExprId)>::with_capacity(((pairs * 2 + 1) * 2).max(4));
    let routes = (0..nodes)
        .map(|value| ExprId::new(value as u32))
        .collect::<BTreeSet<_>>();
    let route_pending = Vec::<ExprId>::with_capacity(((nodes * 2 + 1) * 2).max(4));
    let reachable = Vec::<bool>::with_capacity(nodes);
    let roots = Vec::<ExprId>::with_capacity(2050);
    let reach_pending = Vec::<ExprId>::with_capacity(((nodes * 2 + 1025) * 2).max(4));
    let scratch = riffdb_types::OwnedCapacityInspectionV1::inspect(&(
        (visited, pending),
        ((routes, route_pending), (reachable, (roots, reach_pending))),
    ))
    .unwrap()
    .unshared_bytes();
    assert!(scratch <= bound.constructors);
    assert_eq!(
        **decode_provider_structural_schema_admitted_v1(&bytes, &budget).unwrap(),
        *schema
    );
}

#[test]
fn admitted_structural_schema_long_pattern_extension_preserves_all_operators_and_limits() {
    let entity = EntityTypeId::first();
    let title = FieldId::new(2).unwrap();
    let record = RecordSchema::new(
        RecordTypeRef::Entity(entity),
        vec![
            FieldSchema::new(FieldId::first(), "id", ValueType::u64()).unwrap(),
            FieldSchema::new(title, "title", ValueType::string(256).unwrap()).unwrap(),
        ],
    )
    .unwrap();
    let key = KeySchema::new(
        KeyPurpose::Entity(entity),
        vec![KeyComponentSchema::new(ValueType::u64(), vec![]).unwrap()],
    )
    .unwrap();
    let entity_schema = EntitySchema::new(
        entity,
        "Ticket",
        record,
        vec![FieldId::first()],
        key,
        vec![],
        vec![],
    )
    .unwrap();
    let bounds = riffdb_types::LongPatternBoundsV1::new(
        256, 256, 1, 256, 256, 256, 4096, 256, 256, 256, 256, 1, 256, 1,
    )
    .unwrap();
    let spec = crate::LongPatternSpecV1::new(
        entity,
        IndexId::first(),
        "by_title".to_owned(),
        title,
        riffdb_types::LongPatternProfileV1::BinaryUtf8V1,
        (1..=8)
            .map(|tag| riffdb_types::LongPatternOperatorV1::from_discriminant(tag).unwrap())
            .collect(),
        bounds,
        1,
        1,
        1,
        1,
        1,
    )
    .unwrap();
    let schema = SchemaIr::new(vec![entity_schema], vec![], vec![], vec![])
        .unwrap()
        .with_long_pattern_specs(vec![spec])
        .unwrap();
    let bytes = section(&schema);
    let budget = ProviderBudget::new();
    let bound = schema_decode_admission::SchemaSectionBound::inspect(&bytes).unwrap();
    let _work = budget.reserve(bound.maximum(bytes.len()).unwrap()).unwrap();
    let output = decode_provider_structural_schema_admitted_v1(&bytes, &budget).unwrap();
    assert_eq!(**output, schema);
    assert!(
        riffdb_types::OwnedCapacityInspectionV1::inspect(&**output)
            .unwrap()
            .unshared_bytes()
            <= bound.heap
    );
    let mut wrong_operator = bytes.clone();
    let mut reader = Reader::new(&bytes);
    // Locate the extension through its marker, without depending on total size.
    let marker = LONG_PATTERN_SPEC_SCHEMA_EXTENSION.to_be_bytes();
    let start = bytes
        .windows(4)
        .position(|window| window == marker)
        .unwrap();
    reader.read(start + 16).unwrap(); // marker, count, entity, index
    reader.bytes(256).unwrap();
    reader.read(5).unwrap();
    assert_eq!(reader.u32().unwrap(), 8);
    let position = bytes.len() - reader.remaining();
    wrong_operator[position] = 255;
    let ordinary = decode_schema(&mut Reader::new(&wrong_operator)).unwrap_err();
    assert_eq!(
        decode_provider_structural_schema_admitted_v1(&wrong_operator, &budget)
            .err()
            .unwrap(),
        ProviderBundleEncodingErrorV1::Invalid(ordinary)
    );
}
#[test]
fn admitted_structural_schema_refuses_before_construction() {
    let bundle =
        ContractBundle::decode(include_bytes!("../../../../fixtures/compiler/bundle.bin")).unwrap();
    let bytes = section(bundle.schema());
    for remaining in [128, 64 * 1024] {
        let budget = ProviderBudget::new();
        let fixed = budget.observation().unwrap().fixed_owner_bytes;
        let _pressure = budget
            .reserve(EXACT_PROVIDER_BUDGET_BYTES - fixed - remaining)
            .unwrap();
        let before = budget.observation().unwrap();
        assert!(matches!(
            decode_provider_structural_schema_admitted_v1(&bytes, &budget),
            Err(ProviderBundleEncodingErrorV1::Unavailable)
        ));
        assert_eq!(budget.observation().unwrap(), before);
    }
}
