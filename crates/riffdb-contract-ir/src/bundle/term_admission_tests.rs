//! Key and expression parsing must reserve before copying or construction.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::exact_provider_budget_v1::{EXACT_PROVIDER_BUDGET_BYTES, ProviderBudget};

fn key_bytes(key: &KeySchema) -> Vec<u8> {
    let mut writer = Writer::new(MAX_BUNDLE_BYTES);
    encode_key_schema(&mut writer, key).unwrap();
    writer.finish()
}
fn arena_bytes(arena: &ExpressionArena) -> Vec<u8> {
    let mut writer = Writer::new(MAX_BUNDLE_BYTES);
    encode_expression_arena(&mut writer, arena).unwrap();
    writer.finish()
}

#[test]
fn admitted_key_and_expression_parsing_preserves_frozen_ir_and_final_captures() {
    for bytes in [
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
    ] {
        let bundle = ContractBundle::decode(bytes).unwrap();
        let budget = ProviderBudget::new();
        for entity in bundle.schema().entities() {
            let mut keys = vec![entity.primary_key()];
            keys.extend(entity.indexes().iter().map(|index| index.key_schema()));
            for key in keys {
                let bytes = key_bytes(key);
                let output = decode_provider_key_schema_admitted_v1(&bytes, &budget).unwrap();
                assert_eq!(**output, *key);
                let capture = output.clone();
                drop(output);
                assert!(budget.observation().unwrap().owned_bytes > 0);
                drop(capture);
                assert_eq!(budget.observation().unwrap().owned_bytes, 0);
            }
        }
        for command in bundle.commands() {
            let bytes = arena_bytes(command.expressions());
            let output =
                decode_provider_expression_arena_admitted_v1(&bytes, bundle.ir_version(), &budget)
                    .unwrap();
            assert_eq!(**output, *command.expressions());
            drop(output);
            assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        }
    }
}

#[test]
fn admitted_term_bounds_cover_native_constructor_overlap_and_canonical_buffers() {
    let budget = ProviderBudget::new();
    let _census = budget
        .reserve(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    fn native<T: riffdb_types::InspectOwnedCapacityV1>(value: &T) -> u64 {
        riffdb_types::OwnedCapacityInspectionV1::inspect(value)
            .unwrap()
            .unshared_bytes()
    }
    let entity = KeySchema::new(
        KeyPurpose::Entity(EntityTypeId::first()),
        vec![KeyComponentSchema::new(ValueType::u64(), vec![]).unwrap()],
    )
    .unwrap();
    for component in [
        KeyComponentSchema::new(
            ValueType::enumeration(EnumTypeId::first()),
            (1..=4096)
                .map(|id| EnumVariantId::new(id).unwrap())
                .collect(),
        )
        .unwrap(),
        KeyComponentSchema::ordered_bytes(32).unwrap(),
    ] {
        let key = KeySchema::index(
            IndexId::first(),
            EntityTypeId::first(),
            vec![component],
            entity.clone(),
        )
        .unwrap();
        let bytes = key_bytes(&key);
        let bound = term_admission::key_bound(&mut Reader::new(&bytes), 0).unwrap();
        assert!(native(&key) <= bound.heap);
        let mut canonical = Writer::with_capacity(bytes.len(), bytes.len());
        encode_key_schema(&mut canonical, &key).unwrap();
        assert!(native(&key) + native(&canonical.finish()) <= bound.maximum(bytes.len()).unwrap());
        let output = decode_provider_key_schema_admitted_v1(&bytes, &budget).unwrap();
        assert_eq!(**output, key);
    }
    for count in [0, 1, 5, crate::MAX_EXPRESSION_NODES] {
        let arena = ExpressionArena::new(
            (0..count)
                .map(|_| {
                    (
                        ExpressionKind::Constant(riffdb_types::CanonicalValue::Null),
                        ValueType::optional(ValueType::string(256).unwrap()).unwrap(),
                    )
                })
                .collect(),
        )
        .unwrap();
        let bytes = arena_bytes(&arena);
        let bound =
            term_admission::expression_bound(&mut Reader::new(&bytes), EXECUTABLE_IR_VERSION_V26)
                .unwrap();
        assert!(native(&arena) <= bound.heap);
        let input = Vec::<(ExpressionKind, ValueType)>::with_capacity(count);
        let depths = Vec::<usize>::with_capacity(count);
        assert!(native(&input) + native(&depths) <= bound.constructors);
        assert!(
            native(&arena) + native(&input) + native(&depths)
                <= bound.maximum(bytes.len()).unwrap()
        );
        drop((input, depths));
        let mut canonical = Writer::with_capacity(bytes.len(), bytes.len());
        encode_expression_arena(&mut canonical, &arena).unwrap();
        assert!(
            native(&arena) + native(&canonical.finish()) <= bound.maximum(bytes.len()).unwrap()
        );
        let output = decode_provider_expression_arena_admitted_v1(
            &bytes,
            EXECUTABLE_IR_VERSION_V26,
            &budget,
        )
        .unwrap();
        assert_eq!(**output, arena);
    }
}

#[test]
fn admitted_terms_preserve_semantic_rejection_canonicality_and_release_on_error() {
    let bundle =
        ContractBundle::decode(include_bytes!("../../../../fixtures/compiler/bundle.bin")).unwrap();
    let budget = ProviderBudget::new();
    let key = key_bytes(bundle.schema().entities()[0].primary_key());
    let arena = arena_bytes(bundle.commands()[0].expressions());
    for end in 0..key.len() {
        assert!(decode_provider_key_schema_admitted_v1(&key[..end], &budget).is_err());
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }
    for end in 0..arena.len() {
        assert!(
            decode_provider_expression_arena_admitted_v1(
                &arena[..end],
                bundle.ir_version(),
                &budget
            )
            .is_err()
        );
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }
    let mut wrong_maximum = key.clone();
    let position = wrong_maximum.len() - 5;
    wrong_maximum[position..position + 4].copy_from_slice(&0_u32.to_be_bytes());
    let ordinary = decode_key_schema(&mut Reader::new(&wrong_maximum), 0).unwrap_err();
    assert_eq!(
        decode_provider_key_schema_admitted_v1(&wrong_maximum, &budget)
            .err()
            .unwrap(),
        ProviderBundleEncodingErrorV1::Invalid(ordinary)
    );
    let mut nonforward = Writer::new(MAX_BUNDLE_BYTES);
    nonforward.u32(1).unwrap();
    nonforward.u8(expression_tag::UNARY).unwrap();
    encode_value_type(&mut nonforward, &ValueType::bool()).unwrap();
    nonforward.u8(UnaryOperator::Not as u8).unwrap();
    nonforward.u32(0).unwrap();
    assert_eq!(
        decode_provider_expression_arena_admitted_v1(
            &nonforward.finish(),
            EXECUTABLE_IR_VERSION_V26,
            &budget
        )
        .err()
        .unwrap(),
        ProviderBundleEncodingErrorV1::Invalid(IrValidationError::NonForwardExpression)
    );
    let entity = KeySchema::new(
        KeyPurpose::Entity(EntityTypeId::first()),
        vec![
            KeyComponentSchema::new(
                ValueType::enumeration(EnumTypeId::first()),
                vec![EnumVariantId::first(), EnumVariantId::new(2).unwrap()],
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let mut reordered = key_bytes(&entity);
    let mut reader = Reader::new(&reordered);
    reader.read(13).unwrap();
    row_policy_admission::type_heap(&mut reader, 0).unwrap();
    assert_eq!(reader.u32().unwrap(), 2);
    let start = reordered.len() - reader.remaining();
    reordered[start..start + 4].copy_from_slice(&2_u32.to_be_bytes());
    reordered[start + 4..start + 8].copy_from_slice(&1_u32.to_be_bytes());
    assert_eq!(
        decode_key_schema(&mut Reader::new(&reordered), 0).unwrap(),
        entity
    );
    assert!(matches!(
        decode_provider_key_schema_admitted_v1(&reordered, &budget),
        Err(ProviderBundleEncodingErrorV1::Invalid(
            IrValidationError::NonCanonicalOrder { .. }
        ))
    ));
    let mut trailing = key;
    trailing.push(0);
    assert_eq!(
        decode_provider_key_schema_admitted_v1(&trailing, &budget)
            .err()
            .unwrap(),
        ProviderBundleEncodingErrorV1::Invalid(IrValidationError::TrailingBytes)
    );
    for version in [0, u32::MAX] {
        assert!(matches!(
            decode_provider_expression_arena_admitted_v1(&arena, version, &budget),
            Err(ProviderBundleEncodingErrorV1::Invalid(
                IrValidationError::UnsupportedVersion { .. }
            ))
        ));
    }
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert!(
        term_admission::TermBound {
            heap: u64::MAX,
            constructors: 1
        }
        .maximum(1)
        .is_err()
    );
}

#[test]
fn admitted_key_and_expression_parsing_refuses_before_construction() {
    let bundle =
        ContractBundle::decode(include_bytes!("../../../../fixtures/compiler/bundle.bin")).unwrap();
    let bytes = key_bytes(bundle.schema().entities()[0].primary_key());
    let arena = arena_bytes(&ExpressionArena::empty());
    for remaining in [128, 64 * 1024] {
        let budget = ProviderBudget::new();
        let fixed = budget.observation().unwrap().fixed_owner_bytes;
        let _pressure = budget
            .reserve(EXACT_PROVIDER_BUDGET_BYTES - fixed - remaining)
            .unwrap();
        let before = budget.observation().unwrap();
        assert!(matches!(
            decode_provider_key_schema_admitted_v1(&bytes, &budget),
            Err(ProviderBundleEncodingErrorV1::Unavailable)
        ));
        assert_eq!(budget.observation().unwrap(), before);
        assert!(matches!(
            decode_provider_expression_arena_admitted_v1(
                &arena,
                EXECUTABLE_IR_VERSION_V26,
                &budget
            ),
            Err(ProviderBundleEncodingErrorV1::Unavailable)
        ));
        assert_eq!(budget.observation().unwrap(), before);
    }
}

#[test]
fn admitted_expression_tags_constants_and_legacy_version_gates_remain_exact() {
    let field = FieldId::first();
    let entity = EntityTypeId::first();
    let mut nodes = vec![
        (
            ExpressionKind::Constant(riffdb_types::CanonicalValue::Bool(true)),
            ValueType::bool(),
        ),
        (ExpressionKind::InputField(field), ValueType::bool()),
        (ExpressionKind::ServiceValue(field), ValueType::bool()),
        (ExpressionKind::CollectionElement, ValueType::bool()),
        (
            ExpressionKind::CollectionElementField(field),
            ValueType::bool(),
        ),
        (
            ExpressionKind::CompleteBinding(BindingId::new(0)),
            ValueType::record(RecordTypeRef::Entity(entity)),
        ),
        (
            ExpressionKind::BoundField {
                binding: BindingId::new(0),
                field,
            },
            ValueType::bool(),
        ),
        (
            ExpressionKind::SchemaField {
                entity_type: entity,
                field,
            },
            ValueType::bool(),
        ),
        (ExpressionKind::SourceEventField(field), ValueType::bool()),
        (ExpressionKind::TransactionTime, ValueType::timestamp()),
        (ExpressionKind::TransactionDate, ValueType::date()),
        (
            ExpressionKind::Unary {
                operator: UnaryOperator::Not,
                operand: ExprId::new(0),
            },
            ValueType::bool(),
        ),
        (
            ExpressionKind::Binary {
                operator: BinaryOperator::And,
                left: ExprId::new(0),
                right: ExprId::new(1),
            },
            ValueType::bool(),
        ),
        (
            ExpressionKind::RootValidationField {
                read: crate::RootValidationReadId::new(0),
                field,
            },
            ValueType::bool(),
        ),
    ];
    let list = riffdb_types::CanonicalValue::list(vec![
        riffdb_types::CanonicalValue::string(
            "nested string"
        )
        .unwrap();
        65
    ])
    .unwrap();
    nodes.push((
        ExpressionKind::Constant(list),
        ValueType::list(ValueType::string(256).unwrap(), 65).unwrap(),
    ));
    let arena = ExpressionArena::new(nodes).unwrap();
    let bytes = arena_bytes(&arena);
    let budget = ProviderBudget::new();
    let bound =
        term_admission::expression_bound(&mut Reader::new(&bytes), EXECUTABLE_IR_VERSION_V26)
            .unwrap();
    let _work = budget.reserve(bound.maximum(bytes.len()).unwrap()).unwrap();
    let inventory = riffdb_types::OwnedCapacityInspectionV1::inspect(&arena).unwrap();
    assert!(inventory.unshared_bytes() <= bound.heap);
    assert!(inventory.shared_allocations().is_empty());
    let output =
        decode_provider_expression_arena_admitted_v1(&bytes, EXECUTABLE_IR_VERSION_V26, &budget)
            .unwrap();
    assert_eq!(**output, arena);
    for (kind, required) in [
        (
            ExpressionKind::ServiceValue(field),
            EXECUTABLE_IR_VERSION_V2,
        ),
        (ExpressionKind::CollectionElement, EXECUTABLE_IR_VERSION_V5),
        (
            ExpressionKind::CollectionElementField(field),
            EXECUTABLE_IR_VERSION_V5,
        ),
    ] {
        let arena = ExpressionArena::new(vec![(kind, ValueType::bool())]).unwrap();
        let bytes = arena_bytes(&arena);
        let ordinary =
            decode_expression_arena_versioned(&mut Reader::new(&bytes), required - 1).unwrap_err();
        assert_eq!(
            decode_provider_expression_arena_admitted_v1(&bytes, required - 1, &budget)
                .err()
                .unwrap(),
            ProviderBundleEncodingErrorV1::Invalid(ordinary)
        );
        assert_eq!(
            **decode_provider_expression_arena_admitted_v1(&bytes, required, &budget).unwrap(),
            arena
        );
    }
}
