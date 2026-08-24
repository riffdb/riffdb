//! ADR-0134 V4 indexed-set provider equivalence and lifecycle evidence.

use std::collections::BTreeMap;
use std::num::NonZeroU16;

use riffdb_projection::{
    ExactPredicateIndexMutationV1, ExactPredicatePartitionIndexV4, ExactPredicatePartitionIndexV5,
    ExactPredicateProviderBindingV1, ExactPredicateProviderBindingV2,
    ExactPredicateProviderErrorV1, ExactPredicateProviderRowV1,
};
use riffdb_query_ir::{
    ExactComparisonProfileV1, ExactOrderDirectionV1, ExactOrderProgramV1, ExactOrderProgramV2,
    ExactOrderTermV1, ExactOrderTermV2, ExactParameterValueV1, ExactPredicateLeafV1,
    ExactPredicateNodeV1, ExactPredicateOperatorV1, ExactPredicateProgramV1,
    ExactPredicateProgramV2, ExactProviderRequirementV1, ExactReferenceCellV1, ExactReferenceRowV1,
    ExactScalarV1, ExactStatePlacementV1, ExactValueSlotV1,
};
use riffdb_types::{
    ApplicationRoleHash, CanonicalRecord, CanonicalValue, CommitSequence, EntityKey,
    EntityKeyBuilder, EntityTypeId, FieldId, PartitionKeyHash, ProjectionGeneration,
    ProjectionProviderPolicyModeV1, QueryPlanHash,
};

fn field(value: u32) -> FieldId {
    FieldId::new(value).expect("field")
}

fn key(value: u32) -> EntityKey {
    let mut builder = EntityKeyBuilder::new(EntityTypeId::first());
    builder.push_u32(value).expect("key component");
    builder.finish().expect("key")
}

fn sequence(value: u64) -> CommitSequence {
    CommitSequence::new(value).expect("sequence")
}

fn leaf(
    field_id: u32,
    operator: ExactPredicateOperatorV1,
    profile: ExactComparisonProfileV1,
    value: Option<ExactValueSlotV1>,
) -> ExactPredicateNodeV1 {
    ExactPredicateNodeV1::Leaf(
        ExactPredicateLeafV1::new(field(field_id), operator, profile, value).expect("leaf"),
    )
}

fn program() -> ExactPredicateProgramV1 {
    ExactPredicateProgramV1::new(
        ExactPredicateNodeV1::And(vec![
            leaf(
                2,
                ExactPredicateOperatorV1::Contains,
                ExactComparisonProfileV1::BinaryUtf8,
                Some(ExactValueSlotV1::Scalar(0)),
            ),
            leaf(
                3,
                ExactPredicateOperatorV1::NotIn,
                ExactComparisonProfileV1::U64,
                Some(ExactValueSlotV1::Set(1)),
            ),
            ExactPredicateNodeV1::When {
                presence_ordinal: 0,
                child: Box::new(leaf(
                    4,
                    ExactPredicateOperatorV1::GreaterEqual,
                    ExactComparisonProfileV1::U64,
                    Some(ExactValueSlotV1::Scalar(2)),
                )),
            },
        ]),
        vec![
            ExactOrderProgramV1::new(vec![
                ExactOrderTermV1::new(
                    field(4),
                    ExactComparisonProfileV1::U64,
                    ExactOrderDirectionV1::Descending,
                    false,
                ),
                ExactOrderTermV1::new(
                    field(1),
                    ExactComparisonProfileV1::U64,
                    ExactOrderDirectionV1::Ascending,
                    true,
                ),
            ])
            .expect("order"),
        ],
        1,
        true,
        4_096,
        499,
        ExactProviderRequirementV1::new(
            ProjectionProviderPolicyModeV1::PartitionAligned,
            4_096,
            16_000_000,
            65_536,
        )
        .expect("requirement"),
    )
    .expect("program")
}

fn binding(program: &ExactPredicateProgramV1, frontier: u64) -> ExactPredicateProviderBindingV1 {
    ExactPredicateProviderBindingV1::new(
        QueryPlanHash::from_bytes([0x11; 32]),
        program,
        ApplicationRoleHash::from_bytes([0x22; 32]),
        PartitionKeyHash::from_bytes([0x33; 32]),
        7,
        ProjectionGeneration::new(3).expect("generation"),
        sequence(frontier),
    )
    .expect("binding")
}

fn row(id: u32, email: String, state: u64, score: u64) -> ExactPredicateProviderRowV1 {
    ExactPredicateProviderRowV1::new(
        key(id),
        BTreeMap::from([
            (
                field(1),
                ExactReferenceCellV1::Value(ExactScalarV1::U64(u64::from(id))),
            ),
            (
                field(2),
                ExactReferenceCellV1::Value(ExactScalarV1::String(email)),
            ),
            (
                field(3),
                ExactReferenceCellV1::Value(ExactScalarV1::U64(state)),
            ),
            (
                field(4),
                ExactReferenceCellV1::Value(ExactScalarV1::U64(score)),
            ),
        ]),
        CanonicalRecord::new(vec![(field(1), CanonicalValue::U64(u64::from(id)))]).expect("output"),
    )
}

fn reference_row(row: &ExactPredicateProviderRowV1) -> ExactReferenceRowV1 {
    ExactReferenceRowV1 {
        entity_key: vec![match row.fields().get(&field(1)).expect("id") {
            ExactReferenceCellV1::Value(value) => value.clone(),
            _ => panic!("fixture id is present"),
        }],
        fields: row.fields().clone(),
    }
}

fn parameters() -> BTreeMap<u16, ExactParameterValueV1> {
    BTreeMap::from([
        (
            0,
            ExactParameterValueV1::Scalar(ExactScalarV1::String("example".to_owned())),
        ),
        (
            1,
            ExactParameterValueV1::canonical_set(vec![
                ExactScalarV1::U64(1),
                ExactScalarV1::U64(3),
            ])
            .expect("set"),
        ),
        (2, ExactParameterValueV1::Scalar(ExactScalarV1::U64(50))),
    ])
}

fn single_program(predicate: ExactPredicateNodeV1) -> ExactPredicateProgramV1 {
    ExactPredicateProgramV1::new(
        predicate,
        vec![
            ExactOrderProgramV1::new(vec![ExactOrderTermV1::new(
                field(1),
                ExactComparisonProfileV1::U64,
                ExactOrderDirectionV1::Ascending,
                true,
            )])
            .expect("key order"),
        ],
        0,
        true,
        32,
        32,
        ExactProviderRequirementV1::new(
            ProjectionProviderPolicyModeV1::PartitionAligned,
            32,
            1_000_000,
            16_384,
        )
        .expect("requirement"),
    )
    .expect("single program")
}

fn scalar_row(id: u32, cell: ExactReferenceCellV1) -> ExactPredicateProviderRowV1 {
    ExactPredicateProviderRowV1::new(
        key(id),
        BTreeMap::from([
            (
                field(1),
                ExactReferenceCellV1::Value(ExactScalarV1::U64(u64::from(id))),
            ),
            (field(2), cell),
        ]),
        CanonicalRecord::new(vec![(field(1), CanonicalValue::U64(u64::from(id)))]).expect("output"),
    )
}

fn nullable_program(
    profile: ExactComparisonProfileV1,
    direction: ExactOrderDirectionV1,
    placement: ExactStatePlacementV1,
) -> ExactPredicateProgramV2 {
    ExactPredicateProgramV2::new(
        leaf(
            1,
            ExactPredicateOperatorV1::Exists,
            ExactComparisonProfileV1::U64,
            None,
        ),
        vec![
            ExactOrderProgramV2::new(vec![
                ExactOrderTermV2::new(field(2), profile, direction, placement, false),
                ExactOrderTermV2::new(
                    field(1),
                    ExactComparisonProfileV1::U64,
                    ExactOrderDirectionV1::Ascending,
                    ExactStatePlacementV1::PresentOnlyV1,
                    true,
                ),
            ])
            .expect("nullable order"),
        ],
        0,
        true,
        32,
        32,
        ExactProviderRequirementV1::new(
            ProjectionProviderPolicyModeV1::PartitionAligned,
            32,
            1_000_000,
            16_384,
        )
        .expect("requirement"),
    )
    .expect("nullable program")
}

fn nullable_binding(
    program: &ExactPredicateProgramV2,
    frontier: u64,
) -> ExactPredicateProviderBindingV2 {
    ExactPredicateProviderBindingV2::new(
        QueryPlanHash::from_bytes([0x41; 32]),
        program,
        ApplicationRoleHash::from_bytes([0x42; 32]),
        PartitionKeyHash::from_bytes([0x43; 32]),
        9,
        ProjectionGeneration::new(4).expect("generation"),
        sequence(frontier),
    )
    .expect("binding")
}

fn assert_nullable_page_equals_reference(
    provider: &ExactPredicatePartitionIndexV5,
    references: &[ExactReferenceRowV1],
    offset: u32,
    limit: u16,
) {
    let program = provider.program();
    let expected = program
        .evaluate_reference(
            references,
            &BTreeMap::new(),
            program.members()[0],
            offset,
            limit,
        )
        .expect("reference");
    let actual = provider
        .result_page(
            &BTreeMap::new(),
            program.members()[0],
            offset,
            NonZeroU16::new(limit).expect("limit"),
        )
        .expect("provider result");
    assert_eq!(actual.exact_total(), expected.total.expect("total"));
    assert_eq!(
        actual
            .rows()
            .iter()
            .map(|row| row.key().clone())
            .collect::<Vec<_>>(),
        expected
            .entity_keys
            .iter()
            .map(|values| match values.as_slice() {
                [ExactScalarV1::U64(value)] => key(u32::try_from(*value).expect("id")),
                _ => panic!("fixture key"),
            })
            .collect::<Vec<_>>()
    );
}

fn assert_provider_equals_reference(
    program: ExactPredicateProgramV1,
    rows: Vec<ExactPredicateProviderRowV1>,
    parameters: BTreeMap<u16, ExactParameterValueV1>,
) {
    let references = rows.iter().map(reference_row).collect::<Vec<_>>();
    let provider =
        ExactPredicatePartitionIndexV4::rebuild(binding(&program, 1), program.clone(), rows)
            .expect("provider");
    let expected = program
        .evaluate_reference(&references, &parameters, program.members()[0], 0, 32)
        .expect("reference");
    let actual = provider
        .result_page(
            &parameters,
            program.members()[0],
            0,
            NonZeroU16::new(32).expect("limit"),
        )
        .expect("provider result");
    assert_eq!(actual.exact_total(), expected.total.expect("total"));
    assert_eq!(
        actual
            .rows()
            .iter()
            .map(|row| row.key().clone())
            .collect::<Vec<_>>(),
        expected
            .entity_keys
            .iter()
            .map(|values| match values.as_slice() {
                [ExactScalarV1::U64(value)] => key(u32::try_from(*value).expect("fixture id")),
                _ => panic!("fixture key"),
            })
            .collect::<Vec<_>>()
    );
}

#[test]
fn indexed_provider_matches_independent_reference_for_randomized_windows() {
    let program = program();
    let mut seed = 0x51c2_c003_u64;
    let mut rows = Vec::new();
    for id in 1..=256 {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let email = if id % 3 == 0 {
            format!("user{id}@example.test")
        } else {
            format!("user{id}@other.test")
        };
        rows.push(row(id, email, seed % 5, (seed >> 8) % 100));
    }
    let reference = rows.iter().map(reference_row).collect::<Vec<_>>();
    let provider =
        ExactPredicatePartitionIndexV4::rebuild(binding(&program, 9), program.clone(), rows)
            .expect("provider");
    let parameters = parameters();
    let member = program.members()[1];
    for (offset, limit) in [(0, 1), (0, 25), (7, 31), (255, 10), (4_096, 5)] {
        let expected = program
            .evaluate_reference(&reference, &parameters, member, offset, limit)
            .expect("reference");
        let actual = provider
            .result_page(
                &parameters,
                member,
                offset,
                NonZeroU16::new(limit).expect("limit"),
            )
            .expect("indexed result");
        assert_eq!(actual.exact_total(), expected.total.expect("count"));
        let actual_ids = actual
            .rows()
            .iter()
            .map(|row| match row.fields().get(&field(1)).expect("id") {
                ExactReferenceCellV1::Value(value) => vec![value.clone()],
                _ => panic!("fixture id is present"),
            })
            .collect::<Vec<_>>();
        assert_eq!(actual_ids, expected.entity_keys);
    }
}

#[test]
fn every_activated_operator_and_presence_state_matches_the_reference_model() {
    let numeric_rows = || {
        vec![
            scalar_row(1, ExactReferenceCellV1::Missing),
            scalar_row(2, ExactReferenceCellV1::Null),
            scalar_row(3, ExactReferenceCellV1::Value(ExactScalarV1::U64(10))),
            scalar_row(4, ExactReferenceCellV1::Value(ExactScalarV1::U64(20))),
            scalar_row(5, ExactReferenceCellV1::Value(ExactScalarV1::U64(30))),
        ]
    };
    for operator in [
        ExactPredicateOperatorV1::Equal,
        ExactPredicateOperatorV1::NotEqual,
        ExactPredicateOperatorV1::Less,
        ExactPredicateOperatorV1::LessEqual,
        ExactPredicateOperatorV1::Greater,
        ExactPredicateOperatorV1::GreaterEqual,
    ] {
        assert_provider_equals_reference(
            single_program(leaf(
                2,
                operator,
                ExactComparisonProfileV1::U64,
                Some(ExactValueSlotV1::Scalar(0)),
            )),
            numeric_rows(),
            BTreeMap::from([(0, ExactParameterValueV1::Scalar(ExactScalarV1::U64(20)))]),
        );
    }
    for operator in [
        ExactPredicateOperatorV1::In,
        ExactPredicateOperatorV1::NotIn,
    ] {
        assert_provider_equals_reference(
            single_program(leaf(
                2,
                operator,
                ExactComparisonProfileV1::U64,
                Some(ExactValueSlotV1::Set(0)),
            )),
            numeric_rows(),
            BTreeMap::from([(
                0,
                ExactParameterValueV1::canonical_set(vec![
                    ExactScalarV1::U64(10),
                    ExactScalarV1::U64(30),
                ])
                .expect("set"),
            )]),
        );
    }
    for operator in [
        ExactPredicateOperatorV1::IsNull,
        ExactPredicateOperatorV1::IsNotNull,
        ExactPredicateOperatorV1::Exists,
    ] {
        assert_provider_equals_reference(
            single_program(leaf(2, operator, ExactComparisonProfileV1::U64, None)),
            numeric_rows(),
            BTreeMap::new(),
        );
    }

    let text_rows = || {
        vec![
            scalar_row(
                1,
                ExactReferenceCellV1::Value(ExactScalarV1::String("alpha".to_owned())),
            ),
            scalar_row(
                2,
                ExactReferenceCellV1::Value(ExactScalarV1::String("alphabet".to_owned())),
            ),
            scalar_row(
                3,
                ExactReferenceCellV1::Value(ExactScalarV1::String("omega-alpha".to_owned())),
            ),
        ]
    };
    for (operator, needle) in [
        (ExactPredicateOperatorV1::StartsWith, "alpha"),
        (ExactPredicateOperatorV1::EndsWith, "alpha"),
        (ExactPredicateOperatorV1::Contains, "pha"),
    ] {
        assert_provider_equals_reference(
            single_program(leaf(
                2,
                operator,
                ExactComparisonProfileV1::BinaryUtf8,
                Some(ExactValueSlotV1::Scalar(0)),
            )),
            text_rows(),
            BTreeMap::from([(
                0,
                ExactParameterValueV1::Scalar(ExactScalarV1::String(needle.to_owned())),
            )]),
        );
    }

    assert_provider_equals_reference(
        single_program(ExactPredicateNodeV1::Or(vec![
            leaf(
                2,
                ExactPredicateOperatorV1::Equal,
                ExactComparisonProfileV1::U64,
                Some(ExactValueSlotV1::Scalar(0)),
            ),
            leaf(
                2,
                ExactPredicateOperatorV1::Equal,
                ExactComparisonProfileV1::U64,
                Some(ExactValueSlotV1::Scalar(1)),
            ),
        ])),
        numeric_rows(),
        BTreeMap::from([
            (0, ExactParameterValueV1::Scalar(ExactScalarV1::U64(10))),
            (1, ExactParameterValueV1::Scalar(ExactScalarV1::U64(30))),
        ]),
    );

    for scalar in [
        ExactScalarV1::Bool(true),
        ExactScalarV1::I64(-7),
        ExactScalarV1::U64(7),
        ExactScalarV1::Decimal {
            coefficient: 701,
            scale: 2,
        },
        ExactScalarV1::Money {
            currency: *b"USD",
            coefficient: 701,
            scale: 2,
        },
        ExactScalarV1::String("éclair".to_owned()),
        ExactScalarV1::Bytes(vec![0, 1, 255]),
        ExactScalarV1::Timestamp(-1),
        ExactScalarV1::Date(-10),
        ExactScalarV1::Uuid([0x44; 16]),
        ExactScalarV1::Enum {
            type_id: 3,
            variant_id: 9,
        },
    ] {
        let profile = scalar.profile();
        assert_provider_equals_reference(
            single_program(leaf(
                2,
                ExactPredicateOperatorV1::Equal,
                profile,
                Some(ExactValueSlotV1::Scalar(0)),
            )),
            vec![scalar_row(1, ExactReferenceCellV1::Value(scalar.clone()))],
            BTreeMap::from([(0, ExactParameterValueV1::Scalar(scalar))]),
        );
    }
}

#[test]
fn update_delete_checkpoint_restart_and_corruption_fail_closed() {
    let program = program();
    let mut provider = ExactPredicatePartitionIndexV4::rebuild(
        binding(&program, 1),
        program.clone(),
        vec![
            row(1, "first@example.test".to_owned(), 0, 70),
            row(2, "second@example.test".to_owned(), 2, 60),
        ],
    )
    .expect("provider");
    provider
        .apply(
            sequence(2),
            &[
                ExactPredicateIndexMutationV1::Delete(key(1)),
                ExactPredicateIndexMutationV1::Upsert(row(
                    2,
                    "updated@example.test".to_owned(),
                    2,
                    90,
                )),
            ],
        )
        .expect("atomic epoch");
    assert_eq!(provider.binding().frontier(), sequence(2));
    let page = provider
        .result_page(
            &parameters(),
            program.members()[1],
            0,
            NonZeroU16::new(10).expect("limit"),
        )
        .expect("page");
    assert_eq!(page.exact_total(), 1);
    assert_eq!(page.rows()[0].key(), &key(2));

    let bytes = provider.to_checkpoint_bytes().expect("checkpoint");
    let before_compaction = bytes.clone();
    provider.compact().expect("derived compaction");
    assert_eq!(
        provider
            .to_checkpoint_bytes()
            .expect("compacted checkpoint"),
        before_compaction,
        "compaction cannot change logical state or durable identity"
    );
    let recovered =
        ExactPredicatePartitionIndexV4::from_checkpoint_bytes(&bytes).expect("strict recovery");
    assert_eq!(recovered, provider);

    let mut corrupt = bytes.clone();
    let corrupt_offset = corrupt.len() / 2;
    corrupt[corrupt_offset] ^= 1;
    assert_eq!(
        ExactPredicatePartitionIndexV4::from_checkpoint_bytes(&corrupt),
        Err(ExactPredicateProviderErrorV1::Integrity)
    );
    assert_eq!(
        ExactPredicatePartitionIndexV4::from_checkpoint_bytes(&bytes[..bytes.len() - 1]),
        Err(ExactPredicateProviderErrorV1::Integrity)
    );
    let mut wrong_version = bytes;
    wrong_version[5] = 3;
    assert_eq!(
        ExactPredicatePartitionIndexV4::from_checkpoint_bytes(&wrong_version),
        Err(ExactPredicateProviderErrorV1::UnsupportedFormat)
    );
}

#[test]
fn invalid_order_state_duplicate_epoch_and_window_preserve_prior_state() {
    let program = program();
    let valid = row(1, "one@example.test".to_owned(), 0, 10);
    let mut invalid_fields = valid.fields().clone();
    invalid_fields.insert(field(4), ExactReferenceCellV1::Null);
    let invalid = ExactPredicateProviderRowV1::new(
        valid.key().clone(),
        invalid_fields,
        valid.output().clone(),
    );
    assert_eq!(
        ExactPredicatePartitionIndexV4::rebuild(
            binding(&program, 1),
            program.clone(),
            vec![invalid]
        ),
        Err(ExactPredicateProviderErrorV1::OrderStateInvalid)
    );

    let mut provider = ExactPredicatePartitionIndexV4::rebuild(
        binding(&program, 1),
        program.clone(),
        vec![row(1, "one@example.test".to_owned(), 0, 10)],
    )
    .expect("provider");
    let prior = provider.clone();
    assert_eq!(
        provider.apply(
            sequence(2),
            &[
                ExactPredicateIndexMutationV1::Delete(key(1)),
                ExactPredicateIndexMutationV1::Delete(key(1)),
            ],
        ),
        Err(ExactPredicateProviderErrorV1::DuplicateRow)
    );
    assert_eq!(provider, prior);
    assert_eq!(
        provider.result_page(
            &parameters(),
            program.members()[1],
            4_097,
            NonZeroU16::new(1).expect("limit"),
        ),
        Err(ExactPredicateProviderErrorV1::WindowInvalid)
    );

    let mut extra_fields = row(2, "two@example.test".to_owned(), 0, 10);
    let mut fields = extra_fields.fields().clone();
    fields.insert(
        field(99),
        ExactReferenceCellV1::Value(ExactScalarV1::U64(99)),
    );
    extra_fields = ExactPredicateProviderRowV1::new(
        extra_fields.key().clone(),
        fields,
        extra_fields.output().clone(),
    );
    assert_eq!(
        ExactPredicatePartitionIndexV4::rebuild(binding(&program, 1), program, vec![extra_fields]),
        Err(ExactPredicateProviderErrorV1::Integrity),
        "undeclared fields cannot inflate or alter provider state"
    );
}

#[test]
fn nullable_v5_provider_matches_reference_for_both_state_placements() {
    for (profile, values) in [
        (
            ExactComparisonProfileV1::U64,
            [ExactScalarV1::U64(20), ExactScalarV1::U64(10)],
        ),
        (
            ExactComparisonProfileV1::BinaryUtf8,
            [
                ExactScalarV1::String("zulu".to_owned()),
                ExactScalarV1::String("alpha".to_owned()),
            ],
        ),
        (
            ExactComparisonProfileV1::Timestamp,
            [ExactScalarV1::Timestamp(20), ExactScalarV1::Timestamp(-10)],
        ),
    ] {
        let rows = vec![
            scalar_row(1, ExactReferenceCellV1::Missing),
            scalar_row(2, ExactReferenceCellV1::Null),
            scalar_row(3, ExactReferenceCellV1::Value(values[0].clone())),
            scalar_row(4, ExactReferenceCellV1::Value(values[1].clone())),
        ];
        let references = rows.iter().map(reference_row).collect::<Vec<_>>();
        for placement in [
            ExactStatePlacementV1::NullsFirstV1,
            ExactStatePlacementV1::NullsLastV1,
        ] {
            for direction in [
                ExactOrderDirectionV1::Ascending,
                ExactOrderDirectionV1::Descending,
            ] {
                let program = nullable_program(profile, direction, placement);
                let binding = nullable_binding(&program, 11);
                let provider =
                    ExactPredicatePartitionIndexV5::rebuild(binding, program.clone(), rows.clone())
                        .expect("V5 provider");
                for (offset, limit) in [(0, 32), (1, 2), (4, 1), (5, 3), (32, 1)] {
                    assert_nullable_page_equals_reference(&provider, &references, offset, limit);
                }
                let bytes = provider.to_checkpoint_bytes().expect("V5 checkpoint");
                assert_eq!(
                    ExactPredicatePartitionIndexV5::from_checkpoint_bytes(&bytes)
                        .expect("V5 recovery"),
                    provider
                );
                assert_eq!(
                    ExactPredicatePartitionIndexV4::from_checkpoint_bytes(&bytes),
                    Err(ExactPredicateProviderErrorV1::UnsupportedFormat),
                    "V4 cannot reinterpret V5 state"
                );
                let mut corrupt = bytes;
                let middle = corrupt.len() / 2;
                corrupt[middle] ^= 1;
                assert_eq!(
                    ExactPredicatePartitionIndexV5::from_checkpoint_bytes(&corrupt),
                    Err(ExactPredicateProviderErrorV1::Integrity)
                );
            }
        }
    }
}

#[test]
fn nullable_v5_state_transitions_are_atomic_and_survive_compaction() {
    let program = nullable_program(
        ExactComparisonProfileV1::BinaryUtf8,
        ExactOrderDirectionV1::Ascending,
        ExactStatePlacementV1::NullsFirstV1,
    );
    let mut provider = ExactPredicatePartitionIndexV5::rebuild(
        nullable_binding(&program, 20),
        program,
        vec![
            scalar_row(1, ExactReferenceCellV1::Missing),
            scalar_row(2, ExactReferenceCellV1::Null),
            scalar_row(
                3,
                ExactReferenceCellV1::Value(ExactScalarV1::String("middle".to_owned())),
            ),
        ],
    )
    .expect("provider");
    let prior = provider.clone();
    assert_eq!(
        provider.apply(sequence(20), &[]),
        Err(ExactPredicateProviderErrorV1::NonAdvancingEpoch)
    );
    assert_eq!(provider, prior);
    provider
        .apply(
            sequence(21),
            &[
                ExactPredicateIndexMutationV1::Upsert(scalar_row(
                    1,
                    ExactReferenceCellV1::Value(ExactScalarV1::String("zulu".to_owned())),
                )),
                ExactPredicateIndexMutationV1::Upsert(scalar_row(2, ExactReferenceCellV1::Missing)),
                ExactPredicateIndexMutationV1::Delete(key(3)),
                ExactPredicateIndexMutationV1::Upsert(scalar_row(4, ExactReferenceCellV1::Null)),
            ],
        )
        .expect("atomic transition epoch");
    let references = [
        scalar_row(
            1,
            ExactReferenceCellV1::Value(ExactScalarV1::String("zulu".to_owned())),
        ),
        scalar_row(2, ExactReferenceCellV1::Missing),
        scalar_row(4, ExactReferenceCellV1::Null),
    ]
    .iter()
    .map(reference_row)
    .collect::<Vec<_>>();
    assert_nullable_page_equals_reference(&provider, &references, 0, 32);
    let checkpoint = provider.to_checkpoint_bytes().expect("checkpoint");
    provider.compact().expect("compaction");
    assert_eq!(
        provider.to_checkpoint_bytes().expect("checkpoint"),
        checkpoint,
        "derived compaction cannot alter nullable order identity or bytes"
    );
}
