//! Independent exact-predicate/order semantic and canonical-codec evidence.

use std::collections::BTreeMap;

use riffdb_query_ir::{
    ExactComparisonProfileV1, ExactOrderDirectionV1, ExactOrderProgramV1, ExactOrderTermV1,
    ExactParameterValueV1, ExactPredicateLeafV1, ExactPredicateNodeV1, ExactPredicateOperatorV1,
    ExactPredicateProgramErrorV1, ExactPredicateProgramV1, ExactProviderRequirementV1,
    ExactReferenceCellV1, ExactReferenceRowV1, ExactScalarV1, ExactValueSlotV1,
    MAX_EXACT_BOOLEAN_BRANCHES_V1, MAX_EXACT_FAMILY_MEMBERS_V1,
};
use riffdb_types::{FieldId, ProjectionProviderPolicyModeV1};

fn requirement() -> ExactProviderRequirementV1 {
    ExactProviderRequirementV1::new(
        ProjectionProviderPolicyModeV1::PartitionAligned,
        1_000,
        100_000,
        4_096,
    )
    .expect("bounded requirement")
}

fn field_id(value: u32) -> FieldId {
    FieldId::new(value).expect("test field")
}

fn leaf(
    field: u32,
    operator: ExactPredicateOperatorV1,
    profile: ExactComparisonProfileV1,
    value: Option<ExactValueSlotV1>,
) -> ExactPredicateNodeV1 {
    ExactPredicateNodeV1::Leaf(
        ExactPredicateLeafV1::new(field_id(field), operator, profile, value).expect("test leaf"),
    )
}

fn order() -> ExactOrderProgramV1 {
    ExactOrderProgramV1::new(vec![
        ExactOrderTermV1::new(
            field_id(4),
            ExactComparisonProfileV1::U64,
            ExactOrderDirectionV1::Descending,
            false,
        ),
        ExactOrderTermV1::new(
            field_id(1),
            ExactComparisonProfileV1::U64,
            ExactOrderDirectionV1::Ascending,
            true,
        ),
    ])
    .expect("complete order")
}

fn row(
    id: u64,
    email: &str,
    state: u64,
    score: u64,
    optional: ExactReferenceCellV1,
) -> ExactReferenceRowV1 {
    ExactReferenceRowV1 {
        entity_key: vec![ExactScalarV1::U64(id)],
        fields: BTreeMap::from([
            (
                field_id(1),
                ExactReferenceCellV1::Value(ExactScalarV1::U64(id)),
            ),
            (
                field_id(2),
                ExactReferenceCellV1::Value(ExactScalarV1::String(email.to_owned())),
            ),
            (
                field_id(3),
                ExactReferenceCellV1::Value(ExactScalarV1::U64(state)),
            ),
            (
                field_id(4),
                ExactReferenceCellV1::Value(ExactScalarV1::U64(score)),
            ),
            (field_id(5), optional),
        ]),
    }
}

#[test]
fn canonical_program_round_trips_and_enumerates_every_presence_order_member() {
    let program = ExactPredicateProgramV1::new(
        ExactPredicateNodeV1::And(vec![
            leaf(
                2,
                ExactPredicateOperatorV1::Contains,
                ExactComparisonProfileV1::BinaryUtf8,
                Some(ExactValueSlotV1::Scalar(0)),
            ),
            ExactPredicateNodeV1::When {
                presence_ordinal: 0,
                child: Box::new(leaf(
                    3,
                    ExactPredicateOperatorV1::NotIn,
                    ExactComparisonProfileV1::U64,
                    Some(ExactValueSlotV1::Set(1)),
                )),
            },
        ]),
        vec![order(), order()],
        1,
        true,
        1_000,
        100,
        requirement(),
    )
    .expect("bounded program");
    assert_eq!(program.members().len(), 4);
    assert_eq!(program.members()[0].presence_bits(), 0);
    assert_eq!(program.members()[3].presence_bits(), 1);
    assert_eq!(program.members()[3].order_ordinal(), 1);
    assert!(program.members().len() <= MAX_EXACT_FAMILY_MEMBERS_V1);

    let decoded = ExactPredicateProgramV1::from_canonical_bytes(program.canonical_bytes())
        .expect("canonical decode");
    assert_eq!(decoded, program);
    assert_eq!(decoded.identity(), program.identity());

    let mut corrupted = program.canonical_bytes().to_vec();
    corrupted.push(0);
    assert_eq!(
        ExactPredicateProgramV1::from_canonical_bytes(&corrupted),
        Err(ExactPredicateProgramErrorV1::InvalidEncoding)
    );
}

#[test]
fn missing_and_null_never_satisfy_value_or_complement_predicates() {
    for operator in [
        ExactPredicateOperatorV1::NotEqual,
        ExactPredicateOperatorV1::NotIn,
    ] {
        let slot = if operator == ExactPredicateOperatorV1::NotIn {
            ExactValueSlotV1::Set(0)
        } else {
            ExactValueSlotV1::Scalar(0)
        };
        let program = ExactPredicateProgramV1::new(
            leaf(5, operator, ExactComparisonProfileV1::U64, Some(slot)),
            vec![order()],
            0,
            true,
            10,
            10,
            requirement(),
        )
        .expect("complement program");
        let parameters = BTreeMap::from([(
            0,
            if operator == ExactPredicateOperatorV1::NotIn {
                ExactParameterValueV1::canonical_set(vec![ExactScalarV1::U64(7)]).expect("set")
            } else {
                ExactParameterValueV1::Scalar(ExactScalarV1::U64(7))
            },
        )]);
        let rows = [
            row(1, "alpha", 1, 1, ExactReferenceCellV1::Missing),
            row(2, "beta", 1, 1, ExactReferenceCellV1::Null),
            row(
                3,
                "gamma",
                1,
                1,
                ExactReferenceCellV1::Value(ExactScalarV1::U64(8)),
            ),
        ];
        let result = program
            .evaluate_reference(&rows, &parameters, program.members()[0], 0, 10)
            .expect("reference result");
        assert_eq!(result.total, Some(1));
        assert_eq!(result.entity_keys, vec![vec![ExactScalarV1::U64(3)]]);
    }
}

#[test]
fn canonical_set_dedup_range_boolean_text_and_independent_order_are_frozen() {
    let predicate = ExactPredicateNodeV1::And(vec![
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
        leaf(
            4,
            ExactPredicateOperatorV1::GreaterEqual,
            ExactComparisonProfileV1::U64,
            Some(ExactValueSlotV1::Scalar(2)),
        ),
        ExactPredicateNodeV1::Or(vec![
            leaf(
                5,
                ExactPredicateOperatorV1::IsNull,
                ExactComparisonProfileV1::U64,
                None,
            ),
            leaf(
                5,
                ExactPredicateOperatorV1::IsNotNull,
                ExactComparisonProfileV1::U64,
                None,
            ),
        ]),
    ]);
    let program =
        ExactPredicateProgramV1::new(predicate, vec![order()], 0, true, 10, 2, requirement())
            .expect("semantic program");
    let parameters = BTreeMap::from([
        (
            0,
            ExactParameterValueV1::Scalar(ExactScalarV1::String("example".to_owned())),
        ),
        (
            1,
            ExactParameterValueV1::canonical_set(vec![
                ExactScalarV1::U64(9),
                ExactScalarV1::U64(2),
                ExactScalarV1::U64(2),
            ])
            .expect("canonical set"),
        ),
        (2, ExactParameterValueV1::Scalar(ExactScalarV1::U64(50))),
    ]);
    assert_eq!(
        parameters.get(&1),
        Some(&ExactParameterValueV1::Set(vec![
            ExactScalarV1::U64(2),
            ExactScalarV1::U64(9)
        ]))
    );
    let rows = [
        row(1, "a@example.test", 1, 60, ExactReferenceCellV1::Null),
        row(
            2,
            "b@example.test",
            2,
            90,
            ExactReferenceCellV1::Value(ExactScalarV1::U64(1)),
        ),
        row(
            3,
            "c@example.test",
            3,
            70,
            ExactReferenceCellV1::Value(ExactScalarV1::U64(1)),
        ),
        row(4, "nomatch.test", 1, 100, ExactReferenceCellV1::Null),
    ];
    let result = program
        .evaluate_reference(&rows, &parameters, program.members()[0], 0, 2)
        .expect("reference result");
    assert_eq!(result.total, Some(2));
    assert_eq!(
        result.entity_keys,
        vec![vec![ExactScalarV1::U64(3)], vec![ExactScalarV1::U64(1)]]
    );
}

#[test]
fn boolean_and_family_bounds_fail_closed() {
    let too_wide = ExactPredicateNodeV1::Or(
        (0..=MAX_EXACT_BOOLEAN_BRANCHES_V1)
            .map(|ordinal| {
                leaf(
                    2,
                    ExactPredicateOperatorV1::Equal,
                    ExactComparisonProfileV1::U64,
                    Some(ExactValueSlotV1::Scalar(ordinal as u16)),
                )
            })
            .collect(),
    );
    assert_eq!(
        ExactPredicateProgramV1::new(too_wide, vec![order()], 0, false, 0, 1, requirement(),),
        Err(ExactPredicateProgramErrorV1::BoundExceeded)
    );
    let bounded = leaf(
        2,
        ExactPredicateOperatorV1::Equal,
        ExactComparisonProfileV1::U64,
        Some(ExactValueSlotV1::Scalar(0)),
    );
    assert_eq!(
        ExactPredicateProgramV1::new(bounded, vec![order()], 7, false, 0, 1, requirement(),),
        Err(ExactPredicateProgramErrorV1::BoundExceeded)
    );
}

#[test]
fn invalid_presence_set_types_and_empty_text_fail_closed() {
    let invalid_presence = ExactPredicateNodeV1::When {
        presence_ordinal: 1,
        child: Box::new(leaf(
            3,
            ExactPredicateOperatorV1::Equal,
            ExactComparisonProfileV1::U64,
            Some(ExactValueSlotV1::Scalar(0)),
        )),
    };
    assert_eq!(
        ExactPredicateProgramV1::new(
            invalid_presence,
            vec![order()],
            1,
            false,
            0,
            1,
            requirement(),
        ),
        Err(ExactPredicateProgramErrorV1::InvalidLeaf)
    );

    assert_eq!(
        ExactParameterValueV1::canonical_set(vec![
            ExactScalarV1::U64(1),
            ExactScalarV1::String("one".to_owned()),
        ]),
        Err(ExactPredicateProgramErrorV1::TypeMismatch)
    );

    let membership = ExactPredicateProgramV1::new(
        leaf(
            3,
            ExactPredicateOperatorV1::NotIn,
            ExactComparisonProfileV1::U64,
            Some(ExactValueSlotV1::Set(0)),
        ),
        vec![order()],
        0,
        false,
        0,
        1,
        requirement(),
    )
    .expect("membership program");
    let wrong_set = BTreeMap::from([(
        0,
        ExactParameterValueV1::canonical_set(vec![ExactScalarV1::String("1".to_owned())])
            .expect("homogeneous set"),
    )]);
    assert_eq!(
        membership.evaluate_reference(
            &[row(1, "value", 2, 1, ExactReferenceCellV1::Null)],
            &wrong_set,
            membership.members()[0],
            0,
            1,
        ),
        Err(ExactPredicateProgramErrorV1::TypeMismatch)
    );

    let text = ExactPredicateProgramV1::new(
        leaf(
            2,
            ExactPredicateOperatorV1::Contains,
            ExactComparisonProfileV1::BinaryUtf8,
            Some(ExactValueSlotV1::Scalar(0)),
        ),
        vec![order()],
        0,
        false,
        0,
        1,
        requirement(),
    )
    .expect("text program");
    let empty_text = BTreeMap::from([(
        0,
        ExactParameterValueV1::Scalar(ExactScalarV1::String(String::new())),
    )]);
    assert_eq!(
        text.evaluate_reference(
            &[row(1, "value", 2, 1, ExactReferenceCellV1::Null)],
            &empty_text,
            text.members()[0],
            0,
            1,
        ),
        Err(ExactPredicateProgramErrorV1::TypeMismatch)
    );
}

#[test]
fn randomized_history_matches_a_direct_two_valued_reference() {
    let program = ExactPredicateProgramV1::new(
        ExactPredicateNodeV1::And(vec![
            leaf(
                3,
                ExactPredicateOperatorV1::NotIn,
                ExactComparisonProfileV1::U64,
                Some(ExactValueSlotV1::Set(0)),
            ),
            leaf(
                4,
                ExactPredicateOperatorV1::Greater,
                ExactComparisonProfileV1::U64,
                Some(ExactValueSlotV1::Scalar(1)),
            ),
        ]),
        vec![order()],
        0,
        true,
        256,
        256,
        requirement(),
    )
    .expect("program");
    let parameters = BTreeMap::from([
        (
            0,
            ExactParameterValueV1::canonical_set(vec![
                ExactScalarV1::U64(1),
                ExactScalarV1::U64(3),
            ])
            .expect("set"),
        ),
        (1, ExactParameterValueV1::Scalar(ExactScalarV1::U64(20))),
    ]);
    let mut seed = 0x5eed_u64;
    let mut rows = Vec::new();
    let mut expected = Vec::new();
    for id in 1..=128_u64 {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let state = seed % 5;
        let score = (seed >> 8) % 100;
        rows.push(row(id, "value", state, score, ExactReferenceCellV1::Null));
        if state != 1 && state != 3 && score > 20 {
            expected.push((score, id));
        }
    }
    expected.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
    let result = program
        .evaluate_reference(&rows, &parameters, program.members()[0], 0, 256)
        .expect("reference result");
    assert_eq!(result.total, Some(expected.len() as u64));
    assert_eq!(
        result.entity_keys,
        expected
            .into_iter()
            .map(|(_, id)| vec![ExactScalarV1::U64(id)])
            .collect::<Vec<_>>()
    );
}
