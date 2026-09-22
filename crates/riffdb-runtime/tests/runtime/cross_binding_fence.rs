//! Execution of the compiler-proven fence on two serialized callers.
// req: CMP-015
use super::*;

#[test]
fn stale_root_revision_refuses_child_update_and_overflow_never_wraps() {
    let source = include_str!("../../../../fixtures/contracts/cross-binding-fence.riff");
    for unsigned in [false, true] {
        let source = if unsigned {
            source.replace(": i64", ": u64")
        } else {
            source.to_owned()
        };
        let bundle = compile_contract_source(&source).expect("fenced contract");
        let plan = command(&bundle, "UpdateChild");
        let number = |n| {
            if unsigned {
                CanonicalValue::U64(n)
            } else {
                CanonicalValue::I64(i64::try_from(n).expect("signed value"))
            }
        };
        let input = |revision, value| {
            input_record(
                plan.input().record(),
                [
                    (
                        "request_key",
                        CanonicalValue::string("fence-request").expect("key"),
                    ),
                    ("tenant", CanonicalValue::Uuid([1; 16])),
                    ("parent_id", CanonicalValue::Uuid([2; 16])),
                    ("other_parent_id", CanonicalValue::Uuid([3; 16])),
                    ("child_id", CanonicalValue::Uuid([4; 16])),
                    ("expected_revision", number(revision)),
                    ("value", number(value)),
                ],
            )
        };
        let first_input = input(0, 10);
        let observations = plan
            .bindings()
            .iter()
            .enumerate()
            .map(|(index, binding)| {
                let target = derive_binding_target(plan, &first_input, index);
                let entity = bundle
                    .schema()
                    .entities()
                    .iter()
                    .find(|e| e.id() == binding.entity_type())
                    .expect("entity");
                let values = if entity.name() == "Parent" {
                    vec![
                        ("tenant", CanonicalValue::Uuid([1; 16])),
                        ("parent_id", CanonicalValue::Uuid([2; 16])),
                        ("revision", number(0)),
                    ]
                } else {
                    vec![
                        ("tenant", CanonicalValue::Uuid([1; 16])),
                        ("parent_id", CanonicalValue::Uuid([2; 16])),
                        ("child_id", CanonicalValue::Uuid([4; 16])),
                        ("value", number(0)),
                    ]
                };
                let fields = CanonicalRecord::new(
                    entity
                        .record()
                        .fields()
                        .iter()
                        .map(|f| {
                            (
                                f.id(),
                                values
                                    .iter()
                                    .find(|(name, _)| *name == f.name())
                                    .expect("field")
                                    .1
                                    .clone(),
                            )
                        })
                        .collect(),
                )
                .expect("fields");
                EntityObservation::Present(stored_record(&bundle, plan, target, fields))
            })
            .collect();
        let before = snapshot(plan_ref(&bundle, plan), observations);
        let now = LogicalTime::new(Timestamp::new(1, 0).expect("time"));
        let evaluate = |input: &CanonicalRecord, snapshot: &ReadSnapshot| {
            execute_command(
                &bundle,
                input,
                snapshot,
                &context(&bundle, plan, input, now),
                EvaluationBudget::v1(),
            )
        };
        let ExecutionResult::CommitRequired(first) =
            evaluate(&first_input, &before).expect("first call")
        else {
            panic!("durable")
        };
        assert_eq!(first.outcome().outcome_id(), outcome_id(plan, "Updated"));
        assert_eq!(first.mutations().len(), 2);
        for mutation in first.mutations() {
            let EntityMutation::Replace { post_image, .. } = mutation else {
                panic!("replace")
            };
            let entity = bundle
                .schema()
                .entities()
                .iter()
                .find(|entity| entity.id() == post_image.target().entity_type_id())
                .expect("entity");
            let (name, expected) = if entity.name() == "Parent" {
                ("revision", number(1))
            } else {
                ("value", number(10))
            };
            assert_eq!(
                field(
                    post_image.fields(),
                    entity_field(&bundle, entity.name(), name)
                ),
                &expected
            );
        }

        let after = snapshot(
            plan_ref(&bundle, plan),
            plan.bindings()
                .iter()
                .enumerate()
                .map(|(index, _)| {
                    let target = derive_binding_target(plan, &first_input, index);
                    let mutation = first
                        .mutations()
                        .iter()
                        .find(|mutation| mutation.target() == &target)
                        .expect("target mutation");
                    let EntityMutation::Replace { post_image, .. } = mutation else {
                        panic!("replace")
                    };
                    EntityObservation::Present(stored_record_with_version(
                        &bundle,
                        plan,
                        post_image.target().clone(),
                        EntityVersion::new(2).expect("version"),
                        post_image.fields().clone(),
                    ))
                })
                .collect(),
        );
        // This explicit schedule models a caller admitted after the first commit,
        // with the same expected revision and a different proposed child value.
        let ExecutionResult::CommitRequired(stale) =
            evaluate(&input(0, 20), &after).expect("declared stale")
        else {
            panic!("durable stale outcome")
        };
        assert_eq!(stale.outcome().outcome_id(), outcome_id(plan, "Stale"));
        assert!(stale.mutations().is_empty());
        let ExecutionResult::CommitRequired(next) =
            evaluate(&input(1, 20), &after).expect("fresh revision")
        else {
            panic!("durable")
        };
        assert_eq!(next.outcome().outcome_id(), outcome_id(plan, "Updated"));
        assert_eq!(next.mutations().len(), 2);

        let maximum = if unsigned { u64::MAX } else { i64::MAX as u64 };
        let overflow_snapshot = snapshot(
            plan_ref(&bundle, plan),
            plan.bindings()
                .iter()
                .enumerate()
                .map(|(index, _)| {
                    let target = derive_binding_target(plan, &first_input, index);
                    let mutation = first
                        .mutations()
                        .iter()
                        .find(|mutation| mutation.target() == &target)
                        .expect("target mutation");
                    let EntityMutation::Replace { post_image, .. } = mutation else {
                        panic!("replace")
                    };
                    let entity = bundle
                        .schema()
                        .entities()
                        .iter()
                        .find(|entity| entity.id() == post_image.target().entity_type_id())
                        .expect("entity");
                    let fields = if entity.name() == "Parent" {
                        let revision = entity_field(&bundle, "Parent", "revision");
                        CanonicalRecord::new(
                            post_image
                                .fields()
                                .fields()
                                .iter()
                                .map(|(id, value)| {
                                    (
                                        *id,
                                        if *id == revision {
                                            number(maximum)
                                        } else {
                                            value.clone()
                                        },
                                    )
                                })
                                .collect(),
                        )
                        .expect("max revision")
                    } else {
                        post_image.fields().clone()
                    };
                    EntityObservation::Present(stored_record(
                        &bundle,
                        plan,
                        post_image.target().clone(),
                        fields,
                    ))
                })
                .collect(),
        );
        assert!(
            matches!(
                evaluate(&input(maximum, 30), &overflow_snapshot),
                Err(ExecutionFault::Arithmetic)
            ),
            "overflow must not produce a mutation or wrap the root fence"
        );
    }
}
