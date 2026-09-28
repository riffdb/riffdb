//! Characterization of ADR-0256's binding-wide dependency, before ADR-0262.
//! This is a deterministic runtime schedule, not a commit-coordinator test.
// req: CMP-015
use super::*;

#[test]
fn revision_increment_alone_allows_serialized_payload_replacement() {
    let (bundle, first_input) = initialized_state_fixture();
    let plan = command(&bundle, "PutState");
    assert!(
        riffdb_contract_ir::CommandExplain::from_plan(plan)
            .unguarded_writes()
            .is_empty(),
        "the current compiler classifies the payload replacement as guarded"
    );
    let target = derive_binding_target(plan, &first_input, 0);
    let entity = bundle.schema().entities().first().expect("state entity");
    let initial_fields = input_record(
        entity.record(),
        [
            ("organization_id", CanonicalValue::Uuid([0x31; 16])),
            ("state_id", CanonicalValue::Uuid([0x32; 16])),
            ("active", CanonicalValue::Bool(true)),
            ("revision", CanonicalValue::U64(0)),
            (
                "payload",
                CanonicalValue::Bytes(CanonicalBytes::new(vec![0]).expect("payload")),
            ),
        ],
    );
    let before = snapshot(
        plan_ref(&bundle, plan),
        vec![EntityObservation::Present(stored_record(
            &bundle,
            plan,
            target.clone(),
            initial_fields,
        ))],
    );
    let now = LogicalTime::new(Timestamp::new(1, 0).expect("time"));
    let evaluate = |input: &CanonicalRecord, observed: &ReadSnapshot| {
        execute_command(
            &bundle,
            input,
            observed,
            &context(&bundle, plan, input, now),
            EvaluationBudget::v1(),
        )
        .expect("evaluation")
    };
    let ExecutionResult::CommitRequired(first) = evaluate(&first_input, &before) else {
        panic!("durable candidate")
    };
    assert_eq!(first.outcome().outcome_id(), outcome_id(plan, "Written"));
    let [EntityMutation::Replace { post_image, .. }] = first.mutations() else {
        panic!("one replacement")
    };
    assert_initialized_state(&bundle, post_image.fields(), 1);

    // Both callers prepared replacement values from the original state. The
    // second execution nevertheless sees the first post-image, so no read/write
    // race or missing transaction revalidation is needed for the lost edit.
    let second_payload = CanonicalValue::Bytes(CanonicalBytes::new(vec![0x55]).expect("payload"));
    let second_input = input_record(
        plan.input().record(),
        [
            ("request_id", CanonicalValue::Uuid([0x40; 16])),
            ("organization_id", CanonicalValue::Uuid([0x31; 16])),
            ("state_id", CanonicalValue::Uuid([0x32; 16])),
            ("payload", second_payload.clone()),
        ],
    );
    let after = snapshot(
        plan_ref(&bundle, plan),
        vec![EntityObservation::Present(stored_record_with_version(
            &bundle,
            plan,
            target,
            EntityVersion::new(2).expect("version"),
            post_image.fields().clone(),
        ))],
    );
    let ExecutionResult::CommitRequired(second) = evaluate(&second_input, &after) else {
        panic!("durable candidate")
    };
    assert_eq!(second.outcome().outcome_id(), outcome_id(plan, "Written"));
    let [EntityMutation::Replace { post_image, .. }] = second.mutations() else {
        panic!("one replacement")
    };
    assert_eq!(
        field(
            post_image.fields(),
            entity_field(&bundle, "State", "revision")
        ),
        &CanonicalValue::U64(2)
    );
    assert_eq!(
        field(
            post_image.fields(),
            entity_field(&bundle, "State", "payload")
        ),
        &second_payload,
        "the second transaction-current execution replaces the first caller's edit"
    );
}
