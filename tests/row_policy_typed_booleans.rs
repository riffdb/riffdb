//! Regression coverage for ADR-0233's exact typed Boolean semantics.
use super::*;

fn boolean_bundle(expression: &str) -> ContractBundle {
    compile_contract_source(&format!(
        r#"contract BooleanPolicy version 1 {{
          principal fact enabled: bool
          principal fact choices: list<bool, 2>
          entity Document {{
            key (organization_id: uuid, document_id: uuid)
            field published: bool
          }}
          aggregate Documents {{
            root Document
            partition_by organization_id
            conflict_key (organization_id, document_id)
          }}
          row policy DocumentAccess on Document {{
            allow read when {expression}
            allow create when {expression}
            allow update when {expression}
            allow delete when {expression}
          }}
        }}"#,
    ))
    .expect("typed Boolean contract")
}

fn boolean_principal(value: CanonicalValue) -> PrincipalFactBindingV1 {
    PrincipalFactBindingV1::new(
        CapabilityId::from_unix_milliseconds_and_random(1, [0x40; 10]).unwrap(),
        NonZeroU64::MIN,
        DatabaseId::from_unix_milliseconds_and_random(1, [0x41; 10]).unwrap(),
        Environment::new("test").unwrap(),
        ActorId::new(uuid_text(OWNER)).unwrap(),
        ActorKind::Human,
        vec![Audience::new("riffdb-row-policy-test").unwrap()],
        TenantScope::Global,
        Timestamp::new(1, 0).unwrap(),
        Timestamp::new(10, 0).unwrap(),
        CapabilityPrincipalFactsV1::new(vec![
            CapabilityPrincipalFactV1::new(
                "choices",
                CanonicalValue::list(vec![CanonicalValue::Bool(true)]).unwrap(),
            )
            .unwrap(),
            CapabilityPrincipalFactV1::new("enabled", value).unwrap(),
        ])
        .unwrap(),
    )
    .unwrap()
}

// req: RAP-001, RAP-002, RAP-009
#[test]
fn typed_boolean_truth_tables_preserve_all_operation_classes() {
    for left in [false, true] {
        for right in [false, true] {
            for operand in [
                left.to_string(),
                "published".into(),
                "principal.fact.enabled".into(),
            ] {
                for (expression, expected) in [
                    (operand.clone(), left),
                    (format!("!({operand})"), !left),
                    (format!("{operand} && {right}"), left && right),
                    (format!("{operand} || {right}"), left || right),
                    (format!("{operand} == {right}"), left == right),
                    (format!("{operand} != {right}"), left != right),
                    (format!("{operand} in principal.fact.choices"), left),
                ] {
                    let bundle = boolean_bundle(&expression);
                    let row = relationship_document(&bundle, left);
                    let principal = boolean_principal(CanonicalValue::Bool(left));
                    for operation in [
                        RowPolicyOperationV1::Read,
                        RowPolicyOperationV1::Create,
                        RowPolicyOperationV1::Delete,
                        RowPolicyOperationV1::Update,
                    ] {
                        assert_eq!(
                            evaluate_row_policy(policy(&bundle), operation, &row, &principal, &[])
                                .is_allowed(),
                            expected,
                            "{expression}, {operation:?}"
                        );
                    }
                }
            }
        }
    }
}

// req: RAP-001, RAP-002, RAP-009
#[test]
fn direct_boolean_updates_require_both_rows() {
    let bundle = boolean_bundle("published");
    let principal = boolean_principal(CanonicalValue::Bool(true));
    for current in [false, true] {
        for successor in [false, true] {
            assert_eq!(
                evaluate_row_transition(
                    policy(&bundle),
                    RowPolicyOperationV1::Update,
                    Some(&relationship_document(&bundle, current)),
                    Some(&relationship_document(&bundle, successor)),
                    &principal,
                    &[]
                )
                .is_allowed(),
                current && successor
            );
        }
    }
}

// req: RAP-001, RAP-002, RAP-009
#[test]
fn direct_booleans_never_coerce_missing_or_mistyped_inputs() {
    let bundle = boolean_bundle("true || principal.fact.enabled");
    let row = relationship_document(&bundle, true);
    for principal in [
        principal_without_facts(OWNER),
        boolean_principal(CanonicalValue::U64(1)),
    ] {
        assert!(
            evaluate_row_policy(
                policy(&bundle),
                RowPolicyOperationV1::Read,
                &row,
                &principal,
                &[]
            )
            .is_denied()
        );
    }
    let bundle = boolean_bundle("true || published");
    let row = relationship_document(&bundle, true);
    let field = bundle.schema().entities()[0]
        .record()
        .fields()
        .iter()
        .find(|field| field.name() == "published")
        .unwrap()
        .id();
    for replacement in [
        None,
        Some(CanonicalValue::U64(1)),
        Some(CanonicalValue::Null),
    ] {
        let fields = row
            .fields()
            .iter()
            .filter_map(|(id, value)| {
                if *id == field {
                    replacement.clone().map(|value| (*id, value))
                } else {
                    Some((*id, value.clone()))
                }
            })
            .collect();
        let malformed = CanonicalRecord::new(fields).unwrap();
        assert!(
            evaluate_row_policy(
                policy(&bundle),
                RowPolicyOperationV1::Read,
                &malformed,
                &boolean_principal(CanonicalValue::Bool(true)),
                &[]
            )
            .is_denied()
        );
    }
    let bundle =
        compile_contract_source(&RELATIONSHIP_SOURCE.replace("published == true", "true")).unwrap();
    assert!(
        evaluate_row_policy(
            policy(&bundle),
            RowPolicyOperationV1::Read,
            &relationship_document(&bundle, true),
            &principal_without_facts(OWNER),
            &[]
        )
        .is_denied()
    );
}
