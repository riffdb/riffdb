//! Finite exact-text plan-family compilation and diagnostic spans.

use riffdb_query_compiler::{ExactTextCompilerDeclarationV1, compile_exact_text_family_v1};
use riffdb_riffql_syntax::Span;
use riffdb_types::{AggregateSemanticIdentityV1, FieldId, ProjectionProviderPolicyModeV1};

#[test]
fn compiler_enumerates_only_the_declared_finite_operator_family() {
    let declaration = ExactTextCompilerDeclarationV1::bounded_binary_utf8(
        FieldId::new(7).unwrap(),
        [true, true, false, true],
        ProjectionProviderPolicyModeV1::PartitionAligned,
        Span { start: 20, end: 90 },
    );
    let family = compile_exact_text_family_v1(declaration).unwrap();
    assert_eq!(family.members().len(), 6);
    assert_ne!(family.members()[0].order(), family.members()[1].order());
    assert_eq!(family.source_span(), Span { start: 20, end: 90 });
    let semantics = family.descriptor().aggregate_semantics();
    assert_eq!(semantics.len(), 1);
    assert!(semantics.contains(AggregateSemanticIdentityV1::ExactCount));
    assert!(!semantics.contains(AggregateSemanticIdentityV1::Count));
    assert!(!semantics.contains(AggregateSemanticIdentityV1::Mean));
}

#[test]
fn unsupported_or_excessive_forms_fail_at_the_declaration_span() {
    let mut declaration = ExactTextCompilerDeclarationV1::bounded_binary_utf8(
        FieldId::new(7).unwrap(),
        [true, true, true, true],
        ProjectionProviderPolicyModeV1::BoundedRowAdmission,
        Span { start: 41, end: 73 },
    );
    declaration.max_value_bytes = u16::MAX;
    let diagnostics = compile_exact_text_family_v1(declaration).unwrap_err();
    assert_eq!(
        diagnostics.as_slice()[0].primary(),
        Span { start: 41, end: 73 }
    );
    assert_eq!(
        diagnostics.as_slice()[0].summary(),
        "exact text provider declaration exceeds a static bound"
    );

    let mut no_order = ExactTextCompilerDeclarationV1::bounded_binary_utf8(
        FieldId::new(7).unwrap(),
        [true, false, false, false],
        ProjectionProviderPolicyModeV1::PartitionAligned,
        Span { start: 80, end: 95 },
    );
    no_order.orders = [false, false];
    let diagnostics = compile_exact_text_family_v1(no_order).unwrap_err();
    assert_eq!(
        diagnostics.as_slice()[0].primary(),
        Span { start: 80, end: 95 }
    );
    assert_eq!(
        diagnostics.as_slice()[0].summary(),
        "exact text provider requires a declared total order"
    );
}
