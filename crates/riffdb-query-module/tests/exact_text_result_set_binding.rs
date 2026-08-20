//! Canonical exact result-set binding round-trip and identity.

use std::num::NonZeroU16;

use riffdb_query_compiler::{
    ExactTextCompilerDeclarationV1, ProjectionResultSetRequirementsV2,
    compile_exact_text_result_family_v1, pin_projection_result_set_provider_v2,
};
use riffdb_query_ir::{ResultSetOutputShapeV1, ResultSetWindowBoundsV2};
use riffdb_query_module::ExactTextResultSetBindingV1;
use riffdb_riffql_syntax::Span;
use riffdb_types::{FieldId, ProjectionProviderPolicyModeV1, QueryOperationName};

#[test]
fn complete_family_and_runtime_bounds_are_one_strict_named_binding() {
    let family =
        compile_exact_text_result_family_v1(ExactTextCompilerDeclarationV1::bounded_binary_utf8(
            FieldId::new(4).unwrap(),
            [false, true, true, true],
            ProjectionProviderPolicyModeV1::PartitionAligned,
            Span { start: 2, end: 8 },
        ))
        .unwrap();
    let plan = pin_projection_result_set_provider_v2(
        family.descriptor().clone(),
        ProjectionResultSetRequirementsV2 {
            filtering: true,
            rank_or_order: true,
            whole_set_measures: true,
            window: ResultSetWindowBoundsV2::Ordinal {
                max_offset: family.max_candidates(),
                max_limit: NonZeroU16::new(500).unwrap(),
            },
            output: ResultSetOutputShapeV1::TypedRows,
        },
    )
    .unwrap();
    let binding = ExactTextResultSetBindingV1::new(
        QueryOperationName::new("SearchUsers").unwrap(),
        family,
        plan,
    )
    .unwrap();
    let bytes = binding.to_canonical_bytes();
    let recovered = ExactTextResultSetBindingV1::from_canonical_bytes(&bytes).unwrap();
    assert_eq!(recovered.to_canonical_bytes(), binding.to_canonical_bytes());
    assert_eq!(recovered.query_name(), binding.query_name());
    assert_eq!(recovered.identity(), binding.identity());
    let mut unknown = bytes;
    unknown[5] = 2;
    assert!(ExactTextResultSetBindingV1::from_canonical_bytes(&unknown).is_err());
}
