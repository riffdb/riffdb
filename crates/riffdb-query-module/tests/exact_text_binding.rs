//! Exact-text named binding compatibility and strictness.

use riffdb_query_compiler::{ExactTextCompilerDeclarationV1, compile_exact_text_family_v1};
use riffdb_query_module::ExactTextQueryBindingV1;
use riffdb_riffql_syntax::Span;
use riffdb_types::{FieldId, ProjectionProviderPolicyModeV1, QueryOperationName};

#[test]
fn exact_text_binding_is_canonical_complete_and_strict() {
    let family = compile_exact_text_family_v1(ExactTextCompilerDeclarationV1::bounded_binary_utf8(
        FieldId::new(7).unwrap(),
        [true, true, true, true],
        ProjectionProviderPolicyModeV1::PartitionAligned,
        Span { start: 10, end: 20 },
    ))
    .unwrap();
    let binding =
        ExactTextQueryBindingV1::new(QueryOperationName::new("SearchUsers").unwrap(), &family);
    let bytes = binding.to_canonical_bytes();
    assert_eq!(&bytes[..6], b"RXTB\0\x01");
    assert_eq!(
        ExactTextQueryBindingV1::from_canonical_bytes(&bytes).unwrap(),
        binding
    );

    let mut unknown = bytes;
    unknown[5] = 2;
    assert!(ExactTextQueryBindingV1::from_canonical_bytes(&unknown).is_err());
}
