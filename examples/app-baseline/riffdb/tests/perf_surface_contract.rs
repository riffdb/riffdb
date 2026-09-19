#![forbid(unsafe_code)]

//! The perf-surface contract exists to give the projection evaluator, the
//! exact byte-prefix text path and the tokenized provider benchmark coverage
//! that the ticketdesk contract cannot provide. If it stops compiling, or
//! stops declaring the mechanisms it exists for, the coverage silently
//! disappears and the loads built on it measure nothing in particular.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_contract_ir::{IndexFieldEncodingV1, TextKeyProfileV1};

const PERF_SURFACE: &str = include_str!("../../contracts/perf-surface.riff");

#[test]
fn the_perf_surface_contract_compiles_and_declares_every_covered_mechanism() {
    let bundle = compile_contract_source(PERF_SURFACE).expect("perf-surface contract compiles");

    assert_eq!(
        bundle.projections().len(),
        1,
        "the contract exists to exercise the projection evaluator"
    );

    let document = bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "Document")
        .expect("Document entity");

    // Assert the encoding, not just the index name: dropping the
    // `text_key(...)` clause would leave an index called `by_title` behind and
    // silently remove the mechanism this contract exists to exercise.
    let by_title = document
        .indexes()
        .iter()
        .find(|index| index.name() == "by_title")
        .expect("by_title index");
    assert!(
        by_title.encodings().iter().any(|encoding| matches!(
            encoding,
            IndexFieldEncodingV1::TextKey(TextKeyProfileV1::BinaryUtf8)
        )),
        "by_title must carry the exact byte-prefix text-key encoding"
    );

    assert!(
        bundle
            .commands()
            .iter()
            .any(|command| command.name() == "PublishDocument"),
        "the command that feeds the projection and both text indexes must exist"
    );
}
