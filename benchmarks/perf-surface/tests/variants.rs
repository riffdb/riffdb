#![forbid(unsafe_code)]

//! A variant is only useful if it carries exactly the mechanism it names. If a
//! clause silently stops being emitted, or leaks into the baseline, every
//! attribution built on these contracts becomes wrong while still looking
//! plausible, so each variant is checked against the compiled bundle rather
//! than against the rendered text.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_contract_ir::{IndexFieldEncodingV1, TextKeyProfileV1};
use riffdb_perf_surface::{Mechanism, contract_source, variants};

fn document_has_text_key(bundle: &riffdb_contract_ir::ContractBundle) -> bool {
    bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "Document")
        .expect("Document entity")
        .indexes()
        .iter()
        .any(|index| {
            index.encodings().iter().any(|encoding| {
                matches!(
                    encoding,
                    IndexFieldEncodingV1::TextKey(TextKeyProfileV1::BinaryUtf8)
                )
            })
        })
}

#[test]
fn every_variant_compiles_and_carries_exactly_the_mechanisms_it_names() {
    for (name, mechanisms) in variants() {
        let source = contract_source(&mechanisms);
        let bundle = compile_contract_source(&source)
            .unwrap_or_else(|error| panic!("variant {name} must compile: {error:?}"));

        let wants_projection = mechanisms.contains(&Mechanism::Projection);
        assert_eq!(
            bundle.projections().len(),
            usize::from(wants_projection),
            "variant {name} projection presence"
        );

        assert_eq!(
            document_has_text_key(&bundle),
            mechanisms.contains(&Mechanism::TextKey),
            "variant {name} text-key presence"
        );

        // Every variant keeps the parts that are not under test, so a
        // difference between two variants cannot come from anything else.
        assert!(
            bundle
                .commands()
                .iter()
                .any(|command| command.name() == "PublishDocument"),
            "variant {name} must keep the measured command"
        );
        assert_eq!(
            bundle.schema().entities().len(),
            2,
            "variant {name} must keep both entities"
        );
    }
}

#[test]
fn the_baseline_carries_no_mechanism_and_all_carries_every_one() {
    let base = compile_contract_source(&contract_source(&[])).expect("base compiles");
    assert_eq!(base.projections().len(), 0);
    assert!(!document_has_text_key(&base));

    let all = compile_contract_source(&contract_source(&Mechanism::ALL)).expect("all compiles");
    assert_eq!(all.projections().len(), 1);
    assert!(document_has_text_key(&all));
}

#[test]
fn the_baseline_still_emits_the_event_the_projection_would_consume() {
    // Otherwise the projection variant would be measuring event emission as
    // well as projection maintenance, and would overstate the projection.
    for mechanisms in [vec![], vec![Mechanism::Projection]] {
        let source = contract_source(&mechanisms);
        assert!(
            source.contains("emit DocumentPublished"),
            "every variant must emit the event"
        );
        let bundle = compile_contract_source(&source).expect("compiles");
        assert_eq!(bundle.schema().events().len(), 1, "event stays declared");
    }
}
