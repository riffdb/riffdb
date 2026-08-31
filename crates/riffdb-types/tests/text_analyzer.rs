#![forbid(unsafe_code)]

//! Conformance corpus for ADR-0173's frozen v1 analyzers.

use riffdb_types::{TEXT_ANALYZER_V1_UNICODE_VERSION, TextAnalyzerV1};

fn terms(analyzer: TextAnalyzerV1, value: &str) -> Vec<(usize, String)> {
    analyzer
        .analyze(value)
        .map(|term| (term.position(), term.term().to_owned()))
        .collect()
}

#[test]
fn keyword_v1_emits_the_complete_value_exactly_once() {
    for value in ["", "Tag Value", "Straße,東京🚀", "line one\nline two"] {
        assert_eq!(
            terms(TextAnalyzerV1::KeywordV1, value),
            vec![(0, value.to_owned())]
        );
    }
}

#[test]
fn standard_v1_matches_the_frozen_conformance_corpus() {
    for (source, expected) in [
        (
            "The quick (\"brown\") fox can't jump 32.3 feet, right?",
            vec![
                "the", "quick", "brown", "fox", "can't", "jump", "32.3", "feet", "right",
            ],
        ),
        ("Straße ﬁle", vec!["strasse", "file"]),
        ("İstanbul ΣΟΦΟΣ", vec!["i\u{307}stanbul", "σοφοσ"]),
        ("cafe\u{301} CAFÉ", vec!["café", "café"]),
        ("ship🚀now", vec!["ship", "now"]),
        ("東京大学", vec!["東", "京", "大", "学"]),
        ("--- 👩🏽‍💻 ---", vec![]),
    ] {
        assert_eq!(
            terms(TextAnalyzerV1::StandardV1, source),
            expected
                .into_iter()
                .enumerate()
                .map(|(position, term)| (position, term.to_owned()))
                .collect::<Vec<_>>(),
            "conformance mismatch for {source:?}"
        );
    }
}

#[test]
fn standard_v1_is_pinned_and_deterministic() {
    assert_eq!(TEXT_ANALYZER_V1_UNICODE_VERSION, (17, 0, 0));
    let source = "Straße ﬁle İstanbul ΣΟΦΟΣ 東京";
    assert_eq!(
        terms(TextAnalyzerV1::StandardV1, source),
        terms(TextAnalyzerV1::StandardV1, source)
    );
}
