//! Reference semantics for exact bounded tokenized matching (ADR-0173).

use std::collections::BTreeMap;

use riffdb_projection::{TokenizedTextMutationV1, TokenizedTextPartitionIndexV1};
use riffdb_query_executor::{
    TokenizedTextExecutionErrorV1, execute_tokenized_text_v1, riff_bm25_v1_score,
};
use riffdb_query_ir::{TokenizedMatchKindV1, TokenizedRankingV1};
use riffdb_types::{CanonicalRecord, PartitionKeyHash};

#[path = "support/tokenized_fixture.rs"]
mod fixture;
use fixture::*;

#[test]
fn riff_bm25_v1_has_exact_fixed_point_scores_and_key_ties() {
    let provider = provider();
    let plan = plan_with_ranking(
        TokenizedMatchKindV1::Disjunction,
        TokenizedRankingV1::RiffBm25V1,
        16,
        16,
    );
    let terms = vec!["rust".to_owned(), "database".to_owned()];
    let scores = [1_u64, 2, 3].map(|id| {
        riff_bm25_v1_score(
            &plan,
            &provider,
            riffdb_types::hash_entity_key(key(id).as_bytes()),
            &terms,
        )
        .unwrap()
    });
    assert_eq!(scores, [3_697_476, 3_697_476, 2_640_400]);
    let page = execute_tokenized_text_v1(
        &plan,
        &provider,
        generation(),
        sequence(11),
        "rust database",
        0,
        16,
    )
    .unwrap();
    assert_eq!(
        ids(&page),
        vec![key(1).as_bytes(), key(2).as_bytes(), key(3).as_bytes()]
    );
}

fn ids(page: &riffdb_query_executor::TokenizedTextResultPageV1) -> Vec<Vec<u8>> {
    page.rows()
        .iter()
        .map(|row| row.key().as_bytes().to_vec())
        .collect()
}

#[test]
fn four_closed_shapes_match_exact_reference_semantics() {
    let provider = provider();
    let conjunction = execute_tokenized_text_v1(
        &plan(TokenizedMatchKindV1::Conjunction, 16, 16),
        &provider,
        generation(),
        sequence(11),
        "rust database",
        0,
        16,
    )
    .unwrap();
    assert_eq!(
        ids(&conjunction),
        vec![key(1).as_bytes(), key(2).as_bytes(), key(3).as_bytes()]
    );

    let disjunction = execute_tokenized_text_v1(
        &plan(TokenizedMatchKindV1::Disjunction, 16, 16),
        &provider,
        generation(),
        sequence(11),
        "rust exact",
        0,
        16,
    )
    .unwrap();
    assert_eq!(
        ids(&disjunction),
        vec![key(1).as_bytes(), key(2).as_bytes(), key(3).as_bytes()]
    );

    let phrase = execute_tokenized_text_v1(
        &plan(TokenizedMatchKindV1::Phrase, 16, 16),
        &provider,
        generation(),
        sequence(11),
        "rust durable",
        0,
        16,
    )
    .unwrap();
    assert_eq!(ids(&phrase), vec![key(1).as_bytes()]);

    let proximity = execute_tokenized_text_v1(
        &plan(TokenizedMatchKindV1::Proximity(2), 16, 16),
        &provider,
        generation(),
        sequence(11),
        "rust database",
        0,
        16,
    )
    .unwrap();
    assert_eq!(ids(&proximity), vec![key(1).as_bytes()]);
}

#[test]
fn budgets_and_epoch_mismatch_refuse_without_partial_rows() {
    let provider = provider();
    assert_eq!(
        execute_tokenized_text_v1(
            &plan(TokenizedMatchKindV1::Disjunction, 1, 1),
            &provider,
            generation(),
            sequence(11),
            "rust exact",
            0,
            1,
        ),
        Err(TokenizedTextExecutionErrorV1::CandidateLimit)
    );
    assert_eq!(
        execute_tokenized_text_v1(
            &plan(TokenizedMatchKindV1::Conjunction, 16, 16),
            &provider,
            generation(),
            sequence(12),
            "rust",
            0,
            1,
        ),
        Err(TokenizedTextExecutionErrorV1::EpochMismatch)
    );
}

#[test]
fn randomized_history_matches_reference_across_recovery_and_compaction() {
    let mut provider = TokenizedTextPartitionIndexV1::new(
        config(),
        PartitionKeyHash::from_bytes([8; 32]),
        generation(),
    );
    let mut reference = BTreeMap::<u64, (String, String)>::new();
    let vocabulary = ["rust", "database", "search", "durable", "exact", "guide"];
    let mut random = 0x51_7c_a9_d3_u64;

    for epoch in 1..=96_u64 {
        random = random
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let id = random % 16 + 1;
        let mutation = if random.is_multiple_of(7) {
            reference.remove(&id);
            TokenizedTextMutationV1::delete(key(id))
        } else {
            let first = vocabulary[(random as usize >> 8) % vocabulary.len()];
            let second = vocabulary[(random as usize >> 16) % vocabulary.len()];
            let third = vocabulary[(random as usize >> 24) % vocabulary.len()];
            let title = format!("{first} {second}");
            let body = format!("{third} {first}");
            reference.insert(id, (title.clone(), body.clone()));
            TokenizedTextMutationV1::upsert(
                key(id),
                vec![(field(1), title), (field(2), body)],
                CanonicalRecord::new(Vec::new()).unwrap(),
            )
            .unwrap()
        };
        provider.apply(sequence(epoch), &[mutation]).unwrap();
        if epoch % 7 == 0 {
            provider.compact().unwrap();
        }
        if epoch % 11 == 0 {
            provider = TokenizedTextPartitionIndexV1::from_checkpoint_bytes(
                &provider.to_checkpoint_bytes().unwrap(),
            )
            .unwrap();
        }

        for kind in [
            TokenizedMatchKindV1::Conjunction,
            TokenizedMatchKindV1::Disjunction,
        ] {
            let page = execute_tokenized_text_v1(
                &plan(kind, 64, 64),
                &provider,
                generation(),
                sequence(epoch),
                "rust search",
                0,
                64,
            )
            .unwrap();
            let mut expected = reference
                .iter()
                .filter_map(|(id, (title, body))| {
                    let combined = format!("{title} {body}");
                    let rust = combined.split_whitespace().any(|term| term == "rust");
                    let search = combined.split_whitespace().any(|term| term == "search");
                    let matches = match kind {
                        TokenizedMatchKindV1::Conjunction => rust && search,
                        TokenizedMatchKindV1::Disjunction => rust || search,
                        TokenizedMatchKindV1::Phrase
                        | TokenizedMatchKindV1::Proximity(_)
                        | TokenizedMatchKindV1::Fuzzy(_) => false,
                    };
                    matches.then(|| key(*id).as_bytes().to_vec())
                })
                .collect::<Vec<_>>();
            expected.sort_unstable();
            assert_eq!(ids(&page), expected, "epoch {epoch}, kind {kind:?}");
            assert_eq!(page.exact_total() as usize, expected.len());
        }
    }
}

// req: OQ-019
#[test]
fn review_proximity_retains_all_reachable_intermediate_positions() {
    let mut provider = provider();
    // a@0, b@1, b@4, c@8: only the later b completes distance-four proximity.
    provider
        .apply(
            sequence(12),
            &[TokenizedTextMutationV1::upsert(
                key(1),
                vec![
                    (field(1), "a b x x b x x x c".into()),
                    (field(2), "".into()),
                ],
                CanonicalRecord::new(Vec::new()).unwrap(),
            )
            .unwrap()],
        )
        .unwrap();
    let plan = plan(TokenizedMatchKindV1::Proximity(4), 10, 10);
    let page =
        execute_tokenized_text_v1(&plan, &provider, generation(), sequence(12), "a b c", 0, 10)
            .unwrap();
    assert_eq!(page.rows().len(), 1);
    assert_eq!(page.rows()[0].key(), &key(1));
}

#[test]
fn fuzzy_bm25_snippets_and_hybrid_rrf_execute_through_compiled_plan() {
    use riffdb_query_executor::execute_tokenized_text_with_vector_v1;
    use riffdb_types::{CanonicalVector, FieldId};

    let vec_field = FieldId::new(3).unwrap();
    let desc = riffdb_types::ProjectionProviderDescriptorV1::new(
        riffdb_types::ProjectionProviderKindV1::TokenizedText,
        riffdb_types::ProjectionProviderPostureV1::Exact,
        riffdb_types::ProjectionProviderCapabilitiesV1::CANDIDATE
            | riffdb_types::ProjectionProviderCapabilitiesV1::FILTER
            | riffdb_types::ProjectionProviderCapabilitiesV1::ORDER
            | riffdb_types::ProjectionProviderCapabilitiesV1::WINDOW
            | riffdb_types::ProjectionProviderCapabilitiesV1::OUTPUT,
        riffdb_types::ProjectionProviderPolicyModeV1::PartitionAligned,
        riffdb_types::ProjectionProviderStaticBoundsV1 {
            max_candidates: 64,
            max_output_rows: 64,
            max_measures: 0,
            max_input_bytes: 1_024,
            max_work_units: 1_024,
            max_state_bytes_per_row: 65_536,
            max_diagnostic_bytes: 1_024,
            retained_epochs: 4,
            max_catchup_lag: 16,
            max_epoch_lease_steps: 64,
        },
        riffdb_types::ProjectionProviderStateIdentityV1::new(
            std::num::NonZeroU32::new(1).unwrap(),
            riffdb_types::TOKENIZED_TEXT_PROVIDER_STATE_SCHEMA_HASH_V1,
        ),
    )
    .unwrap();
    let hybrid_plan = riffdb_query_ir::TokenizedTextPlanV1::new(
        riffdb_types::EntityTypeId::first(),
        riffdb_types::IndexId::first(),
        [7; 32],
        riffdb_types::TextAnalyzerV1::EnglishStemV1,
        vec![
            riffdb_query_ir::TokenizedTextPlanFieldV1::new(FieldId::new(1).unwrap(), 4).unwrap(),
            riffdb_query_ir::TokenizedTextPlanFieldV1::new(FieldId::new(2).unwrap(), 1).unwrap(),
        ],
        TokenizedMatchKindV1::Fuzzy(1),
        TokenizedRankingV1::HybridRrfV1 {
            vector_field: vec_field,
            snippets: true,
        },
        8,
        64,
        64,
        desc,
        riffdb_riffql_syntax::Span { start: 0, end: 10 },
    )
    .unwrap();

    let cfg = riffdb_projection::TokenizedTextConfigV1::new(
        [7; 32],
        riffdb_types::TextAnalyzerV1::EnglishStemV1,
        vec![
            riffdb_projection::TokenizedTextFieldV1::new(FieldId::new(1).unwrap(), 4).unwrap(),
            riffdb_projection::TokenizedTextFieldV1::new(FieldId::new(2).unwrap(), 1).unwrap(),
        ],
    )
    .unwrap();
    let mut prov = riffdb_projection::TokenizedTextPartitionIndexV1::new(
        cfg,
        riffdb_types::PartitionKeyHash::from_bytes([9; 32]),
        generation(),
    );

    let make_row = |id: u64, title: &str, body: &str, v: [f32; 4]| {
        let out = CanonicalRecord::new(vec![
            (
                FieldId::new(1).unwrap(),
                riffdb_types::CanonicalValue::string(title).unwrap(),
            ),
            (
                FieldId::new(2).unwrap(),
                riffdb_types::CanonicalValue::string(body).unwrap(),
            ),
            (
                vec_field,
                riffdb_types::CanonicalValue::Vector(CanonicalVector::new(v.to_vec()).unwrap()),
            ),
        ])
        .unwrap();
        TokenizedTextMutationV1::upsert(
            key(id),
            vec![(field(1), title.to_owned()), (field(2), body.to_owned())],
            out,
        )
        .unwrap()
    };
    prov.apply(
        sequence(1),
        &[
            make_row(
                1,
                "distributed postgres storage",
                "consensus replication guide",
                [0.9, 0.1, 0.0, 0.0],
            ),
            make_row(
                2,
                "postgres query optimizer",
                "postgres indexing internals",
                [0.1, 0.9, 0.0, 0.0],
            ),
        ],
    )
    .unwrap();

    // Typo query "postgre" (edit distance 1 from "postgres") + vector closest to doc 1
    let page = execute_tokenized_text_with_vector_v1(
        &hybrid_plan,
        &prov,
        generation(),
        sequence(1),
        "postgre",
        Some(&[1.0, 0.0, 0.0, 0.0]),
        0,
        10,
    )
    .unwrap();
    assert_eq!(page.exact_total(), 2);
    let top_fields = page.rows()[0].output().fields();
    let riffdb_types::CanonicalValue::String(snippet_title) = &top_fields[0].1 else {
        panic!("expected string title");
    };
    assert!(
        snippet_title.as_str().contains("<<postgres>>"),
        "expected highlighted snippet in title, got: {}",
        snippet_title.as_str()
    );
}
