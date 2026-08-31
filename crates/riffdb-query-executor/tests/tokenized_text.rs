//! Reference semantics for exact bounded tokenized matching (ADR-0173).

use std::collections::BTreeMap;
use std::num::NonZeroU32;

use riffdb_projection::{
    TokenizedTextConfigV1, TokenizedTextFieldV1, TokenizedTextMutationV1,
    TokenizedTextPartitionIndexV1,
};
use riffdb_query_executor::{
    TokenizedTextExecutionErrorV1, execute_tokenized_text_v1, riff_bm25_v1_score,
};
use riffdb_query_ir::{
    TokenizedMatchKindV1, TokenizedRankingV1, TokenizedTextPlanFieldV1, TokenizedTextPlanV1,
};
use riffdb_riffql_syntax::Span;
use riffdb_types::{
    CanonicalRecord, CommitSequence, EntityKey, EntityKeyBuilder, EntityTypeId, FieldId, IndexId,
    PartitionKeyHash, ProjectionGeneration, ProjectionProviderCapabilitiesV1,
    ProjectionProviderDescriptorV1, ProjectionProviderKindV1, ProjectionProviderPolicyModeV1,
    ProjectionProviderPostureV1, ProjectionProviderStateIdentityV1,
    ProjectionProviderStaticBoundsV1, TOKENIZED_TEXT_PROVIDER_STATE_SCHEMA_HASH_V1, TextAnalyzerV1,
};

fn field(value: u32) -> FieldId {
    FieldId::new(value).unwrap()
}

fn key(value: u64) -> EntityKey {
    let mut builder = EntityKeyBuilder::new(EntityTypeId::first());
    builder.push_u64(value).unwrap();
    builder.finish().unwrap()
}

fn sequence(value: u64) -> CommitSequence {
    CommitSequence::new(value).unwrap()
}

fn generation() -> ProjectionGeneration {
    ProjectionGeneration::new(7).unwrap()
}

fn config() -> TokenizedTextConfigV1 {
    TokenizedTextConfigV1::new(
        [9; 32],
        TextAnalyzerV1::StandardV1,
        vec![
            TokenizedTextFieldV1::new(field(1), 4).unwrap(),
            TokenizedTextFieldV1::new(field(2), 1).unwrap(),
        ],
    )
    .unwrap()
}

fn provider() -> TokenizedTextPartitionIndexV1 {
    let mut provider = TokenizedTextPartitionIndexV1::new(
        config(),
        PartitionKeyHash::from_bytes([3; 32]),
        generation(),
    );
    let rows = [
        (1, "rust durable database", "fast exact search"),
        (2, "database with rust", "durable systems"),
        (3, "rust guide", "search database durable"),
        (4, "unrelated title", "nothing relevant"),
    ]
    .map(|(id, title, body)| {
        TokenizedTextMutationV1::upsert(
            key(id),
            vec![(field(1), title.to_owned()), (field(2), body.to_owned())],
            CanonicalRecord::new(Vec::new()).unwrap(),
        )
        .unwrap()
    });
    provider.apply(sequence(11), &rows).unwrap();
    provider
}

fn plan(kind: TokenizedMatchKindV1, max_candidates: u32, max_results: u32) -> TokenizedTextPlanV1 {
    plan_with_ranking(
        kind,
        TokenizedRankingV1::Boolean,
        max_candidates,
        max_results,
    )
}

fn plan_with_ranking(
    kind: TokenizedMatchKindV1,
    ranking: TokenizedRankingV1,
    max_candidates: u32,
    max_results: u32,
) -> TokenizedTextPlanV1 {
    let descriptor = ProjectionProviderDescriptorV1::new(
        ProjectionProviderKindV1::TokenizedText,
        ProjectionProviderPostureV1::Exact,
        ProjectionProviderCapabilitiesV1::CANDIDATE
            | ProjectionProviderCapabilitiesV1::FILTER
            | ProjectionProviderCapabilitiesV1::ORDER
            | ProjectionProviderCapabilitiesV1::WINDOW
            | ProjectionProviderCapabilitiesV1::OUTPUT,
        ProjectionProviderPolicyModeV1::PartitionAligned,
        ProjectionProviderStaticBoundsV1 {
            max_candidates,
            max_output_rows: max_results,
            max_measures: 0,
            max_input_bytes: 1_024,
            max_work_units: 10_000,
            max_state_bytes_per_row: 1_024,
            max_diagnostic_bytes: 1_024,
            retained_epochs: 8,
            max_catchup_lag: 8,
            max_epoch_lease_steps: 8,
        },
        ProjectionProviderStateIdentityV1::new(
            NonZeroU32::new(1).unwrap(),
            TOKENIZED_TEXT_PROVIDER_STATE_SCHEMA_HASH_V1,
        ),
    )
    .unwrap();
    TokenizedTextPlanV1::new(
        EntityTypeId::first(),
        IndexId::first(),
        [9; 32],
        TextAnalyzerV1::StandardV1,
        vec![
            TokenizedTextPlanFieldV1::new(field(1), 4).unwrap(),
            TokenizedTextPlanFieldV1::new(field(2), 1).unwrap(),
        ],
        kind,
        ranking,
        8,
        max_candidates,
        max_results,
        descriptor,
        Span { start: 1, end: 2 },
    )
    .unwrap()
}

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
                        TokenizedMatchKindV1::Phrase | TokenizedMatchKindV1::Proximity(_) => false,
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
