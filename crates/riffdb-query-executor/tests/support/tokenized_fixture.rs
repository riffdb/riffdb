use std::num::NonZeroU32;

use riffdb_projection::{
    TokenizedTextConfigV1, TokenizedTextFieldV1, TokenizedTextMutationV1,
    TokenizedTextPartitionIndexV1,
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

pub(super) fn field(value: u32) -> FieldId {
    FieldId::new(value).unwrap()
}

pub(super) fn key(value: u64) -> EntityKey {
    let mut builder = EntityKeyBuilder::new(EntityTypeId::first());
    builder.push_u64(value).unwrap();
    builder.finish().unwrap()
}

pub(super) fn sequence(value: u64) -> CommitSequence {
    CommitSequence::new(value).unwrap()
}

pub(super) fn generation() -> ProjectionGeneration {
    ProjectionGeneration::new(7).unwrap()
}

pub(super) fn config() -> TokenizedTextConfigV1 {
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

pub(super) fn provider() -> TokenizedTextPartitionIndexV1 {
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

pub(super) fn plan(
    kind: TokenizedMatchKindV1,
    max_candidates: u32,
    max_results: u32,
) -> TokenizedTextPlanV1 {
    plan_with_ranking(
        kind,
        TokenizedRankingV1::Boolean,
        max_candidates,
        max_results,
    )
}

pub(super) fn plan_with_ranking(
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
