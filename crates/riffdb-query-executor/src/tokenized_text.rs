//! Exact bounded tokenized-text execution over maintained postings (ADR-0173).

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

use riffdb_projection::TokenizedTextPartitionIndexV1;
use riffdb_query_ir::{TokenizedMatchKindV1, TokenizedRankingV1, TokenizedTextPlanV1};
use riffdb_types::{
    CanonicalRecord, CommitSequence, EntityKey, EntityKeyHash, HashDomain, ProjectionGeneration,
    hash,
};

/// One compiler-shaped row in canonical entity-key order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TokenizedTextResultRowV1 {
    key: EntityKey,
    output: CanonicalRecord,
}

impl TokenizedTextResultRowV1 {
    /// Canonical authoritative key.
    #[must_use]
    pub const fn key(&self) -> &EntityKey {
        &self.key
    }

    /// Already policy-shaped compiler output.
    #[must_use]
    pub const fn output(&self) -> &CanonicalRecord {
        &self.output
    }
}

/// One all-or-refusal exact page from a selected provider epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TokenizedTextResultPageV1 {
    rows: Vec<TokenizedTextResultRowV1>,
    exact_total: u32,
    generation: ProjectionGeneration,
    epoch: CommitSequence,
    statistics_identity: [u8; 32],
}

impl TokenizedTextResultPageV1 {
    /// Canonically ordered page rows.
    #[must_use]
    pub fn rows(&self) -> &[TokenizedTextResultRowV1] {
        &self.rows
    }

    /// Exact whole match count from the same provider epoch.
    #[must_use]
    pub const fn exact_total(&self) -> u32 {
        self.exact_total
    }

    /// Never-reused provider generation.
    #[must_use]
    pub const fn generation(&self) -> ProjectionGeneration {
        self.generation
    }

    /// Exact provider frontier used for membership and output.
    #[must_use]
    pub const fn epoch(&self) -> CommitSequence {
        self.epoch
    }
    /// Exact authorized-corpus statistics used by this ordering.
    #[must_use]
    pub const fn statistics_identity(&self) -> [u8; 32] {
        self.statistics_identity
    }
}

/// Executes one compiler-sealed boolean shape without scanning entity state.
pub fn execute_tokenized_text_v1(
    plan: &TokenizedTextPlanV1,
    provider: &TokenizedTextPartitionIndexV1,
    expected_generation: ProjectionGeneration,
    expected_epoch: CommitSequence,
    query: &str,
    offset: u32,
    limit: u32,
) -> Result<TokenizedTextResultPageV1, TokenizedTextExecutionErrorV1> {
    validate_binding(plan, provider, expected_generation, expected_epoch)?;
    if query.len() > plan.descriptor().static_bounds().max_input_bytes as usize {
        return Err(TokenizedTextExecutionErrorV1::InputLimit);
    }
    if limit == 0
        || limit > plan.max_results()
        || offset
            .checked_add(limit)
            .is_none_or(|end| end > plan.max_results())
    {
        return Err(TokenizedTextExecutionErrorV1::ResultLimit);
    }

    let mut terms = Vec::new();
    for analyzed in plan.analyzer().analyze(query) {
        if terms.len() >= plan.max_terms() as usize {
            return Err(TokenizedTextExecutionErrorV1::TermLimit);
        }
        terms.push(analyzed.term().to_owned());
    }
    if terms.is_empty() {
        return Err(TokenizedTextExecutionErrorV1::EmptyQuery);
    }
    let statistics = CorpusStatistics::new(plan, provider, &terms)?;
    let statistics_identity = tokenized_statistics_identity(plan, &statistics)?;

    let candidates = match plan.kind() {
        TokenizedMatchKindV1::Conjunction => conjunction_candidates(plan, provider, &terms),
        TokenizedMatchKindV1::Disjunction => disjunction_candidates(plan, provider, &terms),
        TokenizedMatchKindV1::Phrase | TokenizedMatchKindV1::Proximity(_) => {
            positional_candidates(plan, provider, &terms)
        }
    }?;
    if candidates.len() > plan.max_candidates() as usize {
        return Err(TokenizedTextExecutionErrorV1::CandidateLimit);
    }
    if candidates.len() > plan.max_results() as usize {
        return Err(TokenizedTextExecutionErrorV1::ResultLimit);
    }

    // Rank over borrowed rows and copy only the page that is returned. Every
    // candidate must still be scored, because the ranking is total over the
    // candidate set and an offset cannot be applied before the order exists;
    // but a candidate outside the page never needed its key and output cloned,
    // and `output` is a whole `CanonicalRecord`. The comparator, the exact
    // total and the page bounds are unchanged, so the bytes returned are the
    // bytes the previous shape returned.
    let mut ranked = candidates
        .into_iter()
        .map(|candidate| {
            let (key, output) = provider
                .row(candidate)
                .ok_or(TokenizedTextExecutionErrorV1::ProviderCorrupt)?;
            let score = match plan.ranking() {
                TokenizedRankingV1::Boolean => 0,
                TokenizedRankingV1::RiffBm25V1 => {
                    score_with_statistics(plan, provider, candidate, &terms, &statistics)?
                }
            };
            Ok((score, key, output))
        })
        .collect::<Result<Vec<_>, _>>()?;
    ranked.sort_unstable_by(|left, right| match plan.ranking() {
        TokenizedRankingV1::Boolean => left.1.as_bytes().cmp(right.1.as_bytes()),
        TokenizedRankingV1::RiffBm25V1 => right
            .0
            .cmp(&left.0)
            .then_with(|| left.1.as_bytes().cmp(right.1.as_bytes())),
    });
    let exact_total =
        u32::try_from(ranked.len()).map_err(|_| TokenizedTextExecutionErrorV1::ResultLimit)?;
    let start = usize::try_from(offset)
        .map_err(|_| TokenizedTextExecutionErrorV1::ResultLimit)?
        .min(ranked.len());
    let end = start.saturating_add(limit as usize).min(ranked.len());
    let rows = ranked[start..end]
        .iter()
        .map(|(_, key, output)| TokenizedTextResultRowV1 {
            key: (*key).clone(),
            output: (*output).clone(),
        })
        .collect();
    Ok(TokenizedTextResultPageV1 {
        rows,
        exact_total,
        generation: expected_generation,
        epoch: expected_epoch,
        statistics_identity,
    })
}

struct CorpusStatistics<'a> {
    documents: u64,
    lengths: BTreeMap<riffdb_types::FieldId, u64>,
    frequencies: BTreeMap<&'a str, u32>,
}

impl<'a> CorpusStatistics<'a> {
    fn new(
        plan: &TokenizedTextPlanV1,
        provider: &TokenizedTextPartitionIndexV1,
        terms: &'a [String],
    ) -> Result<Self, TokenizedTextExecutionErrorV1> {
        #[cfg(test)]
        STATISTICS_BUILDS.with(|count| count.set(count.get() + 1));
        let overflow = TokenizedTextExecutionErrorV1::ScoreOverflow;
        let documents = u64::try_from(provider.document_count()).map_err(|_| overflow)?;
        let lengths = plan
            .fields()
            .iter()
            .map(|field| {
                Ok((
                    field.field(),
                    provider
                        .total_field_length(field.field())
                        .map_err(|_| overflow)?,
                ))
            })
            .collect::<Result<_, _>>()?;
        let mut frequencies = BTreeMap::new();
        for term in terms {
            if !frequencies.contains_key(term.as_str()) {
                frequencies.insert(
                    term.as_str(),
                    u32::try_from(term_documents(plan, provider, term).len())
                        .map_err(|_| overflow)?,
                );
            }
        }
        Ok(Self {
            documents,
            lengths,
            frequencies,
        })
    }
}

fn tokenized_statistics_identity(
    plan: &TokenizedTextPlanV1,
    statistics: &CorpusStatistics<'_>,
) -> Result<[u8; 32], TokenizedTextExecutionErrorV1> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"RTSTATS\x01");
    bytes.extend_from_slice(&statistics.documents.to_be_bytes());
    for field in plan.fields() {
        bytes.extend_from_slice(&field.field().get().to_be_bytes());
        bytes.extend_from_slice(&statistics.lengths[&field.field()].to_be_bytes());
    }
    for (term, frequency) in &statistics.frequencies {
        bytes.extend_from_slice(
            &u32::try_from(term.len())
                .map_err(|_| TokenizedTextExecutionErrorV1::ScoreOverflow)?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(term.as_bytes());
        bytes.extend_from_slice(&frequency.to_be_bytes());
    }
    Ok(*hash(HashDomain::QueryParameters, &bytes).as_bytes())
}

const BM25_SCALE_V1: u128 = 1_000_000;
const BM25_K1_NUMERATOR_V1: u128 = 1_200;
const BM25_ONE_MINUS_B_NUMERATOR_V1: u128 = 250;
const BM25_B_NUMERATOR_V1: u128 = 750;
const BM25_K1_PLUS_ONE_NUMERATOR_V1: u128 = 2_200;

/// Computes the frozen `riff_bm25_v1` score over one policy-aligned snapshot.
#[doc(hidden)]
pub fn riff_bm25_v1_score(
    plan: &TokenizedTextPlanV1,
    provider: &TokenizedTextPartitionIndexV1,
    candidate: EntityKeyHash,
    terms: &[String],
) -> Result<u64, TokenizedTextExecutionErrorV1> {
    let statistics = CorpusStatistics::new(plan, provider, terms)?;
    score_with_statistics(plan, provider, candidate, terms, &statistics)
}

fn score_with_statistics(
    plan: &TokenizedTextPlanV1,
    provider: &TokenizedTextPartitionIndexV1,
    candidate: EntityKeyHash,
    terms: &[String],
    statistics: &CorpusStatistics<'_>,
) -> Result<u64, TokenizedTextExecutionErrorV1> {
    let documents = u128::from(statistics.documents);
    if documents == 0 {
        return Err(TokenizedTextExecutionErrorV1::ProviderCorrupt);
    }
    let mut score = 0_u128;
    for term in terms {
        let document_frequency = u128::from(statistics.frequencies[term.as_str()]);
        if document_frequency == 0 || document_frequency > documents {
            continue;
        }
        let idf = BM25_SCALE_V1
            .checked_mul(
                documents
                    .checked_sub(document_frequency)
                    .and_then(|value| value.checked_add(1))
                    .ok_or(TokenizedTextExecutionErrorV1::ScoreOverflow)?,
            )
            .ok_or(TokenizedTextExecutionErrorV1::ScoreOverflow)?
            / document_frequency
                .checked_add(1)
                .ok_or(TokenizedTextExecutionErrorV1::ScoreOverflow)?;
        for field in plan.fields() {
            let Some(posting) = provider
                .posting(field.field(), term)
                .and_then(|postings| postings.get(&candidate))
            else {
                continue;
            };
            let frequency = u128::from(posting.frequency());
            let document_length = u128::from(
                provider
                    .field_length(candidate, field.field())
                    .ok_or(TokenizedTextExecutionErrorV1::ProviderCorrupt)?,
            );
            let total_length = u128::from(statistics.lengths[&field.field()]);
            if total_length == 0 {
                return Err(TokenizedTextExecutionErrorV1::ProviderCorrupt);
            }
            // Exact rational evaluation of
            // tf*(k1+1) / (tf + k1*(1-b+b*dl/avgdl)). The shared
            // denominator is 1_000_000*total_length; every division truncates.
            let normalized = BM25_ONE_MINUS_B_NUMERATOR_V1
                .checked_mul(total_length)
                .and_then(|value| {
                    BM25_B_NUMERATOR_V1
                        .checked_mul(document_length)?
                        .checked_mul(documents)?
                        .checked_add(value)
                })
                .ok_or(TokenizedTextExecutionErrorV1::ScoreOverflow)?;
            let denominator = frequency
                .checked_mul(BM25_SCALE_V1)
                .and_then(|value| value.checked_mul(total_length))
                .and_then(|value| {
                    BM25_K1_NUMERATOR_V1
                        .checked_mul(normalized)?
                        .checked_add(value)
                })
                .ok_or(TokenizedTextExecutionErrorV1::ScoreOverflow)?;
            let tf_factor = BM25_SCALE_V1
                .checked_mul(frequency)
                .and_then(|value| value.checked_mul(BM25_K1_PLUS_ONE_NUMERATOR_V1))
                .and_then(|value| value.checked_mul(1_000))
                .and_then(|value| value.checked_mul(total_length))
                .ok_or(TokenizedTextExecutionErrorV1::ScoreOverflow)?
                / denominator;
            let contribution = u128::from(field.weight())
                .checked_mul(idf)
                .and_then(|value| value.checked_mul(tf_factor))
                .ok_or(TokenizedTextExecutionErrorV1::ScoreOverflow)?
                / BM25_SCALE_V1;
            score = score
                .checked_add(contribution)
                .ok_or(TokenizedTextExecutionErrorV1::ScoreOverflow)?;
        }
    }
    u64::try_from(score).map_err(|_| TokenizedTextExecutionErrorV1::ScoreOverflow)
}

fn validate_binding(
    plan: &TokenizedTextPlanV1,
    provider: &TokenizedTextPartitionIndexV1,
    expected_generation: ProjectionGeneration,
    expected_epoch: CommitSequence,
) -> Result<(), TokenizedTextExecutionErrorV1> {
    if provider.generation() != expected_generation || provider.frontier() != Some(expected_epoch) {
        return Err(TokenizedTextExecutionErrorV1::EpochMismatch);
    }
    if provider.config().index_identity() != plan.index_identity()
        || provider.config().analyzer() != plan.analyzer()
        || provider.config().fields().len() != plan.fields().len()
        || provider
            .config()
            .fields()
            .iter()
            .zip(plan.fields())
            .any(|(state, planned)| {
                state.field() != planned.field() || state.weight() != planned.weight()
            })
    {
        return Err(TokenizedTextExecutionErrorV1::PlanMismatch);
    }
    Ok(())
}

fn term_documents(
    plan: &TokenizedTextPlanV1,
    provider: &TokenizedTextPartitionIndexV1,
    term: &str,
) -> BTreeSet<EntityKeyHash> {
    #[cfg(test)]
    POSTING_ENUMERATIONS.with(|count| count.set(count.get() + 1));
    plan.fields()
        .iter()
        .filter_map(|field| provider.posting(field.field(), term))
        .flat_map(|posting| posting.keys().copied())
        .collect()
}

fn conjunction_candidates(
    plan: &TokenizedTextPlanV1,
    provider: &TokenizedTextPartitionIndexV1,
    terms: &[String],
) -> Result<BTreeSet<EntityKeyHash>, TokenizedTextExecutionErrorV1> {
    let mut candidates = term_documents(plan, provider, &terms[0]);
    check_candidates(plan, candidates.len())?;
    for term in &terms[1..] {
        let documents = term_documents(plan, provider, term);
        candidates.retain(|candidate| documents.contains(candidate));
    }
    Ok(candidates)
}

fn disjunction_candidates(
    plan: &TokenizedTextPlanV1,
    provider: &TokenizedTextPartitionIndexV1,
    terms: &[String],
) -> Result<BTreeSet<EntityKeyHash>, TokenizedTextExecutionErrorV1> {
    let mut candidates = BTreeSet::new();
    for term in terms {
        candidates.extend(term_documents(plan, provider, term));
        check_candidates(plan, candidates.len())?;
    }
    Ok(candidates)
}

fn positional_candidates(
    plan: &TokenizedTextPlanV1,
    provider: &TokenizedTextPartitionIndexV1,
    terms: &[String],
) -> Result<BTreeSet<EntityKeyHash>, TokenizedTextExecutionErrorV1> {
    let mut result = BTreeSet::new();
    for field in plan.fields() {
        let Some(first) = provider.posting(field.field(), &terms[0]) else {
            continue;
        };
        for candidate in first.keys().copied() {
            if terms[1..].iter().all(|term| {
                provider
                    .posting(field.field(), term)
                    .is_some_and(|posting| posting.contains_key(&candidate))
            }) && positions_match(plan.kind(), provider, field.field(), candidate, terms)
            {
                result.insert(candidate);
                check_candidates(plan, result.len())?;
            }
        }
    }
    Ok(result)
}

fn positions_match(
    kind: TokenizedMatchKindV1,
    provider: &TokenizedTextPartitionIndexV1,
    field: riffdb_types::FieldId,
    candidate: EntityKeyHash,
    terms: &[String],
) -> bool {
    let distance = match kind {
        TokenizedMatchKindV1::Phrase => 1,
        TokenizedMatchKindV1::Proximity(distance) => u32::from(distance),
        TokenizedMatchKindV1::Conjunction | TokenizedMatchKindV1::Disjunction => return false,
    };
    let positions = terms
        .iter()
        .filter_map(|term| provider.posting(field, term)?.get(&candidate))
        .map(|posting| posting.positions())
        .collect::<Vec<_>>();
    if positions.len() != terms.len() {
        return false;
    }
    ordered_positions_match(&positions, distance)
}

fn ordered_positions_match(positions: &[&[u32]], distance: u32) -> bool {
    let Some(first) = positions.first() else {
        return false;
    };
    // Keep every reachable position. A later occurrence can reach the next
    // term even when the earliest occurrence cannot. Each posting is sorted;
    // the predecessor cursor advances once per lane, without backtracking.
    let mut reachable = first.to_vec();
    let mut next_reachable = Vec::new();
    for next_positions in &positions[1..] {
        next_reachable.clear();
        let mut predecessor = 0;
        for &position in *next_positions {
            let lower = position.saturating_sub(distance);
            while predecessor < reachable.len() && reachable[predecessor] < lower {
                predecessor += 1;
            }
            if reachable
                .get(predecessor)
                .is_some_and(|previous| *previous < position)
            {
                next_reachable.push(position);
            }
        }
        if next_reachable.is_empty() {
            return false;
        }
        std::mem::swap(&mut reachable, &mut next_reachable);
    }
    !reachable.is_empty()
}

fn check_candidates(
    plan: &TokenizedTextPlanV1,
    count: usize,
) -> Result<(), TokenizedTextExecutionErrorV1> {
    if count > plan.max_candidates() as usize {
        Err(TokenizedTextExecutionErrorV1::CandidateLimit)
    } else {
        Ok(())
    }
}

/// Safe public refusal categories; no query terms or row contents are exposed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TokenizedTextExecutionErrorV1 {
    /// Compiled plan and selected provider configuration differ.
    PlanMismatch,
    /// Requested provider generation or frontier is unavailable.
    EpochMismatch,
    /// Query bytes exceed the compiler-sealed input bound.
    InputLimit,
    /// Analyzer produced no terms.
    EmptyQuery,
    /// Analyzer output exceeds the compiler-sealed term budget.
    TermLimit,
    /// Maintained postings produce too many candidates.
    CandidateLimit,
    /// Requested or whole result window exceeds the compiler-sealed bound.
    ResultLimit,
    /// Maintained state is internally inconsistent.
    ProviderCorrupt,
    /// Fixed-point relevance arithmetic exceeded its frozen unsigned range.
    ScoreOverflow,
}

impl fmt::Display for TokenizedTextExecutionErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::PlanMismatch => "tokenized query plan does not match the selected provider",
            Self::EpochMismatch => "tokenized provider epoch is unavailable",
            Self::InputLimit => "tokenized query input exceeds its declared bound",
            Self::EmptyQuery => "tokenized query produces no terms",
            Self::TermLimit => "tokenized query exceeds its declared term bound",
            Self::CandidateLimit => "tokenized query exceeds its declared candidate bound",
            Self::ResultLimit => "tokenized query exceeds its declared result bound",
            Self::ProviderCorrupt => "tokenized provider state is inconsistent",
            Self::ScoreOverflow => "tokenized relevance score exceeds its fixed-point bound",
        })
    }
}

impl Error for TokenizedTextExecutionErrorV1 {}

#[cfg(test)]
mod positional_tests {
    use super::ordered_positions_match;

    fn reference(lanes: &[&[u32]], previous: Option<u32>, distance: u32) -> bool {
        match lanes.split_first() {
            None => true,
            Some((lane, rest)) => lane.iter().any(|&position| {
                previous.is_none_or(|prior| position > prior && position - prior <= distance)
                    && reference(rest, Some(position), distance)
            }),
        }
    }

    // req: OQ-019
    #[test]
    fn review_positional_matching_agrees_with_exhaustive_paths() {
        let lanes = (0..16)
            .map(|mask| {
                (0..4)
                    .filter(|position| mask & (1 << position) != 0)
                    .collect::<Vec<u32>>()
            })
            .collect::<Vec<_>>();
        for first in &lanes {
            for second in &lanes {
                for third in &lanes {
                    for distance in 1..=4 {
                        let positions = [first.as_slice(), second.as_slice(), third.as_slice()];
                        assert_eq!(
                            ordered_positions_match(&positions, distance),
                            reference(&positions, None, distance),
                            "{positions:?} distance {distance}"
                        );
                    }
                }
            }
        }
    }
}

#[cfg(test)]
thread_local! { static STATISTICS_BUILDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; static POSTING_ENUMERATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

#[cfg(test)]
mod statistics_tests {
    use super::*;
    #[allow(dead_code)]
    mod fixture {
        include!("../tests/support/tokenized_fixture.rs");
    }

    #[test]
    fn paging_returns_the_same_order_and_rows_as_one_unpaged_page() {
        // Ranking borrows rows and copies only the returned page, so paging is
        // the thing that change could break. Walking one row at a time has to
        // reconstruct the unpaged page exactly: same order, same bytes, and the
        // same exact_total at every offset, which is what a caller pages on.
        let provider = fixture::provider();
        for ranking in [TokenizedRankingV1::RiffBm25V1, TokenizedRankingV1::Boolean] {
            let plan =
                fixture::plan_with_ranking(TokenizedMatchKindV1::Disjunction, ranking, 16, 16);
            let whole = execute_tokenized_text_v1(
                &plan,
                &provider,
                fixture::generation(),
                fixture::sequence(11),
                "rust database",
                0,
                16,
            )
            .unwrap();
            assert!(whole.rows.len() > 1, "fixture must page to be meaningful");

            let mut walked = Vec::new();
            for offset in 0..u32::try_from(whole.rows.len()).unwrap() {
                let page = execute_tokenized_text_v1(
                    &plan,
                    &provider,
                    fixture::generation(),
                    fixture::sequence(11),
                    "rust database",
                    offset,
                    1,
                )
                .unwrap();
                assert_eq!(page.rows.len(), 1, "offset {offset}");
                assert_eq!(
                    page.exact_total, whole.exact_total,
                    "exact_total must not depend on the page at offset {offset}"
                );
                walked.push(page.rows[0].clone());
            }
            assert_eq!(walked.len(), whole.rows.len());
            for (index, (paged, unpaged)) in walked.iter().zip(&whole.rows).enumerate() {
                assert_eq!(paged.key, unpaged.key, "key diverged at rank {index}");
                assert_eq!(
                    paged.output, unpaged.output,
                    "output diverged at rank {index}"
                );
            }

            // Past the end returns nothing rather than clamping into the page.
            let beyond = execute_tokenized_text_v1(
                &plan,
                &provider,
                fixture::generation(),
                fixture::sequence(11),
                "rust database",
                u32::try_from(whole.rows.len()).unwrap() + 5,
                4,
            )
            .unwrap();
            assert!(beyond.rows.is_empty());
            assert_eq!(beyond.exact_total, whole.exact_total);
        }
    }

    // req: OQ-019
    #[test]
    fn review_statistics_are_snapshot_scoped_and_duplicate_terms_keep_exact_scores() {
        let provider = fixture::provider();
        let plan = fixture::plan_with_ranking(
            TokenizedMatchKindV1::Disjunction,
            TokenizedRankingV1::RiffBm25V1,
            16,
            16,
        );
        STATISTICS_BUILDS.with(|count| count.set(0));
        POSTING_ENUMERATIONS.with(|count| count.set(0));
        let page = execute_tokenized_text_v1(
            &plan,
            &provider,
            fixture::generation(),
            fixture::sequence(11),
            "rust database",
            0,
            16,
        )
        .unwrap();
        assert_eq!(STATISTICS_BUILDS.with(std::cell::Cell::get), 1);
        assert_eq!(POSTING_ENUMERATIONS.with(std::cell::Cell::get), 4);
        let repeated = execute_tokenized_text_v1(
            &plan,
            &provider,
            fixture::generation(),
            fixture::sequence(11),
            "rust database rust database",
            0,
            16,
        )
        .unwrap();
        assert_eq!(STATISTICS_BUILDS.with(std::cell::Cell::get), 2);
        assert_eq!(POSTING_ENUMERATIONS.with(std::cell::Cell::get), 10);
        assert_eq!(page.statistics_identity(), repeated.statistics_identity());
        assert_eq!(page.rows.len(), 3);
        for (single, twice) in page.rows.iter().zip(&repeated.rows) {
            assert_eq!(single.key, twice.key);
            let key = riffdb_types::hash_entity_key(single.key.as_bytes());
            let terms = vec!["rust".to_owned(), "database".to_owned()];
            let doubled = [terms.clone(), terms.clone()].concat();
            assert_eq!(
                riff_bm25_v1_score(&plan, &provider, key, &terms).unwrap() * 2,
                riff_bm25_v1_score(&plan, &provider, key, &doubled).unwrap()
            );
        }
        let terms = vec!["rust".to_owned(), "database".to_owned(), "rust".to_owned()];
        let statistics = CorpusStatistics::new(&plan, &provider, &terms).unwrap();
        assert_eq!(statistics.frequencies.len(), 2);
        assert_eq!(statistics.lengths.len(), 2);
    }
}
