//! Exact nearest-neighbor search for the vector projection reference path.
//!
//! This module provides brute-force (exact) K-nearest-neighbor search over
//! org-partitioned vector segments. It is the reference implementation for
//! WP-593 and the ground-truth recall harness for WP-594's approximate tier.
//!
//! `exact_knn` ranks exactly the candidate slice it is given. Callers own
//! candidate assembly; the engine entry point
//! [`crate::nearest_query_snapshot`] applies org scoping and predicate
//! filtering BEFORE building that slice (VEC-006/VEC-007: the filter runs
//! before distance ranking).

#![forbid(unsafe_code)]

use riffdb_types::{CanonicalVector, DistanceMetric};

/// One scored candidate from a nearest-neighbor search.
#[derive(Clone, Debug)]
pub struct ScoredCandidate {
    /// Index into the candidate set (caller maps to entity identity).
    pub index: usize,
    /// Distance from the query vector (lower is closer for all metrics).
    pub distance: f32,
}

/// Typed rejection of a malformed KNN input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NearestError {
    /// A candidate's dimension differs from the query vector's dimension.
    ///
    /// Recoverable runtime condition (for example dimension skew across a
    /// projection segment mid-migration): a typed error, never a panic.
    DimensionMismatch {
        /// The query vector's dimension.
        query: u32,
        /// The offending candidate's dimension.
        candidate: u32,
        /// Index of the offending candidate in the input slice.
        index: usize,
    },
}

impl std::fmt::Display for NearestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DimensionMismatch {
                query,
                candidate,
                index,
            } => write!(
                f,
                "candidate {index} has dimension {candidate}; the query vector has {query}"
            ),
        }
    }
}

impl std::error::Error for NearestError {}

/// Performs exact KNN over a set of candidate vectors.
///
/// Crate-private on purpose: [`crate::nearest_query_snapshot`] is the only
/// entry point, so org scoping and predicate filtering can never be bypassed
/// by ranking a hand-assembled candidate slice (VEC-006/VEC-007).
///
/// # Arguments
/// * `query` - The query vector
/// * `candidates` - The candidate vectors (already org-scoped and filtered)
/// * `metric` - The distance metric to use
/// * `k` - Maximum results to return
///
/// # Returns
/// Up to `k` results sorted by distance ascending (closest first), or a
/// typed error if any candidate's dimension differs from the query's.
pub(crate) fn exact_knn(
    query: &CanonicalVector,
    candidates: &[&CanonicalVector],
    metric: DistanceMetric,
    k: u32,
) -> Result<Vec<ScoredCandidate>, NearestError> {
    if candidates.is_empty() || k == 0 {
        return Ok(Vec::new());
    }

    let k = k as usize;
    let mut scored = Vec::with_capacity(candidates.len());
    for (index, candidate) in candidates.iter().enumerate() {
        if candidate.dimension() != query.dimension() {
            return Err(NearestError::DimensionMismatch {
                query: query.dimension(),
                candidate: candidate.dimension(),
                index,
            });
        }
        scored.push(ScoredCandidate {
            index,
            distance: compute_distance(query.components(), candidate.components(), metric),
        });
    }

    // Partial sort: only need top-k, but for exact reference path we do a full
    // sort for determinism and simplicity. The approximate tier (WP-594) will
    // use a bounded heap. `total_cmp` is a total order (transitive), unlike
    // the previous `partial_cmp(..).unwrap_or(Equal)` comparator, which was
    // non-transitive in the presence of NaN and made the sort order
    // unspecified.
    scored.sort_by(|a, b| a.distance.total_cmp(&b.distance));

    scored.truncate(k);
    Ok(scored)
}

/// Computes the distance between two equal-dimension vectors under the given metric.
///
/// Canonical vectors carry only finite components, but accumulation over
/// finite inputs can still overflow to an infinity or produce NaN. EVERY
/// non-finite result — NaN, `+infinity`, and `-infinity` alike — is mapped
/// to `+infinity` so an unrankable pair deterministically sorts last.
/// Sanitizing only NaN previously let a dot-product overflow return
/// `-infinity`, which is not NaN and therefore sorted FIRST: one row of
/// large finite components deterministically seized rank 0 of every
/// dot-product result.
#[inline]
pub(crate) fn compute_distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
    let distance = match metric {
        DistanceMetric::Cosine => cosine_distance(a, b),
        DistanceMetric::Euclidean => euclidean_distance(a, b),
        DistanceMetric::DotProduct => dot_product_distance(a, b),
    };
    if distance.is_finite() {
        distance
    } else {
        f32::INFINITY
    }
}

/// Cosine distance = 1 - cosine_similarity.
/// Range: [0, 2] where 0 = identical direction.
#[inline]
fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0_f32;
    let mut norm_a = 0.0_f32;
    let mut norm_b = 0.0_f32;
    for (ai, bi) in a.iter().zip(b.iter()) {
        dot += ai * bi;
        norm_a += ai * ai;
        norm_b += bi * bi;
    }
    let denominator = norm_a.sqrt() * norm_b.sqrt();
    if denominator == 0.0 {
        return 1.0; // undefined → maximum distance
    }
    1.0 - (dot / denominator)
}

/// Euclidean (L2) distance.
/// Range: [0, ∞) where 0 = identical.
#[inline]
fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    let mut sum = 0.0_f32;
    for (ai, bi) in a.iter().zip(b.iter()) {
        let diff = ai - bi;
        sum += diff * diff;
    }
    sum.sqrt()
}

/// Negative dot product distance (higher dot product = closer).
/// Range: (-∞, ∞) but normalized vectors give [−1, 1] mapped to [0, 2].
#[inline]
fn dot_product_distance(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0_f32;
    for (ai, bi) in a.iter().zip(b.iter()) {
        dot += ai * bi;
    }
    -dot // negate so lower = closer
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vec_from(components: &[f32]) -> CanonicalVector {
        CanonicalVector::new(components.to_vec()).unwrap()
    }

    fn knn(
        query: &CanonicalVector,
        candidates: &[&CanonicalVector],
        metric: DistanceMetric,
        k: u32,
    ) -> Vec<ScoredCandidate> {
        exact_knn(query, candidates, metric, k).expect("equal dimensions")
    }

    #[test]
    fn exact_knn_returns_closest_by_cosine() {
        let query = vec_from(&[1.0, 0.0, 0.0]);
        let c1 = vec_from(&[1.0, 0.0, 0.0]); // identical
        let c2 = vec_from(&[0.0, 1.0, 0.0]); // orthogonal
        let c3 = vec_from(&[0.7, 0.7, 0.0]); // 45 degrees
        let candidates: Vec<&CanonicalVector> = vec![&c1, &c2, &c3];

        let results = knn(&query, &candidates, DistanceMetric::Cosine, 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].index, 0); // identical is closest
        assert_eq!(results[1].index, 2); // 45 degrees is next
    }

    #[test]
    fn exact_knn_returns_closest_by_euclidean() {
        let query = vec_from(&[0.0, 0.0]);
        let c1 = vec_from(&[1.0, 0.0]); // distance 1
        let c2 = vec_from(&[3.0, 4.0]); // distance 5
        let c3 = vec_from(&[0.5, 0.5]); // distance ~0.7
        let candidates: Vec<&CanonicalVector> = vec![&c1, &c2, &c3];

        let results = knn(&query, &candidates, DistanceMetric::Euclidean, 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].index, 2); // closest
        assert_eq!(results[1].index, 0); // next
    }

    #[test]
    fn exact_knn_returns_closest_by_dot_product() {
        let query = vec_from(&[1.0, 0.0]);
        let c1 = vec_from(&[2.0, 0.0]); // dot=2 → distance=-2
        let c2 = vec_from(&[0.5, 0.0]); // dot=0.5 → distance=-0.5
        let c3 = vec_from(&[-1.0, 0.0]); // dot=-1 → distance=1
        let candidates: Vec<&CanonicalVector> = vec![&c1, &c2, &c3];

        let results = knn(&query, &candidates, DistanceMetric::DotProduct, 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].index, 0); // highest dot product = lowest distance
        assert_eq!(results[1].index, 1);
    }

    #[test]
    fn exact_knn_respects_k_bound() {
        let query = vec_from(&[1.0, 0.0]);
        let c1 = vec_from(&[1.0, 0.0]);
        let c2 = vec_from(&[0.9, 0.1]);
        let c3 = vec_from(&[0.8, 0.2]);
        let candidates: Vec<&CanonicalVector> = vec![&c1, &c2, &c3];

        let results = knn(&query, &candidates, DistanceMetric::Cosine, 1);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].index, 0);
    }

    #[test]
    fn exact_knn_handles_empty_candidates() {
        let query = vec_from(&[1.0, 0.0]);
        let candidates: Vec<&CanonicalVector> = vec![];
        let results = knn(&query, &candidates, DistanceMetric::Cosine, 10);
        assert!(results.is_empty());
    }

    #[test]
    fn exact_knn_handles_zero_k() {
        let query = vec_from(&[1.0, 0.0]);
        let c1 = vec_from(&[1.0, 0.0]);
        let candidates: Vec<&CanonicalVector> = vec![&c1];
        let results = knn(&query, &candidates, DistanceMetric::Cosine, 0);
        assert!(results.is_empty());
    }

    /// A dimension-mismatched candidate is a typed error, never a panic
    /// (previously `assert_eq!` panicked on runtime data).
    #[test]
    fn exact_knn_rejects_dimension_mismatch_as_a_typed_error() {
        let query = vec_from(&[1.0, 0.0, 0.0]);
        let good = vec_from(&[0.5, 0.5, 0.5]);
        let skewed = vec_from(&[1.0, 0.0]);
        let candidates: Vec<&CanonicalVector> = vec![&good, &skewed];
        let error = exact_knn(&query, &candidates, DistanceMetric::Cosine, 2)
            .expect_err("dimension skew must be a typed error");
        assert_eq!(
            error,
            NearestError::DimensionMismatch {
                query: 3,
                candidate: 2,
                index: 1,
            }
        );
    }

    #[test]
    fn cosine_distance_of_identical_vectors_is_zero() {
        let d = cosine_distance(&[1.0, 2.0, 3.0], &[1.0, 2.0, 3.0]);
        assert!(d.abs() < 1e-6);
    }

    #[test]
    fn euclidean_distance_of_identical_vectors_is_zero() {
        let d = euclidean_distance(&[1.0, 2.0, 3.0], &[1.0, 2.0, 3.0]);
        assert!(d.abs() < 1e-6);
    }

    /// Overflowed accumulations cannot poison the ranking under ANY metric:
    /// every non-finite distance — NaN, `+inf`, and `-inf` — is mapped to
    /// `+infinity` and sorts last; the comparator is a total order.
    ///
    /// One poisoned stored row (large finite components) against an ordinary
    /// query. Per metric, the overflow before sanitization differs:
    /// - Cosine: `inf / inf = NaN`
    /// - Euclidean: `+inf`
    /// - DotProduct: `-inf` — the case an `is_nan()`-only sanitizer misses,
    ///   which let the overflow row deterministically seize rank 0.
    #[test]
    fn non_finite_distances_sort_last_deterministically_for_every_metric() {
        // Both query components are nonzero so the poisoned row's product
        // terms accumulate MAX + MAX and overflow under every metric.
        let query = vec_from(&[1.0, 1.0]);
        let near = vec_from(&[1.0, 0.1]);
        let far = vec_from(&[0.0, 1.0]);
        let poisoned = vec_from(&[f32::MAX, f32::MAX]);
        let candidates: Vec<&CanonicalVector> = vec![&poisoned, &near, &far];

        for metric in [
            DistanceMetric::Cosine,
            DistanceMetric::Euclidean,
            DistanceMetric::DotProduct,
        ] {
            let results = knn(&query, &candidates, metric, 3);
            assert_eq!(results.len(), 3, "{metric}: all candidates returned");
            assert!(
                results.iter().all(|scored| !scored.distance.is_nan()),
                "{metric}: no NaN survives sanitization"
            );
            // The clean rows keep their true relative order at the top.
            assert_eq!(results[0].index, 1, "{metric}: near row ranks first");
            assert_eq!(results[1].index, 2, "{metric}: far row ranks second");
            // The poisoned row sorts LAST as +infinity — never rank 0.
            assert_eq!(
                results[2].index, 0,
                "{metric}: the overflow row must sort last, never seize rank 0"
            );
            assert_eq!(results[2].distance, f32::INFINITY);
        }
    }

    /// The DotProduct overflow really is negative before sanitization: this
    /// pins the direction so the `is_finite` guard cannot regress to
    /// `is_nan` (under which `-inf` passes through and ranks first).
    #[test]
    fn dot_product_overflow_is_negative_infinity_before_sanitization() {
        let raw = dot_product_distance(&[1.0, 1.0], &[f32::MAX, f32::MAX]);
        assert_eq!(raw, f32::NEG_INFINITY);
    }

    /// Candidate-set exclusion at the `exact_knn` boundary, with the
    /// k-discriminating case: at `k = 2` over {excluded-nearest, a, b} the
    /// filtered run must return BOTH remaining candidates. A
    /// filter-after-ranking implementation (rank all three, then drop the
    /// excluded row) returns only one row here — this test reds it. The
    /// engine-level proof over predicates lives in
    /// `tests/projection_semantics.rs`.
    #[test]
    fn excluding_the_nearest_candidate_before_ranking_fills_k_from_the_rest() {
        let query = vec_from(&[1.0, 0.0, 0.0]);
        let excluded_nearest = vec_from(&[1.0, 0.0, 0.0]);
        let a = vec_from(&[0.7, 0.7, 0.0]);
        let b = vec_from(&[0.0, 1.0, 0.0]);

        let unfiltered: Vec<&CanonicalVector> = vec![&excluded_nearest, &a, &b];
        let filtered: Vec<&CanonicalVector> = vec![&a, &b];

        let ranked_then_dropped: Vec<ScoredCandidate> =
            knn(&query, &unfiltered, DistanceMetric::Cosine, 2)
                .into_iter()
                .filter(|scored| scored.index != 0)
                .collect();
        let filtered_then_ranked = knn(&query, &filtered, DistanceMetric::Cosine, 2);

        // The order swap is observable: filter-after-rank starves the result.
        assert_eq!(ranked_then_dropped.len(), 1);
        assert_eq!(filtered_then_ranked.len(), 2);
        assert!(filtered_then_ranked[0].distance < filtered_then_ranked[1].distance);
    }
}
