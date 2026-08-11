//! Exact nearest-neighbor search for the vector projection reference path.
//!
//! This module provides brute-force (exact) K-nearest-neighbor search over
//! org-partitioned vector segments. It is the reference implementation for
//! WP-593 and the ground-truth recall harness for WP-594's approximate tier.
//!
//! All distance computations run only over rows that passed policy filtering
//! (VEC-007: policy before ranking).

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

/// Performs exact KNN over a set of candidate vectors.
///
/// # Arguments
/// * `query` - The query vector
/// * `candidates` - The candidate vectors (already policy-filtered)
/// * `metric` - The distance metric to use
/// * `k` - Maximum results to return
///
/// # Returns
/// Up to `k` results sorted by distance ascending (closest first).
///
/// # Panics
/// Panics if any candidate has a different dimension than the query.
pub fn exact_knn(
    query: &CanonicalVector,
    candidates: &[&CanonicalVector],
    metric: DistanceMetric,
    k: u32,
) -> Vec<ScoredCandidate> {
    if candidates.is_empty() || k == 0 {
        return Vec::new();
    }

    let k = k as usize;
    let mut scored: Vec<ScoredCandidate> = candidates
        .iter()
        .enumerate()
        .map(|(index, candidate)| {
            assert_eq!(
                query.dimension(),
                candidate.dimension(),
                "candidate dimension mismatch"
            );
            ScoredCandidate {
                index,
                distance: compute_distance(query.components(), candidate.components(), metric),
            }
        })
        .collect();

    // Partial sort: only need top-k, but for exact reference path we do a full
    // sort for determinism and simplicity. The approximate tier (WP-594) will
    // use a bounded heap.
    scored.sort_by(|a, b| {
        a.distance
            .partial_cmp(&b.distance)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    scored.truncate(k);
    scored
}

/// Computes the distance between two equal-dimension vectors under the given metric.
///
/// All metrics return a non-negative value where 0.0 means identical.
#[inline]
fn compute_distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
    match metric {
        DistanceMetric::Cosine => cosine_distance(a, b),
        DistanceMetric::Euclidean => euclidean_distance(a, b),
        DistanceMetric::DotProduct => dot_product_distance(a, b),
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

    #[test]
    fn exact_knn_returns_closest_by_cosine() {
        let query = vec_from(&[1.0, 0.0, 0.0]);
        let c1 = vec_from(&[1.0, 0.0, 0.0]); // identical
        let c2 = vec_from(&[0.0, 1.0, 0.0]); // orthogonal
        let c3 = vec_from(&[0.7, 0.7, 0.0]); // 45 degrees
        let candidates: Vec<&CanonicalVector> = vec![&c1, &c2, &c3];

        let results = exact_knn(&query, &candidates, DistanceMetric::Cosine, 2);
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

        let results = exact_knn(&query, &candidates, DistanceMetric::Euclidean, 2);
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

        let results = exact_knn(&query, &candidates, DistanceMetric::DotProduct, 2);
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

        let results = exact_knn(&query, &candidates, DistanceMetric::Cosine, 1);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].index, 0);
    }

    #[test]
    fn exact_knn_handles_empty_candidates() {
        let query = vec_from(&[1.0, 0.0]);
        let candidates: Vec<&CanonicalVector> = vec![];
        let results = exact_knn(&query, &candidates, DistanceMetric::Cosine, 10);
        assert!(results.is_empty());
    }

    #[test]
    fn exact_knn_handles_zero_k() {
        let query = vec_from(&[1.0, 0.0]);
        let c1 = vec_from(&[1.0, 0.0]);
        let candidates: Vec<&CanonicalVector> = vec![&c1];
        let results = exact_knn(&query, &candidates, DistanceMetric::Cosine, 0);
        assert!(results.is_empty());
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
}
