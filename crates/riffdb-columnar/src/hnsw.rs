//! Deterministic bounded first-party HNSW for one admitted organization set.
//!
//! The graph is built only after organization scoping, scalar predicates, and
//! principal-policy admission. Consequently its entry point and topology are
//! functions solely of rows authorized for this exact query; another tenant
//! or a denied row cannot shape traversal.

#![forbid(unsafe_code)]

use riffdb_types::{CanonicalVector, DistanceMetric};
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use crate::nearest::{NearestError, ScoredCandidate, compute_distance};

const MAX_LEVEL: usize = 8;
const MAX_NEIGHBORS: usize = 12;
const EF_CONSTRUCTION: usize = 64;
const MAX_EF_SEARCH: usize = 512;

#[derive(Clone, Debug)]
struct RankedCandidate(ScoredCandidate);

impl PartialEq for RankedCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for RankedCandidate {}
impl PartialOrd for RankedCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for RankedCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .distance
            .total_cmp(&other.0.distance)
            .then_with(|| self.0.index.cmp(&other.0.index))
    }
}

#[derive(Clone, Debug)]
struct Node {
    neighbors: Vec<Vec<usize>>,
}

/// Bounded graph statistics used by acceptance and safe observability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BuildStats {
    pub(crate) node_count: usize,
    pub(crate) edge_count: usize,
    pub(crate) maximum_level: usize,
}

/// Deterministic HNSW graph over a caller-retained vector slice.
pub(crate) struct HnswIndex {
    nodes: Vec<Node>,
    entry: usize,
    maximum_level: usize,
    metric: DistanceMetric,
}

impl HnswIndex {
    #[cfg(test)]
    pub(crate) fn build(
        vectors: &[&CanonicalVector],
        metric: DistanceMetric,
    ) -> Result<Self, NearestError> {
        Self::build_while(vectors, metric, || true).map(|index| index.expect("uncancelled build"))
    }

    pub(crate) fn build_while(
        vectors: &[&CanonicalVector],
        metric: DistanceMetric,
        mut retain: impl FnMut() -> bool,
    ) -> Result<Option<Self>, NearestError> {
        let Some(first) = vectors.first() else {
            return Ok(Some(Self {
                nodes: Vec::new(),
                entry: 0,
                maximum_level: 0,
                metric,
            }));
        };
        for (index, vector) in vectors.iter().enumerate() {
            if vector.dimension() != first.dimension() {
                return Err(NearestError::DimensionMismatch {
                    query: first.dimension(),
                    candidate: vector.dimension(),
                    index,
                });
            }
        }

        let mut index = Self {
            nodes: Vec::with_capacity(vectors.len()),
            entry: 0,
            maximum_level: 0,
            metric,
        };
        for node_index in 0..vectors.len() {
            if !retain() {
                return Ok(None);
            }
            index.insert(node_index, vectors);
        }
        Ok(Some(index))
    }

    #[cfg(test)]
    pub(crate) fn retained_buffer_allocations(&self) -> usize {
        usize::from(self.nodes.capacity() != 0)
            + self
                .nodes
                .iter()
                .map(|node| {
                    usize::from(node.neighbors.capacity() != 0)
                        + node
                            .neighbors
                            .iter()
                            .filter(|neighbors| neighbors.capacity() != 0)
                            .count()
                })
                .sum::<usize>()
    }

    fn insert(&mut self, node_index: usize, vectors: &[&CanonicalVector]) {
        let level = deterministic_level(node_index);
        self.nodes.push(Node {
            neighbors: (0..=level).map(|_| Vec::new()).collect(),
        });
        if node_index == 0 {
            self.entry = 0;
            self.maximum_level = level;
            return;
        }

        let mut entry = self.entry;
        for layer in ((level + 1)..=self.maximum_level).rev() {
            entry = self.greedy_closest(vectors[node_index], vectors, entry, layer);
        }
        for layer in (0..=level.min(self.maximum_level)).rev() {
            let candidates = self.search_layer(
                vectors[node_index],
                vectors,
                entry,
                layer,
                EF_CONSTRUCTION,
                node_index,
            );
            let selected = candidates
                .iter()
                .take(MAX_NEIGHBORS)
                .map(|candidate| candidate.index)
                .collect::<Vec<_>>();
            self.nodes[node_index].neighbors[layer] = selected.clone();
            for neighbor in selected {
                self.nodes[neighbor].neighbors[layer].push(node_index);
                self.prune_neighbors(neighbor, layer, vectors);
            }
            if let Some(best) = candidates.first() {
                entry = best.index;
            }
        }
        if level > self.maximum_level {
            self.entry = node_index;
            self.maximum_level = level;
        }
    }

    fn prune_neighbors(&mut self, node: usize, layer: usize, vectors: &[&CanonicalVector]) {
        if self.nodes[node].neighbors[layer].len() <= MAX_NEIGHBORS {
            return;
        }
        let neighbors = std::mem::take(&mut self.nodes[node].neighbors[layer]);
        let mut scored = neighbors
            .into_iter()
            .map(|index| {
                RankedCandidate(ScoredCandidate {
                    index,
                    distance: compute_distance(
                        vectors[node].components(),
                        vectors[index].components(),
                        self.metric,
                    ),
                })
            })
            .collect::<Vec<_>>();
        scored.sort_unstable();
        scored.truncate(MAX_NEIGHBORS);
        self.nodes[node].neighbors[layer] = scored
            .into_iter()
            .map(|candidate| candidate.0.index)
            .collect();
    }

    fn greedy_closest(
        &self,
        query: &CanonicalVector,
        vectors: &[&CanonicalVector],
        mut current: usize,
        layer: usize,
    ) -> usize {
        let mut current_distance = compute_distance(
            query.components(),
            vectors[current].components(),
            self.metric,
        );
        loop {
            let mut improved = false;
            if let Some(neighbors) = self.nodes[current].neighbors.get(layer) {
                for neighbor in neighbors {
                    let distance = compute_distance(
                        query.components(),
                        vectors[*neighbor].components(),
                        self.metric,
                    );
                    if distance.total_cmp(&current_distance).is_lt()
                        || (distance == current_distance && *neighbor < current)
                    {
                        current = *neighbor;
                        current_distance = distance;
                        improved = true;
                    }
                }
            }
            if !improved {
                return current;
            }
        }
    }

    fn search_layer(
        &self,
        query: &CanonicalVector,
        vectors: &[&CanonicalVector],
        entry: usize,
        layer: usize,
        ef: usize,
        node_limit: usize,
    ) -> Vec<ScoredCandidate> {
        let mut visited = vec![false; node_limit];
        if entry >= node_limit {
            return Vec::new();
        }
        let capacity = ef.min(node_limit);
        if capacity == 0 {
            return Vec::new();
        }
        visited[entry] = true;
        let first = RankedCandidate(ScoredCandidate {
            index: entry,
            distance: compute_distance(
                query.components(),
                vectors[entry].components(),
                self.metric,
            ),
        });
        let mut pending = BinaryHeap::from([Reverse(first.clone())]);
        let mut nearest = BinaryHeap::from([first]);
        // Preserve the existing fixed expansion budget and discovery semantics:
        // frontier entries are not discarded when they leave the best-result set.
        for _ in 0..capacity {
            let Some(Reverse(candidate)) = pending.pop() else {
                break;
            };
            if let Some(neighbors) = self.nodes[candidate.0.index].neighbors.get(layer) {
                for neighbor in neighbors {
                    if *neighbor >= node_limit || visited[*neighbor] {
                        continue;
                    }
                    visited[*neighbor] = true;
                    let scored = RankedCandidate(ScoredCandidate {
                        index: *neighbor,
                        distance: compute_distance(
                            query.components(),
                            vectors[*neighbor].components(),
                            self.metric,
                        ),
                    });
                    pending.push(Reverse(scored.clone()));
                    if nearest.len() < capacity {
                        nearest.push(scored);
                    } else if let Some(mut worst) = nearest.peek_mut()
                        && scored < *worst
                    {
                        *worst = scored;
                    }
                }
            }
        }
        nearest
            .into_sorted_vec()
            .into_iter()
            .map(|candidate| candidate.0)
            .collect()
    }

    #[cfg(test)]
    fn reference_search_layer(
        &self,
        query: &CanonicalVector,
        vectors: &[&CanonicalVector],
        entry: usize,
        layer: usize,
        ef: usize,
        node_limit: usize,
    ) -> Vec<ScoredCandidate> {
        let mut visited = vec![false; node_limit];
        let mut discovered = Vec::new();
        if entry >= node_limit {
            return discovered;
        }
        visited[entry] = true;
        discovered.push(ScoredCandidate {
            index: entry,
            distance: compute_distance(
                query.components(),
                vectors[entry].components(),
                self.metric,
            ),
        });
        let mut expanded = vec![false; node_limit];
        for _ in 0..ef.min(node_limit) {
            let Some((position, candidate)) = discovered
                .iter()
                .enumerate()
                .filter(|(_, candidate)| !expanded[candidate.index])
                .min_by(|(_, left), (_, right)| {
                    left.distance
                        .total_cmp(&right.distance)
                        .then_with(|| left.index.cmp(&right.index))
                })
                .map(|(position, candidate)| (position, candidate.index))
            else {
                break;
            };
            let _ = position;
            expanded[candidate] = true;
            if let Some(neighbors) = self.nodes[candidate].neighbors.get(layer) {
                for neighbor in neighbors {
                    if *neighbor >= node_limit || visited[*neighbor] {
                        continue;
                    }
                    visited[*neighbor] = true;
                    discovered.push(ScoredCandidate {
                        index: *neighbor,
                        distance: compute_distance(
                            query.components(),
                            vectors[*neighbor].components(),
                            self.metric,
                        ),
                    });
                }
            }
        }
        discovered.sort_by(|left, right| {
            left.distance
                .total_cmp(&right.distance)
                .then_with(|| left.index.cmp(&right.index))
        });
        discovered.truncate(ef.min(node_limit));
        discovered
    }

    pub(crate) fn search(
        &self,
        query: &CanonicalVector,
        vectors: &[&CanonicalVector],
        k: u32,
        recall_target_bps: u32,
    ) -> Result<Vec<ScoredCandidate>, NearestError> {
        if self.nodes.is_empty() || k == 0 {
            return Ok(Vec::new());
        }
        if query.dimension() != vectors[0].dimension() {
            return Err(NearestError::DimensionMismatch {
                query: query.dimension(),
                candidate: vectors[0].dimension(),
                index: 0,
            });
        }
        let mut entry = self.entry;
        for layer in (1..=self.maximum_level).rev() {
            entry = self.greedy_closest(query, vectors, entry, layer);
        }
        let quality_ef = vectors
            .len()
            .saturating_mul(recall_target_bps as usize)
            .div_ceil(10_000);
        let ef = quality_ef
            .max(k as usize * 8)
            .clamp(32, MAX_EF_SEARCH)
            .min(vectors.len());
        let mut result = self.search_layer(query, vectors, entry, 0, ef, vectors.len());
        result.truncate(k as usize);
        Ok(result)
    }

    pub(crate) fn stats(&self) -> BuildStats {
        BuildStats {
            node_count: self.nodes.len(),
            edge_count: self
                .nodes
                .iter()
                .flat_map(|node| &node.neighbors)
                .map(Vec::len)
                .sum(),
            maximum_level: self.maximum_level,
        }
    }
}

fn deterministic_level(index: usize) -> usize {
    let mut value = (index as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^= value >> 31;
    ((value.trailing_zeros() as usize) / 2).min(MAX_LEVEL)
}

#[cfg(test)]
mod tests {
    use rand::{Rng, SeedableRng, rngs::StdRng};

    use super::*;
    use crate::nearest::exact_knn;

    const RECALL_TARGET_BPS: u32 = 9_500;

    fn random_vector(rng: &mut StdRng, dimension: usize) -> CanonicalVector {
        CanonicalVector::new(
            (0..dimension)
                .map(|_| rng.gen_range(-1.0_f32..1.0_f32))
                .collect(),
        )
        .expect("bounded finite vector")
    }

    fn recall_bps(approximate: &[ScoredCandidate], exact: &[ScoredCandidate]) -> u32 {
        if exact.is_empty() {
            return 10_000;
        }
        let overlap = approximate
            .iter()
            .filter(|candidate| exact.iter().any(|item| item.index == candidate.index))
            .count();
        u32::try_from(overlap * 10_000 / exact.len()).expect("recall is bounded")
    }

    // req: VEC-006, VEC-007
    #[test]
    fn heap_traversal_preserves_fixed_budget_discovery_and_ties() {
        let mut rng = StdRng::seed_from_u64(0x5940_beef);
        let mut vectors = (0..96)
            .map(|_| random_vector(&mut rng, 8))
            .collect::<Vec<_>>();
        vectors.extend(std::iter::repeat_n(vectors[0].clone(), 16));
        let refs = vectors.iter().collect::<Vec<_>>();
        for metric in [
            DistanceMetric::Euclidean,
            DistanceMetric::Cosine,
            DistanceMetric::DotProduct,
        ] {
            let index = HnswIndex::build(&refs, metric).unwrap();
            for ef in [0, 1, 2, 12, 64, 112] {
                for entry in [0, 12, 100, 112] {
                    let actual = index.search_layer(&vectors[0], &refs, entry, 0, ef, refs.len());
                    let expected =
                        index.reference_search_layer(&vectors[0], &refs, entry, 0, ef, refs.len());
                    assert_eq!(
                        actual
                            .iter()
                            .map(|v| (v.index, v.distance.to_bits()))
                            .collect::<Vec<_>>(),
                        expected
                            .iter()
                            .map(|v| (v.index, v.distance.to_bits()))
                            .collect::<Vec<_>>()
                    );
                }
            }
        }
    }

    #[test]
    fn randomized_histories_meet_declared_recall_against_exact_at_matched_frontiers() {
        let mut rng = StdRng::seed_from_u64(0x5940_9500);
        let mut vectors = Vec::new();

        for frontier in 1..=8 {
            vectors.extend((0..48).map(|_| random_vector(&mut rng, 12)));
            let candidates = vectors.iter().collect::<Vec<_>>();
            let index = HnswIndex::build(&candidates, DistanceMetric::Euclidean)
                .expect("uniform dimensions");

            for probe in 0..12 {
                let query = random_vector(&mut rng, 12);
                let exact = exact_knn(&query, &candidates, DistanceMetric::Euclidean, 20)
                    .expect("exact reference");
                let approximate = index
                    .search(&query, &candidates, 20, RECALL_TARGET_BPS)
                    .expect("approximate search");
                assert!(
                    recall_bps(&approximate, &exact) >= RECALL_TARGET_BPS,
                    "frontier {frontier}, probe {probe} fell below declared recall"
                );
            }
        }
    }

    #[test]
    fn maintenance_rebuild_at_a_frozen_frontier_preserves_recall_and_statistics() {
        let mut rng = StdRng::seed_from_u64(0x5940_cafe);
        let vectors = (0..384)
            .map(|_| random_vector(&mut rng, 16))
            .collect::<Vec<_>>();
        let candidates = vectors.iter().collect::<Vec<_>>();
        let before = HnswIndex::build(&candidates, DistanceMetric::Cosine).expect("before");
        let after = HnswIndex::build(&candidates, DistanceMetric::Cosine).expect("after");
        assert_eq!(before.stats(), after.stats());

        for probe in 0..32 {
            let query = random_vector(&mut rng, 16);
            let exact = exact_knn(&query, &candidates, DistanceMetric::Cosine, 10)
                .expect("exact reference");
            let old_result = before
                .search(&query, &candidates, 10, RECALL_TARGET_BPS)
                .expect("old graph");
            let rebuilt_result = after
                .search(&query, &candidates, 10, RECALL_TARGET_BPS)
                .expect("rebuilt graph");
            assert_eq!(
                old_result.iter().map(|item| item.index).collect::<Vec<_>>(),
                rebuilt_result
                    .iter()
                    .map(|item| item.index)
                    .collect::<Vec<_>>(),
                "probe {probe}: deterministic rebuild changed result shape"
            );
            assert!(
                recall_bps(&rebuilt_result, &exact) >= RECALL_TARGET_BPS,
                "probe {probe}: rebuilt graph regressed below the declared recall"
            );
        }
    }
}
