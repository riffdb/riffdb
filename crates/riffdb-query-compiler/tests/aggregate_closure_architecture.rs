//! Repository-wide closure inventory for ADR-0152.

use std::collections::BTreeSet;

use riffdb_types::{CanonicalValue, aggregate_semantic_registry_v1, encode_canonical_value};

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExactPartial {
    count: u64,
    sum: i128,
    min: Option<i64>,
    max: Option<i64>,
    present: u64,
    distinct: BTreeSet<Vec<u8>>,
    distinct_present: BTreeSet<Vec<u8>>,
    mean_total: i128,
    mean_count: u64,
    any: bool,
    all: bool,
}

impl ExactPartial {
    fn empty() -> Self {
        Self {
            count: 0,
            sum: 0,
            min: None,
            max: None,
            present: 0,
            distinct: BTreeSet::new(),
            distinct_present: BTreeSet::new(),
            mean_total: 0,
            mean_count: 0,
            any: false,
            all: true,
        }
    }

    fn contribute(&mut self, number: i64, label: &CanonicalValue, enabled: bool) {
        self.count = self.count.checked_add(1).unwrap();
        self.sum = self.sum.checked_add(i128::from(number)).unwrap();
        self.min = Some(self.min.map_or(number, |value| value.min(number)));
        self.max = Some(self.max.map_or(number, |value| value.max(number)));
        let encoded = encode_canonical_value(label).unwrap();
        self.distinct.insert(encoded.clone());
        if !matches!(label, CanonicalValue::Null) {
            self.present = self.present.checked_add(1).unwrap();
            self.distinct_present.insert(encoded);
        }
        self.mean_total = self.mean_total.checked_add(i128::from(number)).unwrap();
        self.mean_count = self.mean_count.checked_add(1).unwrap();
        self.any |= enabled;
        self.all &= enabled;
    }

    fn merge(&mut self, other: Self) {
        self.count = self.count.checked_add(other.count).unwrap();
        self.sum = self.sum.checked_add(other.sum).unwrap();
        self.min = match (self.min, other.min) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        };
        self.max = match (self.max, other.max) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (left, right) => left.or(right),
        };
        self.present = self.present.checked_add(other.present).unwrap();
        self.distinct.extend(other.distinct);
        self.distinct_present.extend(other.distinct_present);
        self.mean_total = self.mean_total.checked_add(other.mean_total).unwrap();
        self.mean_count = self.mean_count.checked_add(other.mean_count).unwrap();
        self.any |= other.any;
        self.all &= other.all;
    }
}

fn fold(rows: &[(i64, CanonicalValue, bool)]) -> ExactPartial {
    let mut state = ExactPartial::empty();
    for (number, label, enabled) in rows {
        state.contribute(*number, label, *enabled);
    }
    state
}

#[test]
fn exact_partial_states_obey_every_registered_merge_identity() {
    let rows = vec![
        (3, CanonicalValue::Null, false),
        (5, CanonicalValue::string("alpha").unwrap(), true),
        (7, CanonicalValue::string("alpha").unwrap(), true),
        (-2, CanonicalValue::string("beta").unwrap(), false),
    ];
    let expected = fold(&rows);
    assert_eq!(fold(&[]), ExactPartial::empty());

    for chunk_size in 1..=rows.len() + 1 {
        let mut merged = ExactPartial::empty();
        for chunk in rows.chunks(chunk_size) {
            merged.merge(fold(chunk));
        }
        assert_eq!(merged, expected, "merge partition {chunk_size}");
    }

    let mut reversed = rows.clone();
    reversed.reverse();
    assert_eq!(fold(&reversed), expected);
    assert_eq!(expected.count, 4);
    assert_eq!(expected.present, 3);
    assert_eq!(expected.distinct.len(), 3);
    assert_eq!(expected.distinct_present.len(), 2);
    assert_eq!((expected.mean_total, expected.mean_count), (13, 4));
    assert!(expected.any);
    assert!(!expected.all);
}

#[test]
fn every_exact_semantic_has_cross_plane_and_generated_evidence() {
    assert_eq!(aggregate_semantic_registry_v1().len(), 11);

    let executor = include_str!("../../riffdb-query-executor/src/lib.rs");
    let columnar = include_str!("../../riffdb-columnar/tests/acceptance.rs");
    let policy = include_str!("../../riffdb-columnar/tests/cp2a.rs");
    let exact_provider = include_str!("../../riffdb-projection/tests/exact_text_provider.rs");
    let module = include_str!("../../riffdb-query-module/tests/module_codec.rs");
    let grpc = include_str!("../../riffdb-api-grpc/src/conversion.rs");
    let evidence = include_str!("../../../docs/architecture/WP-696-AGGREGATE-CLOSURE.md");

    for required in [
        "exact_aggregate_core_freezes_novalue_empty_and_mean_state",
        "distinct_budget_exhaustion_withholds_the_complete_aggregate",
    ] {
        assert!(executor.contains(required));
    }
    for required in [
        "acceptance_query_range_sort_limit_aggregates_group_by_budgets",
        "acceptance_randomized_histories_equivalence",
        "acceptance_compaction_result_invariant",
        "acceptance_checkpoint_recover_and_replay",
    ] {
        assert!(columnar.contains(required));
    }
    assert!(policy.contains("protected_snapshot_admission_precedes_scan_budget_and_aggregate"));
    assert!(exact_provider.contains("exact_count_precedes_direct_ordinal_window"));
    assert!(
        module.contains("exact_aggregate_core_uses_v11_and_generates_one_structural_mean_schema")
    );
    assert!(grpc.contains("symbolic_exact_mean_crosses_the_public_record_carrier_exactly"));

    for semantic in aggregate_semantic_registry_v1() {
        assert!(evidence.contains(semantic.source_spelling()));
    }
}

#[test]
fn deferred_families_bridges_callbacks_and_frameworks_remain_absent() {
    let registry = include_str!("../../riffdb-types/src/aggregate.rs");
    let executor_manifest = include_str!("../../riffdb-query-executor/Cargo.toml");
    let columnar_manifest = include_str!("../../riffdb-columnar/Cargo.toml");
    let manifests = format!("{executor_manifest}\n{columnar_manifest}").to_ascii_lowercase();
    for forbidden_dependency in ["datafusion", "arrow", "roaring", "hyperloglog", "tdigest"] {
        assert!(!manifests.contains(forbidden_dependency));
    }
    for forbidden_runtime in [
        "AggregateSemanticIdentityV1::Variance",
        "AggregateSemanticIdentityV1::Percentile",
        "AggregateSemanticIdentityV1::ApproximateDistinct",
        "AggregateSemanticIdentityV1::UserDefined",
    ] {
        assert!(!registry.contains(forbidden_runtime));
    }

    let provider = include_str!("../../riffdb-types/src/projection_provider.rs");
    assert!(!provider.contains("BetterAuth"));
    assert!(!provider.contains("OpenFga"));
    assert!(!provider.contains("callback"));
    assert!(!provider.contains("bridge"));
}
