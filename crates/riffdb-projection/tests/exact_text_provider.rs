//! Exact text provider reference, rebuild, and recovery conformance.

use riffdb_projection::{
    ExactTextIndexMutationV1, ExactTextIndexMutationV2, ExactTextPartitionIndexV1,
    ExactTextPartitionIndexV2, ExactTextPartitionIndexV3, ExactTextProviderErrorV1,
};
use riffdb_types::{
    CanonicalRecord, CanonicalValue, CommitSequence, EntityKey, EntityKeyBuilder, EntityKeyHash,
    EntityTypeId, ExactTextFieldValueV1, ExactTextOperatorV1, ExactTextOrderV1, ExactTextProfileV1,
    FieldId, PartitionKeyHash, ProjectionGeneration, hash_entity_key,
};
use std::collections::BTreeMap;
use std::num::NonZeroU16;

const CHECKPOINT_FIXTURE: &str =
    include_str!("../../../fixtures/projection/exact-text-provider-state-v1.txt");
const CHECKPOINT_FIXTURE_V2: &str =
    include_str!("../../../fixtures/projection/exact-text-provider-state-v2.txt");

fn seq(value: u64) -> CommitSequence {
    CommitSequence::new(value).unwrap()
}

fn row(value: u8) -> EntityKeyHash {
    EntityKeyHash::from_bytes([value; 32])
}

fn key(value: u32) -> EntityKey {
    let mut builder = EntityKeyBuilder::new(EntityTypeId::first());
    builder.push_u32(value).unwrap();
    builder.finish().unwrap()
}

fn output(value: u64) -> CanonicalRecord {
    CanonicalRecord::new(vec![(FieldId::first(), CanonicalValue::U64(value))]).unwrap()
}

fn partition(value: u8) -> PartitionKeyHash {
    PartitionKeyHash::from_bytes([value; 32])
}

#[test]
fn indexed_results_match_oracle_across_delete_compact_rebuild_and_recovery() {
    let profile = ExactTextProfileV1::BinaryUtf8V1;
    let mut index = ExactTextPartitionIndexV1::new(
        partition(1),
        ProjectionGeneration::new(1).unwrap(),
        profile,
    );
    index
        .apply(
            seq(1),
            &[
                ExactTextIndexMutationV1::upsert(row(1), "alpha café").unwrap(),
                ExactTextIndexMutationV1::upsert(row(2), "beta café").unwrap(),
                ExactTextIndexMutationV1::upsert(row(3), "東京駅").unwrap(),
            ],
        )
        .unwrap();
    let needle = profile.bind_needle("café").unwrap();
    assert_eq!(
        index.lookup(ExactTextOperatorV1::Contains, &needle),
        &[row(1), row(2)]
    );

    index
        .apply(seq(2), &[ExactTextIndexMutationV1::delete(row(1))])
        .unwrap();
    index.compact();
    assert_eq!(
        index.lookup(ExactTextOperatorV1::Contains, &needle),
        &[row(2)]
    );

    let bytes = index.to_checkpoint_bytes().unwrap();
    let recovered = ExactTextPartitionIndexV1::from_checkpoint_bytes(&bytes).unwrap();
    assert_eq!(recovered, index);
    assert_eq!(
        recovered.lookup(ExactTextOperatorV1::Contains, &needle),
        &[row(2)]
    );

    let rebuilt = ExactTextPartitionIndexV1::rebuild(
        partition(1),
        ProjectionGeneration::new(2).unwrap(),
        seq(2),
        profile,
        index.rows(),
    )
    .unwrap();
    assert_eq!(
        rebuilt.lookup(ExactTextOperatorV1::Contains, &needle),
        &[row(2)]
    );
}

#[test]
fn partitions_and_format_versions_fail_closed() {
    let profile = ExactTextProfileV1::BinaryUtf8V1;
    let mut first = ExactTextPartitionIndexV1::new(
        partition(1),
        ProjectionGeneration::new(1).unwrap(),
        profile,
    );
    first
        .apply(
            seq(1),
            &[ExactTextIndexMutationV1::upsert(row(1), "secret").unwrap()],
        )
        .unwrap();
    let second = ExactTextPartitionIndexV1::new(
        partition(2),
        ProjectionGeneration::new(1).unwrap(),
        profile,
    );
    let needle = profile.bind_needle("secret").unwrap();
    assert!(
        second
            .lookup(ExactTextOperatorV1::Contains, &needle)
            .is_empty()
    );

    let mut bytes = first.to_checkpoint_bytes().unwrap();
    bytes[5] = 2;
    assert_eq!(
        ExactTextPartitionIndexV1::from_checkpoint_bytes(&bytes),
        Err(ExactTextProviderErrorV1::UnsupportedFormat)
    );
}

#[test]
fn randomized_incremental_writes_match_independent_truth_table() {
    let profile = ExactTextProfileV1::BinaryUtf8V1;
    let mut index = ExactTextPartitionIndexV1::new(
        partition(9),
        ProjectionGeneration::new(1).unwrap(),
        profile,
    );
    let corpus = [
        "alpha",
        "beta",
        "café",
        "東京駅",
        "a🙂b",
        "Straße",
        "wild*card",
    ];
    let needles = ["a", "é", "駅", "🙂", "*", "beta", "strasse"];
    let operators = [
        ExactTextOperatorV1::Equals,
        ExactTextOperatorV1::StartsWith,
        ExactTextOperatorV1::EndsWith,
        ExactTextOperatorV1::Contains,
    ];
    let mut oracle = BTreeMap::new();
    let mut state = 0x51c2_c003_u64;
    for epoch in 1..=300 {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let row_id = ((state >> 32) % 31 + 1) as u8;
        let mutation = if state & 7 == 0 {
            oracle.remove(&row(row_id));
            ExactTextIndexMutationV1::delete(row(row_id))
        } else {
            let value = corpus[usize::try_from(state).unwrap() % corpus.len()];
            oracle.insert(row(row_id), value.to_owned());
            ExactTextIndexMutationV1::upsert(row(row_id), value).unwrap()
        };
        index.apply(seq(epoch), &[mutation]).unwrap();

        let needle = profile
            .bind_needle(needles[usize::try_from(state >> 8).unwrap() % needles.len()])
            .unwrap();
        for operator in operators {
            let expected: Vec<_> = oracle
                .iter()
                .filter_map(|(row, value)| {
                    profile
                        .matches(operator, ExactTextFieldValueV1::Value(value), &needle)
                        .then_some(*row)
                })
                .collect::<Vec<_>>();
            let mut expected = expected;
            expected.sort_unstable_by(|left, right| {
                oracle[left]
                    .as_bytes()
                    .cmp(oracle[right].as_bytes())
                    .then_with(|| left.cmp(right))
            });
            assert_eq!(index.lookup(operator, &needle), expected);
        }
        if epoch % 37 == 0 {
            index.compact();
            let recovered = ExactTextPartitionIndexV1::from_checkpoint_bytes(
                &index.to_checkpoint_bytes().unwrap(),
            )
            .unwrap();
            let rebuilt = ExactTextPartitionIndexV1::rebuild(
                partition(9),
                ProjectionGeneration::new(2).unwrap(),
                seq(epoch),
                profile,
                index.rows(),
            )
            .unwrap();
            for operator in operators {
                assert_eq!(
                    recovered.lookup(operator, &needle),
                    index.lookup(operator, &needle)
                );
                assert_eq!(
                    rebuilt.lookup(operator, &needle),
                    index.lookup(operator, &needle)
                );
            }
        }
    }
}

#[test]
fn exact_count_precedes_direct_ordinal_window_in_both_total_orders() {
    let profile = ExactTextProfileV1::BinaryUtf8V1;
    let mut index = ExactTextPartitionIndexV1::new(
        partition(4),
        ProjectionGeneration::new(3).unwrap(),
        profile,
    );
    index
        .apply(
            seq(9),
            &[
                ExactTextIndexMutationV1::upsert(row(4), "beta match").unwrap(),
                ExactTextIndexMutationV1::upsert(row(3), "alpha match").unwrap(),
                ExactTextIndexMutationV1::upsert(row(2), "beta match").unwrap(),
                ExactTextIndexMutationV1::upsert(row(1), "gamma").unwrap(),
            ],
        )
        .unwrap();
    let needle = profile.bind_needle("match").unwrap();
    let limit = NonZeroU16::new(2).unwrap();

    let ascending = index
        .result_page(
            ExactTextOperatorV1::Contains,
            &needle,
            ExactTextOrderV1::ValueAscEntityKey,
            1,
            limit,
        )
        .unwrap();
    assert_eq!(ascending.exact_total(), 3);
    assert_eq!(ascending.rows(), &[row(2), row(4)]);

    let descending = index
        .result_page(
            ExactTextOperatorV1::Contains,
            &needle,
            ExactTextOrderV1::ValueDescEntityKey,
            0,
            limit,
        )
        .unwrap();
    assert_eq!(descending.exact_total(), 3);
    assert_eq!(descending.rows(), &[row(2), row(4)]);

    for offset in [3, 4, u32::MAX] {
        let empty = index
            .result_page(
                ExactTextOperatorV1::Contains,
                &needle,
                ExactTextOrderV1::ValueAscEntityKey,
                offset,
                limit,
            )
            .unwrap();
        assert_eq!(empty.exact_total(), 3);
        assert!(empty.rows().is_empty());
    }
}

#[test]
fn provider_state_v1_matches_the_frozen_compatibility_fixture() {
    let profile = ExactTextProfileV1::BinaryUtf8V1;
    let mut index = ExactTextPartitionIndexV1::new(
        partition(1),
        ProjectionGeneration::new(1).unwrap(),
        profile,
    );
    index
        .apply(
            seq(1),
            &[ExactTextIndexMutationV1::upsert(row(2), "é").unwrap()],
        )
        .unwrap();
    let expected = CHECKPOINT_FIXTURE
        .lines()
        .find_map(|line| line.strip_prefix("bytes_hex="))
        .map(decode_hex)
        .unwrap();
    assert_eq!(index.to_checkpoint_bytes().unwrap(), expected);
}

#[test]
fn activated_v2_retains_typed_rows_and_matches_the_frozen_checkpoint() {
    let profile = ExactTextProfileV1::BinaryUtf8V1;
    let mut index = ExactTextPartitionIndexV2::new(
        partition(0x31),
        ProjectionGeneration::new(9).unwrap(),
        profile,
    );
    index
        .apply(
            seq(17),
            &[
                ExactTextIndexMutationV2::upsert(key(2), "beta", output(22)).unwrap(),
                ExactTextIndexMutationV2::upsert(key(1), "alpha", output(11)).unwrap(),
            ],
        )
        .unwrap();
    let needle = profile.bind_needle("a").unwrap();
    let page = index
        .result_page(
            ExactTextOperatorV1::Contains,
            &needle,
            ExactTextOrderV1::ValueAscEntityKey,
            0,
            NonZeroU16::new(2).unwrap(),
        )
        .unwrap();
    assert_eq!(page.exact_total(), 2);
    assert_eq!(page.rows()[0].key(), &key(1));
    assert_eq!(page.rows()[0].output(), &output(11));
    assert_eq!(page.rows()[1].key(), &key(2));
    assert_eq!(page.rows()[1].output(), &output(22));

    let checkpoint = index.to_checkpoint_bytes().unwrap();
    let actual = checkpoint
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(actual, CHECKPOINT_FIXTURE_V2.trim());
    assert_eq!(
        ExactTextPartitionIndexV2::from_checkpoint_bytes(&checkpoint).unwrap(),
        index
    );
    assert_eq!(
        ExactTextPartitionIndexV1::from_checkpoint_bytes(&checkpoint),
        Err(ExactTextProviderErrorV1::UnsupportedFormat)
    );
    assert_eq!(
        ExactTextPartitionIndexV2::from_checkpoint_bytes(
            &ExactTextPartitionIndexV1::rebuild(
                partition(1),
                ProjectionGeneration::new(1).unwrap(),
                seq(1),
                profile,
                &BTreeMap::from([(row(1), "one".to_owned())]),
            )
            .unwrap()
            .to_checkpoint_bytes()
            .unwrap(),
        ),
        Err(ExactTextProviderErrorV1::UnsupportedFormat)
    );
}

#[test]
fn activated_v2_uses_canonical_entity_keys_not_hashes_as_the_tie_breaker() {
    let candidates = (1..=64).map(key).collect::<Vec<_>>();
    let (first, second) = candidates
        .iter()
        .enumerate()
        .flat_map(|(left, first)| {
            candidates
                .iter()
                .skip(left + 1)
                .map(move |second| (first, second))
        })
        .find(|(first, second)| {
            first.as_bytes() < second.as_bytes()
                && hash_entity_key(first.as_bytes()) > hash_entity_key(second.as_bytes())
        })
        .expect("the deterministic corpus contains a hash/key order inversion");
    let mut index = ExactTextPartitionIndexV2::new(
        partition(0x32),
        ProjectionGeneration::new(10).unwrap(),
        ExactTextProfileV1::BinaryUtf8V1,
    );
    index
        .apply(
            seq(18),
            &[
                ExactTextIndexMutationV2::upsert(first.clone(), "same", output(1)).unwrap(),
                ExactTextIndexMutationV2::upsert(second.clone(), "same", output(2)).unwrap(),
            ],
        )
        .unwrap();

    let page = index
        .result_page(
            ExactTextOperatorV1::Equals,
            &ExactTextProfileV1::BinaryUtf8V1
                .bind_needle("same")
                .unwrap(),
            ExactTextOrderV1::ValueAscEntityKey,
            0,
            NonZeroU16::new(2).unwrap(),
        )
        .unwrap();
    assert_eq!(page.rows()[0].key(), first);
    assert_eq!(page.rows()[1].key(), second);

    index.compact().unwrap();
    let recovered =
        ExactTextPartitionIndexV2::from_checkpoint_bytes(&index.to_checkpoint_bytes().unwrap())
            .unwrap();
    let recovered_page = recovered
        .result_page(
            ExactTextOperatorV1::Equals,
            &ExactTextProfileV1::BinaryUtf8V1
                .bind_needle("same")
                .unwrap(),
            ExactTextOrderV1::ValueDescEntityKey,
            0,
            NonZeroU16::new(2).unwrap(),
        )
        .unwrap();
    assert_eq!(recovered_page.rows()[0].key(), first);
    assert_eq!(recovered_page.rows()[1].key(), second);
}

#[test]
fn filtered_v3_counts_and_selects_ordinals_inside_the_typed_partition() {
    let rows = BTreeMap::from([
        (
            key(1),
            (
                "alpha match".to_owned(),
                CanonicalValue::Bool(true),
                output(1),
            ),
        ),
        (
            key(2),
            (
                "beta match".to_owned(),
                CanonicalValue::Bool(false),
                output(2),
            ),
        ),
        (
            key(3),
            (
                "gamma match".to_owned(),
                CanonicalValue::Bool(true),
                output(3),
            ),
        ),
    ]);
    let index = ExactTextPartitionIndexV3::rebuild(
        partition(0x33),
        ProjectionGeneration::new(11).unwrap(),
        seq(19),
        ExactTextProfileV1::BinaryUtf8V1,
        FieldId::new(2).unwrap(),
        &rows,
    )
    .unwrap();
    let needle = ExactTextProfileV1::BinaryUtf8V1
        .bind_needle("match")
        .unwrap();
    let filtered = index
        .result_page(
            ExactTextOperatorV1::Contains,
            &needle,
            Some(&CanonicalValue::Bool(true)),
            ExactTextOrderV1::ValueDescEntityKey,
            1,
            NonZeroU16::new(1).unwrap(),
        )
        .unwrap();
    assert_eq!(filtered.exact_total(), 2);
    assert_eq!(filtered.rows()[0].key(), &key(1));

    let absent = index
        .result_page(
            ExactTextOperatorV1::Contains,
            &needle,
            Some(&CanonicalValue::Null),
            ExactTextOrderV1::ValueAscEntityKey,
            0,
            NonZeroU16::new(10).unwrap(),
        )
        .unwrap();
    assert_eq!(absent.exact_total(), 0);

    let bytes = index.to_checkpoint_bytes().unwrap();
    let recovered = ExactTextPartitionIndexV3::from_checkpoint_bytes(&bytes).unwrap();
    assert_eq!(recovered, index);
    assert_eq!(
        ExactTextPartitionIndexV2::from_checkpoint_bytes(&bytes),
        Err(ExactTextProviderErrorV1::UnsupportedFormat)
    );
}

#[test]
fn maximum_value_and_one_byte_over_are_checked_before_mutation() {
    let maximum = "a".repeat(riffdb_types::MAX_EXACT_TEXT_VALUE_BYTES_V1);
    assert!(ExactTextIndexMutationV1::upsert(row(1), &maximum).is_ok());
    assert_eq!(
        ExactTextIndexMutationV1::upsert(row(1), &(maximum + "a")),
        Err(ExactTextProviderErrorV1::ValueTooLong)
    );
}

fn decode_hex(value: &str) -> Vec<u8> {
    assert_eq!(value.len() % 2, 0);
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).unwrap();
            u8::from_str_radix(pair, 16).unwrap()
        })
        .collect()
}

/// ADR-0172: the provider matches under the frozen fold when the profile
/// declares it, and a value and a needle fold by the same function.
///
/// This is the half MLflow's ILIKE actually needs -- `contains` and
/// `ends_with` case-insensitively, which the index encoding alone cannot give.
#[test]
fn the_fold_profile_matches_case_and_compatibility_insensitively() {
    use riffdb_projection::{ExactTextIndexMutationV1, ExactTextPartitionIndexV1};
    use riffdb_types::{
        CommitSequence, EntityKeyHash, ExactTextOperatorV1, ExactTextProfileV1, PartitionKeyHash,
        ProjectionGeneration,
    };

    let partition = PartitionKeyHash::from_bytes([7_u8; 32]);
    let generation = ProjectionGeneration::new(1).expect("generation");
    let mut index =
        ExactTextPartitionIndexV1::new(partition, generation, ExactTextProfileV1::UnicodeFoldV1);
    let row = EntityKeyHash::from_bytes([1_u8; 32]);
    index
        .apply(
            CommitSequence::new(1).expect("epoch"),
            &[ExactTextIndexMutationV1::upsert(row, "Straße Ünïcode ﬁle").expect("upsert")],
        )
        .expect("apply");

    let matches = |operator, needle: &str| {
        let bound = ExactTextProfileV1::UnicodeFoldV1
            .bind_needle(needle)
            .expect("needle");
        !index.lookup(operator, &bound).is_empty()
    };

    // Case-insensitive across all three anchored operators.
    assert!(matches(ExactTextOperatorV1::StartsWith, "STRASSE"));
    assert!(matches(ExactTextOperatorV1::Contains, "ÜNÏCODE"));
    assert!(matches(ExactTextOperatorV1::EndsWith, "file"));
    // NFKC compatibility: the stored ligature matches its decomposed spelling.
    assert!(matches(ExactTextOperatorV1::EndsWith, "FILE"));
    // The expanding fold is symmetric: ß in the value matches ss in the needle.
    assert!(matches(ExactTextOperatorV1::StartsWith, "straß"));
    // A genuine non-match is still a non-match.
    assert!(!matches(ExactTextOperatorV1::Contains, "absent"));
}

/// The binary profile must be unaffected, or linking the fold would silently
/// change every existing index's behaviour.
#[test]
fn the_binary_profile_stays_case_sensitive() {
    use riffdb_projection::{ExactTextIndexMutationV1, ExactTextPartitionIndexV1};
    use riffdb_types::{
        CommitSequence, EntityKeyHash, ExactTextOperatorV1, ExactTextProfileV1, PartitionKeyHash,
        ProjectionGeneration,
    };

    let mut index = ExactTextPartitionIndexV1::new(
        PartitionKeyHash::from_bytes([7_u8; 32]),
        ProjectionGeneration::new(1).expect("generation"),
        ExactTextProfileV1::BinaryUtf8V1,
    );
    index
        .apply(
            CommitSequence::new(1).expect("epoch"),
            &[
                ExactTextIndexMutationV1::upsert(EntityKeyHash::from_bytes([1_u8; 32]), "Straße")
                    .expect("upsert"),
            ],
        )
        .expect("apply");

    let matches = |needle: &str| {
        let bound = ExactTextProfileV1::BinaryUtf8V1
            .bind_needle(needle)
            .expect("needle");
        !index
            .lookup(ExactTextOperatorV1::Contains, &bound)
            .is_empty()
    };
    assert!(matches("Straße"), "exact bytes still match");
    assert!(!matches("STRASSE"), "the binary profile is case-sensitive");
}
