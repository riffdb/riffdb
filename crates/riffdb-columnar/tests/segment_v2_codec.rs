//! Independent semantic tests for the inactive ADR-0160 segment V2 candidate.

use riffdb_columnar::{
    ColumnarManifestV2, ColumnarManifestV2Entry, DefinitionFingerprint, MAX_SEGMENT_V2_BYTES,
    MAX_SEGMENT_V2_ROWS, OrgKey, PrimaryKeyBytes, SegmentV2, SegmentV2Cell, SegmentV2Codec,
    SegmentV2Column, SegmentV2Error, SegmentV2Identity, SegmentV2LogicalType, SegmentV2Predicate,
    SegmentV2PruningDecision, SegmentV2SegmentId,
};
use riffdb_types::{
    CanonicalValue, CommitSequence, CurrencyCode, Date, Decimal, DecimalSpec, EntityVersion,
    EnumTypeId, EnumVariantId, FieldId, FrontierPosition, HashDomain, Money, ProjectionGeneration,
    Timestamp, hash,
};

fn identity() -> SegmentV2Identity {
    SegmentV2Identity::new(
        DefinitionFingerprint::from_bytes([0x11; 32]),
        7,
        ProjectionGeneration::first(),
        OrgKey::from_value(&CanonicalValue::Uuid([0x22; 16])).expect("org"),
        SegmentV2SegmentId::from_bytes([0x33; 16]),
        FrontierPosition::BeforeFirst,
        FrontierPosition::AppliedThrough(CommitSequence::new(9).expect("sequence")),
    )
    .expect("identity")
}

fn keys_and_versions(rows: usize) -> (Vec<PrimaryKeyBytes>, Vec<EntityVersion>) {
    let keys = (0..rows)
        .map(|index| {
            PrimaryKeyBytes::from_entity_key_bytes(
                u32::try_from(index).expect("bounded row").to_be_bytes(),
            )
        })
        .collect();
    let versions = (0..rows)
        .map(|index| {
            EntityVersion::new(u64::try_from(index + 1).expect("version")).expect("nonzero")
        })
        .collect();
    (keys, versions)
}

fn one_column_segment(column: SegmentV2Column) -> SegmentV2 {
    let (keys, versions) = keys_and_versions(column.cells().len());
    SegmentV2::new(identity(), keys, versions, vec![column]).expect("segment")
}

#[test]
fn v2_round_trip_is_canonical_and_preserves_optional_states() {
    let status = SegmentV2Column::new(
        FieldId::new(41).expect("field"),
        SegmentV2LogicalType::String,
        vec![
            SegmentV2Cell::Missing,
            SegmentV2Cell::Null,
            SegmentV2Cell::Value(CanonicalValue::string("open").expect("value")),
        ],
    )
    .expect("column");
    let priority = SegmentV2Column::new(
        FieldId::new(42).expect("field"),
        SegmentV2LogicalType::U64,
        vec![
            SegmentV2Cell::Value(CanonicalValue::U64(10)),
            SegmentV2Cell::Value(CanonicalValue::U64(11)),
            SegmentV2Cell::Value(CanonicalValue::U64(12)),
        ],
    )
    .expect("column");
    let segment = SegmentV2::new(
        identity(),
        vec![
            PrimaryKeyBytes::from_entity_key_bytes([1]),
            PrimaryKeyBytes::from_entity_key_bytes([2]),
            PrimaryKeyBytes::from_entity_key_bytes([3]),
        ],
        vec![
            EntityVersion::new(1).expect("version"),
            EntityVersion::new(2).expect("version"),
            EntityVersion::new(3).expect("version"),
        ],
        vec![status, priority],
    )
    .expect("segment");

    let encoded = SegmentV2Codec::encode(&segment).expect("encode");
    let decoded = SegmentV2Codec::decode(&encoded).expect("decode");

    assert_eq!(decoded, segment);
    assert_eq!(
        SegmentV2Codec::encode(&decoded).expect("re-encode"),
        encoded,
        "canonical V2 bytes must not depend on construction history"
    );
}

#[test]
fn v2_round_trip_covers_the_closed_logical_registry() {
    let decimal_spec = DecimalSpec::new(12, 3).expect("decimal type");
    let currency = CurrencyCode::new("USD").expect("currency");
    let enum_type = EnumTypeId::new(71).expect("enum type");
    let columns = vec![
        (SegmentV2LogicalType::Bool, CanonicalValue::Bool(true)),
        (SegmentV2LogicalType::I64, CanonicalValue::I64(-9)),
        (SegmentV2LogicalType::U64, CanonicalValue::U64(9)),
        (
            SegmentV2LogicalType::String,
            CanonicalValue::string("value").expect("string"),
        ),
        (
            SegmentV2LogicalType::Bytes,
            CanonicalValue::bytes([1, 2, 3]).expect("bytes"),
        ),
        (
            SegmentV2LogicalType::Timestamp,
            CanonicalValue::Timestamp(Timestamp::new(-4, 17).expect("timestamp")),
        ),
        (
            SegmentV2LogicalType::Date,
            CanonicalValue::Date(Date::new(-2)),
        ),
        (SegmentV2LogicalType::Uuid, CanonicalValue::Uuid([0x55; 16])),
        (
            SegmentV2LogicalType::Enum(enum_type),
            CanonicalValue::Enum {
                type_id: enum_type,
                variant_id: EnumVariantId::new(3).expect("variant"),
            },
        ),
        (
            SegmentV2LogicalType::Decimal(decimal_spec),
            CanonicalValue::Decimal(Decimal::new(decimal_spec, -1234).expect("decimal")),
        ),
        (
            SegmentV2LogicalType::Money {
                currency,
                amount: decimal_spec,
            },
            CanonicalValue::Money(Money::new(
                currency,
                Decimal::new(decimal_spec, 4000).expect("amount"),
            )),
        ),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, (logical_type, value))| {
        SegmentV2Column::new(
            FieldId::new(u32::try_from(index + 1).expect("field")).expect("field"),
            logical_type,
            vec![SegmentV2Cell::Value(value)],
        )
        .expect("column")
    })
    .rev()
    .collect::<Vec<_>>();
    let (keys, versions) = keys_and_versions(1);
    let segment = SegmentV2::new(identity(), keys, versions, columns).expect("segment");
    let encoded = SegmentV2Codec::encode(&segment).expect("encode");
    let decoded = SegmentV2Codec::decode(&encoded).expect("decode");

    assert_eq!(decoded, segment);
    assert!(
        decoded
            .columns()
            .windows(2)
            .all(|pair| pair[0].field_id() < pair[1].field_id()),
        "construction order must not escape canonical field-ID ordering"
    );
}

#[test]
fn v2_rejects_misalignment_duplicates_and_fixed_bounds() {
    let column = SegmentV2Column::new(
        FieldId::new(1).expect("field"),
        SegmentV2LogicalType::U64,
        vec![SegmentV2Cell::Value(CanonicalValue::U64(1))],
    )
    .expect("column");
    let (keys, versions) = keys_and_versions(1);
    assert!(matches!(
        SegmentV2::new(identity(), keys.clone(), Vec::new(), vec![column.clone()]),
        Err(SegmentV2Error::Invalid("entity-version row count"))
    ));
    assert!(matches!(
        SegmentV2::new(identity(), keys, versions, vec![column.clone(), column]),
        Err(SegmentV2Error::Invalid("duplicate field lane"))
    ));
    let oversized = vec![SegmentV2Cell::Missing; MAX_SEGMENT_V2_ROWS + 1];
    assert!(matches!(
        SegmentV2Column::new(
            FieldId::new(2).expect("field"),
            SegmentV2LogicalType::U64,
            oversized
        ),
        Err(SegmentV2Error::BoundExceeded("column rows"))
    ));
    let oversized_bytes = vec![0u8; MAX_SEGMENT_V2_BYTES + 1];
    assert!(matches!(
        SegmentV2Codec::decode(&oversized_bytes),
        Err(SegmentV2Error::BoundExceeded("segment bytes"))
    ));
}

#[test]
fn v2_detects_every_single_byte_change_and_every_truncation() {
    let column = SegmentV2Column::new(
        FieldId::new(1).expect("field"),
        SegmentV2LogicalType::String,
        vec![
            SegmentV2Cell::Value(CanonicalValue::string("alpha").expect("value")),
            SegmentV2Cell::Value(CanonicalValue::string("beta").expect("value")),
        ],
    )
    .expect("column");
    let encoded = SegmentV2Codec::encode(&one_column_segment(column)).expect("encode");
    for index in 0..encoded.len() {
        let mut corrupt = encoded.clone();
        corrupt[index] ^= 1;
        assert!(
            SegmentV2Codec::decode(&corrupt).is_err(),
            "byte {index} was not integrity protected"
        );
    }
    for length in 0..encoded.len() {
        assert!(
            SegmentV2Codec::decode(&encoded[..length]).is_err(),
            "truncation at {length} was accepted"
        );
    }
}

#[test]
fn v2_rejects_resealed_unknown_format_and_stale_lane_checksum() {
    let column = SegmentV2Column::new(
        FieldId::new(1).expect("field"),
        SegmentV2LogicalType::U64,
        vec![SegmentV2Cell::Value(CanonicalValue::U64(7))],
    )
    .expect("column");
    let encoded = SegmentV2Codec::encode(&one_column_segment(column)).expect("encode");

    let mut unknown_format = encoded.clone();
    unknown_format[9] ^= 1;
    reseal_complete_checksum(&mut unknown_format);
    assert!(matches!(
        SegmentV2Codec::decode(&unknown_format),
        Err(SegmentV2Error::Corrupt("segment format version"))
    ));

    let mut stale_lane = encoded;
    let body_end = stale_lane.len() - 32;
    stale_lane[body_end - 1] ^= 1;
    reseal_complete_checksum(&mut stale_lane);
    assert!(matches!(
        SegmentV2Codec::decode(&stale_lane),
        Err(SegmentV2Error::ChecksumMismatch("lane"))
    ));
}

#[test]
fn exact_statistics_prune_only_when_no_row_can_match() {
    let column = SegmentV2Column::new(
        FieldId::new(1).expect("field"),
        SegmentV2LogicalType::U64,
        vec![
            SegmentV2Cell::Missing,
            SegmentV2Cell::Null,
            SegmentV2Cell::Value(CanonicalValue::U64(10)),
            SegmentV2Cell::Value(CanonicalValue::U64(20)),
        ],
    )
    .expect("column");
    let cases = [
        (
            SegmentV2Predicate::Equal(CanonicalValue::U64(9)),
            SegmentV2PruningDecision::Skip,
        ),
        (
            SegmentV2Predicate::Equal(CanonicalValue::U64(10)),
            SegmentV2PruningDecision::Scan,
        ),
        (
            SegmentV2Predicate::LessThan(CanonicalValue::U64(10)),
            SegmentV2PruningDecision::Skip,
        ),
        (
            SegmentV2Predicate::LessThanOrEqual(CanonicalValue::U64(10)),
            SegmentV2PruningDecision::Scan,
        ),
        (
            SegmentV2Predicate::GreaterThan(CanonicalValue::U64(20)),
            SegmentV2PruningDecision::Skip,
        ),
        (
            SegmentV2Predicate::GreaterThanOrEqual(CanonicalValue::U64(20)),
            SegmentV2PruningDecision::Scan,
        ),
        (
            SegmentV2Predicate::IsMissing,
            SegmentV2PruningDecision::Scan,
        ),
        (SegmentV2Predicate::IsNull, SegmentV2PruningDecision::Scan),
        (
            SegmentV2Predicate::IsPresent,
            SegmentV2PruningDecision::Scan,
        ),
    ];
    for (predicate, expected) in cases {
        assert_eq!(
            column.pruning_decision(&predicate).expect("decision"),
            expected,
            "predicate {predicate:?}"
        );
    }
    assert!(matches!(
        column.pruning_decision(&SegmentV2Predicate::Equal(
            CanonicalValue::string("wrong type").expect("value")
        )),
        Err(SegmentV2Error::Invalid("predicate type mismatch"))
    ));
}

#[test]
fn randomized_round_trip_and_pruning_have_zero_false_negatives() {
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    for rows in 1..=128usize {
        let mut cells = Vec::with_capacity(rows);
        for _ in 0..rows {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            cells.push(match state % 11 {
                0 => SegmentV2Cell::Missing,
                1 => SegmentV2Cell::Null,
                _ => SegmentV2Cell::Value(CanonicalValue::U64(state % 257)),
            });
        }
        let column = SegmentV2Column::new(
            FieldId::new(1).expect("field"),
            SegmentV2LogicalType::U64,
            cells.clone(),
        )
        .expect("column");
        let segment = one_column_segment(column.clone());
        let encoded = SegmentV2Codec::encode(&segment).expect("encode");
        assert_eq!(SegmentV2Codec::decode(&encoded).expect("decode"), segment);

        for needle in [0u64, 1, 64, 128, 256, 257] {
            let predicate = SegmentV2Predicate::Equal(CanonicalValue::U64(needle));
            if column.pruning_decision(&predicate).expect("decision")
                == SegmentV2PruningDecision::Skip
            {
                assert!(
                    !cells.iter().any(|cell| {
                        matches!(cell, SegmentV2Cell::Value(CanonicalValue::U64(value)) if *value == needle)
                    }),
                    "false-negative equality prune for {needle}"
                );
            }
        }
    }
}

#[test]
fn v2_candidate_manifest_is_canonical_partition_scoped_and_integrity_checked() {
    let column = SegmentV2Column::new(
        FieldId::new(1).expect("field"),
        SegmentV2LogicalType::U64,
        vec![SegmentV2Cell::Value(CanonicalValue::U64(7))],
    )
    .expect("column");
    let segment = one_column_segment(column);
    let segment_bytes = SegmentV2Codec::encode(&segment).expect("segment encode");
    let segment_checksum = *hash(HashDomain::CanonicalValue, &segment_bytes).as_bytes();
    let entry = ColumnarManifestV2Entry::new(
        segment.identity().segment_id(),
        segment.identity().frontier_start(),
        segment.identity().frontier_end(),
        segment.primary_keys().len(),
        segment_bytes.len(),
        segment_checksum,
    )
    .expect("entry");
    assert_eq!(
        entry.file_name(),
        "seg-v2-33333333333333333333333333333333.col"
    );
    let manifest = ColumnarManifestV2::new(
        segment.identity().definition_fingerprint(),
        segment.identity().history_incarnation(),
        segment.identity().generation(),
        segment.identity().organization().clone(),
        segment.identity().frontier_end(),
        vec![entry],
    )
    .expect("manifest");
    let bytes = manifest.encode().expect("manifest encode");
    assert_eq!(
        ColumnarManifestV2::decode(&bytes).expect("decode"),
        manifest
    );
    assert_eq!(
        ColumnarManifestV2::decode(&bytes)
            .expect("decode")
            .encode()
            .expect("re-encode"),
        bytes
    );

    let mut corrupt = bytes;
    corrupt[0] ^= 1;
    assert!(matches!(
        ColumnarManifestV2::decode(&corrupt),
        Err(SegmentV2Error::ChecksumMismatch("manifest"))
    ));
}

#[test]
// req: OQ-024, OQ-053
fn checked_in_v2_fixture_decodes_and_reencodes_byte_exactly() {
    let fixture = include_str!("../../../fixtures/compatibility/columnar-v2-codec-v1.txt");
    let value = |name: &str| {
        fixture
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .unwrap_or_else(|| panic!("missing fixture field {name}"))
    };
    assert_eq!(value("production_selected_layout="), "1");
    assert_eq!(
        value("candidate_status="),
        "mechanics_accepted_wp710_inactive_pending_wp711"
    );
    let segment_bytes = decode_hex(value("segment_hex="));
    let manifest_bytes = decode_hex(value("manifest_hex="));
    let segment = SegmentV2Codec::decode(&segment_bytes).expect("fixture segment");
    let manifest = ColumnarManifestV2::decode(&manifest_bytes).expect("fixture manifest");
    assert_eq!(
        SegmentV2Codec::encode(&segment).expect("segment re-encode"),
        segment_bytes
    );
    assert_eq!(
        manifest.encode().expect("manifest re-encode"),
        manifest_bytes
    );
}

#[test]
#[ignore = "fixture generator; compare output before updating the checked-in fixture"]
fn emit_columnar_v2_compatibility_fixture() {
    let status = SegmentV2Column::new(
        FieldId::new(41).expect("field"),
        SegmentV2LogicalType::String,
        vec![
            SegmentV2Cell::Missing,
            SegmentV2Cell::Null,
            SegmentV2Cell::Value(CanonicalValue::string("open").expect("value")),
        ],
    )
    .expect("column");
    let priority = SegmentV2Column::new(
        FieldId::new(42).expect("field"),
        SegmentV2LogicalType::U64,
        vec![
            SegmentV2Cell::Value(CanonicalValue::U64(10)),
            SegmentV2Cell::Value(CanonicalValue::U64(11)),
            SegmentV2Cell::Value(CanonicalValue::U64(12)),
        ],
    )
    .expect("column");
    let (keys, versions) = keys_and_versions(3);
    let segment = SegmentV2::new(identity(), keys, versions, vec![status, priority])
        .expect("fixture segment");
    let segment_bytes = SegmentV2Codec::encode(&segment).expect("segment bytes");
    let entry = ColumnarManifestV2Entry::new(
        segment.identity().segment_id(),
        segment.identity().frontier_start(),
        segment.identity().frontier_end(),
        segment.primary_keys().len(),
        segment_bytes.len(),
        *hash(HashDomain::CanonicalValue, &segment_bytes).as_bytes(),
    )
    .expect("entry");
    let manifest = ColumnarManifestV2::new(
        segment.identity().definition_fingerprint(),
        segment.identity().history_incarnation(),
        segment.identity().generation(),
        segment.identity().organization().clone(),
        segment.identity().frontier_end(),
        vec![entry],
    )
    .expect("manifest");
    let manifest_bytes = manifest.encode().expect("manifest bytes");
    println!("segment_hex={}", encode_hex(&segment_bytes));
    println!("manifest_hex={}", encode_hex(&manifest_bytes));
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn decode_hex(encoded: &str) -> Vec<u8> {
    assert_eq!(encoded.len() % 2, 0, "hex fixture length");
    encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).expect("hex utf8");
            u8::from_str_radix(text, 16).expect("hex byte")
        })
        .collect()
}

fn reseal_complete_checksum(bytes: &mut [u8]) {
    let body_end = bytes.len() - 32;
    let digest = hash(HashDomain::CanonicalValue, &bytes[..body_end]);
    bytes[body_end..].copy_from_slice(digest.as_bytes());
}
