//! Canonical generation-root identity, codec, and compatibility evidence.

use riffdb_columnar::{
    COLUMNAR_ENCODING_REGISTRY_VERSION_V1, COLUMNAR_LAYOUT_VERSION_V2,
    COLUMNAR_MANIFEST_FORMAT_VERSION_V2, COLUMNAR_SEGMENT_FORMAT_VERSION_V2,
    ColumnarGenerationRootV1, ColumnarGenerationRootV1Entry, DefinitionFingerprint,
    MAX_COLUMNAR_GENERATION_ROOT_V1_PARTITIONS, OrgKey, PhysicalGenerationFingerprintV1,
    SegmentV2Codec,
};
use riffdb_types::{
    CanonicalValue, CommitSequence, FrontierPosition, HashDomain, ProjectionGeneration, hash,
};

fn entry(org: u8, checksum: u8) -> ColumnarGenerationRootV1Entry {
    ColumnarGenerationRootV1Entry::new(
        OrgKey::from_value(&CanonicalValue::U64(u64::from(org))).expect("organization"),
        128,
        [checksum; 32],
    )
    .expect("root entry")
}

fn root(entries: Vec<ColumnarGenerationRootV1Entry>) -> ColumnarGenerationRootV1 {
    ColumnarGenerationRootV1::new(
        DefinitionFingerprint::from_bytes([0x11; 32]),
        7,
        ProjectionGeneration::new(9).expect("generation"),
        FrontierPosition::AppliedThrough(CommitSequence::new(23).expect("frontier")),
        entries.len() as u64,
        entries.len() as u64 * 3,
        entries,
    )
    .expect("root")
}

// req: PRJ-004, PRJ-009, PRJ-010, OQ-024, OQ-053
#[test]
fn columnar_generation_root_v1_round_trips_and_refuses_noncanonical_inventory() {
    let root = root(vec![entry(1, 0x21), entry(2, 0x22)]);
    let bytes = root.encode().expect("root bytes");
    let fixture = include_str!("../../../fixtures/compatibility/columnar-generation-root-v1.txt");
    assert_eq!(
        encode_hex(&bytes),
        fixture_value(fixture, "root_hex="),
        "the registered root fixture must remain byte-exact"
    );
    assert_eq!(
        encode_hex(root.physical_generation_fingerprint().as_bytes()),
        fixture_value(fixture, "physical_generation_fingerprint="),
    );
    let decoded = ColumnarGenerationRootV1::decode(&bytes).expect("root decode");
    assert_eq!(decoded, root);
    assert_eq!(decoded.encoded_length(), bytes.len() as u64);
    assert_eq!(decoded.layout_version(), COLUMNAR_LAYOUT_VERSION_V2);
    assert_eq!(
        decoded.segment_format_version(),
        COLUMNAR_SEGMENT_FORMAT_VERSION_V2
    );
    assert_eq!(
        decoded.encoding_registry_version(),
        COLUMNAR_ENCODING_REGISTRY_VERSION_V1
    );
    assert_eq!(
        decoded.manifest_format_version(),
        COLUMNAR_MANIFEST_FORMAT_VERSION_V2
    );
    assert_eq!(
        decoded.physical_generation_fingerprint(),
        PhysicalGenerationFingerprintV1::compute(DefinitionFingerprint::from_bytes([0x11; 32]))
    );
    assert_eq!(
        decoded.partitions()[0].file_name(),
        format!("partition-{}.manifest-v2", "21".repeat(32))
    );

    let mut corrupt_checksum = bytes.clone();
    let final_byte = corrupt_checksum.last_mut().expect("checksum byte");
    *final_byte ^= 0x01;
    assert!(ColumnarGenerationRootV1::decode(&corrupt_checksum).is_err());

    let mut unknown_format = bytes.clone();
    unknown_format[8..10].copy_from_slice(&2u16.to_be_bytes());
    rewrite_checksum(&mut unknown_format);
    assert!(ColumnarGenerationRootV1::decode(&unknown_format).is_err());

    let mut unknown_flags = bytes.clone();
    unknown_flags[10..12].copy_from_slice(&1u16.to_be_bytes());
    rewrite_checksum(&mut unknown_flags);
    assert!(ColumnarGenerationRootV1::decode(&unknown_flags).is_err());

    let mut trailing = bytes.clone();
    trailing.insert(trailing.len() - 32, 0);
    rewrite_checksum(&mut trailing);
    assert!(ColumnarGenerationRootV1::decode(&trailing).is_err());

    let mut physical_mismatch = bytes.clone();
    physical_mismatch[52] ^= 0x01;
    rewrite_checksum(&mut physical_mismatch);
    assert!(ColumnarGenerationRootV1::decode(&physical_mismatch).is_err());

    let mut reordered = bytes.clone();
    let first = reordered[147..201].to_vec();
    let second = reordered[201..255].to_vec();
    reordered[147..201].copy_from_slice(&second);
    reordered[201..255].copy_from_slice(&first);
    rewrite_checksum(&mut reordered);
    assert!(ColumnarGenerationRootV1::decode(&reordered).is_err());

    assert!(
        ColumnarGenerationRootV1::new(
            DefinitionFingerprint::from_bytes([0x11; 32]),
            7,
            ProjectionGeneration::new(9).expect("generation"),
            FrontierPosition::BeforeFirst,
            2,
            6,
            vec![entry(2, 0x22), entry(1, 0x21)],
        )
        .is_err(),
        "reordered inventory must refuse rather than normalize"
    );
    assert!(
        ColumnarGenerationRootV1::new(
            DefinitionFingerprint::from_bytes([0x11; 32]),
            7,
            ProjectionGeneration::new(9).expect("generation"),
            FrontierPosition::BeforeFirst,
            2,
            6,
            vec![entry(1, 0x21), entry(1, 0x22)],
        )
        .is_err(),
        "duplicate organizations must refuse"
    );
    assert!(
        ColumnarGenerationRootV1::new(
            DefinitionFingerprint::from_bytes([0x11; 32]),
            7,
            ProjectionGeneration::new(9).expect("generation"),
            FrontierPosition::BeforeFirst,
            2,
            6,
            vec![entry(1, 0x21), entry(2, 0x21)],
        )
        .is_err(),
        "duplicate derived filenames must refuse"
    );
    assert!(
        ColumnarGenerationRootV1::new(
            DefinitionFingerprint::from_bytes([0x11; 32]),
            7,
            ProjectionGeneration::new(9).expect("generation"),
            FrontierPosition::BeforeFirst,
            0,
            1,
            Vec::new(),
        )
        .is_err(),
        "an empty inventory must have zero segment and row totals"
    );

    let excessive = (0..=MAX_COLUMNAR_GENERATION_ROOT_V1_PARTITIONS)
        .map(|index| {
            ColumnarGenerationRootV1Entry::new(
                OrgKey::from_value(&CanonicalValue::U64(index as u64)).expect("organization"),
                128,
                PhysicalGenerationFingerprintV1::compute(DefinitionFingerprint::from_bytes(
                    (index as u32)
                        .to_be_bytes()
                        .repeat(8)
                        .try_into()
                        .expect("digest"),
                ))
                .into_bytes(),
            )
            .expect("entry")
        })
        .collect();
    assert!(
        ColumnarGenerationRootV1::new(
            DefinitionFingerprint::from_bytes([0x11; 32]),
            7,
            ProjectionGeneration::new(9).expect("generation"),
            FrontierPosition::BeforeFirst,
            (MAX_COLUMNAR_GENERATION_ROOT_V1_PARTITIONS + 1) as u64,
            (MAX_COLUMNAR_GENERATION_ROOT_V1_PARTITIONS + 1) as u64,
            excessive,
        )
        .is_err()
    );
}

// req: PRJ-010, OQ-017, OQ-022, OQ-024, OQ-053
#[test]
fn columnar_v2_physical_generation_fingerprint_preserves_frozen_definition_bytes() {
    let definition = DefinitionFingerprint::from_bytes([0x11; 32]);
    let fingerprint = PhysicalGenerationFingerprintV1::compute(definition);
    assert_eq!(
        fingerprint,
        PhysicalGenerationFingerprintV1::compute(definition)
    );
    assert_ne!(fingerprint.as_bytes(), definition.as_bytes());

    let fixture = include_str!("../../../fixtures/compatibility/columnar-v2-codec-v1.txt");
    let segment_hex = fixture
        .lines()
        .find_map(|line| line.strip_prefix("segment_hex="))
        .expect("segment fixture");
    let segment_bytes = decode_hex(segment_hex);
    let segment = SegmentV2Codec::decode(&segment_bytes).expect("frozen Segment V2 fixture");
    assert_eq!(segment.identity().definition_fingerprint(), definition);
    assert_eq!(
        SegmentV2Codec::encode(&segment).expect("fixture re-encode"),
        segment_bytes
    );
}

fn decode_hex(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).expect("hex utf8");
            u8::from_str_radix(text, 16).expect("hex byte")
        })
        .collect()
}

fn encode_hex(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn fixture_value<'a>(fixture: &'a str, prefix: &str) -> &'a str {
    fixture
        .lines()
        .find_map(|line| line.strip_prefix(prefix))
        .expect("fixture field")
}

fn rewrite_checksum(bytes: &mut [u8]) {
    let body_length = bytes.len() - 32;
    let checksum = *hash(HashDomain::CanonicalValue, &bytes[..body_length]).as_bytes();
    bytes[body_length..].copy_from_slice(&checksum);
}
