#![forbid(unsafe_code)]
//! Byte-exact format custody; synthetic codec vectors are not publication evidence.
// req: REP-003, REC-001

use riffdb_storage_api::{ChangelogFrameV1, ChangelogFrameV2, ChangelogFrameV3};
use sha2::{Digest, Sha256};

fn decode_hex(text: &str) -> Vec<u8> {
    let content = text.strip_suffix('\n').expect("canonical final newline");
    assert!(content.len().is_multiple_of(2));
    content
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            assert!(
                pair.iter()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
            );
            u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()
        })
        .collect()
}

#[test]
fn changelog_frozen_vectors_preserve_exact_v1_v2_and_refuse_v3_downgrade() {
    let v1 = decode_hex(include_str!(
        "../../../fixtures/replication/changelog-frame-v1.hex"
    ));
    let v2 = decode_hex(include_str!(
        "../../../fixtures/replication/changelog-frame-v2.hex"
    ));
    let v3 = decode_hex(include_str!(
        "../../../fixtures/replication/changelog-frame-v3.hex"
    ));
    for (bytes, length, digest) in [
        (
            &v1,
            263,
            "990cadfe56476181f4bd583c77a4158fc8f614dc830aaa57e7ebab85fc2a0b5f",
        ),
        (
            &v2,
            271,
            "3e3aec62a325d153742700a7c9925dd94fdaad111c700e62aa78724b16f583a5",
        ),
        (
            &v3,
            737,
            "2f9ca9ae53f1e45c87e6940113b76b35d8e1b3d6359cc4d3c3cff5c27cefa6bf",
        ),
    ] {
        assert_eq!(bytes.len(), length);
        let actual = Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(actual, digest);
    }
    assert_eq!(
        ChangelogFrameV1::decode(&v1)
            .unwrap()
            .0
            .encode()
            .unwrap()
            .as_bytes(),
        v1
    );
    assert_eq!(
        ChangelogFrameV2::decode(&v2)
            .unwrap()
            .0
            .encode()
            .unwrap()
            .as_bytes(),
        v2
    );
    assert_eq!(ChangelogFrameV3::decode(&v3).unwrap().encode().unwrap(), v3);
    assert!(ChangelogFrameV3::decode(&v1).is_err());
    assert!(ChangelogFrameV3::decode(&v2).is_err());
    assert!(ChangelogFrameV1::decode(&v3).is_err());
    assert!(ChangelogFrameV2::decode(&v3).is_err());
    // Refusal cannot depend only on an accidental checksum incompatibility.
    for predecessor_magic in [b"RDBCLF01", b"RDBCLF02"] {
        let mut downgraded = v3.clone();
        downgraded[..8].copy_from_slice(predecessor_magic);
        let footer = downgraded.len() - 48;
        let checksum: [u8; 32] = Sha256::digest(&downgraded[..footer]).into();
        let checksum_offset = downgraded.len() - 32;
        downgraded[checksum_offset..].copy_from_slice(&checksum);
        assert!(ChangelogFrameV3::decode(&downgraded).is_err());
    }
}
