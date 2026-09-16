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
fn changelog_v3_is_only_production_replication_identity() {
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
            745,
            "da40934ddb54b93beb7e8febe151bafc70a9e67c5faa22a4a06c705308b13c6a",
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
    assert_no_legacy_production_consumers();
}

fn assert_no_legacy_production_consumers() {
    use std::{fs, path::Path};
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    // Codecs retain their original reader/round-trip machinery. No other
    // production crate may consume the legacy frame or stream types, even if
    // it would otherwise be hidden behind an unexported module or feature.
    let mut pending = Vec::new();
    for entry in fs::read_dir(root.join("crates")).unwrap() {
        let source = entry.unwrap().path().join("src");
        if source.is_dir() {
            pending.push(source);
        }
    }
    let mut checked = 0;
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                if path.file_name().unwrap() != "tests" {
                    pending.push(path);
                }
                continue;
            }
            if path.extension().is_none_or(|extension| extension != "rs")
                || path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .ends_with("_tests.rs")
                || path == root.join("crates/riffdb-storage-api/src/changelog.rs")
                || path == root.join("crates/riffdb-storage-api/src/changelog_v2.rs")
            {
                continue;
            }
            let source = fs::read_to_string(&path).unwrap();
            let production = source.split("\n#[cfg(test)]\nmod tests").next().unwrap();
            for forbidden in [
                "ChangelogFrameV1",
                "ChangelogFrameV2",
                "ChangelogStreamValidatorV1",
                "ChangelogStreamValidatorV2",
                "ChangelogFrameConsumer",
                "EntityReplicaBootstrapManifestV2",
                "EntityReplicaBootstrapRowV2",
                "DeleteAwareEntityFollowerV2",
                "start_changelog_emitter",
            ] {
                assert!(
                    !production.contains(forbidden),
                    "{} consumes {forbidden}",
                    path.display()
                );
            }
            checked += 1;
        }
    }
    assert!(
        checked > 100,
        "the workspace production scan must not be empty"
    );
    let adapter =
        fs::read_to_string(root.join("crates/riffdb-storage-redb/src/changelog.rs")).unwrap();
    assert!(adapter.contains("fn changelog_receipts_v3("));
    assert!(adapter.contains("fn authoritative_state_v3("));
    let compatibility =
        fs::read_to_string(root.join("tests/storage_recovery/changelog_compatibility.rs")).unwrap();
    assert!(compatibility.contains("fn start_changelog_emitter("));
    assert!(compatibility.contains("fn start_changelog_emitter_v2("));
    // WP-746/749 own future activation. This proves the present production
    // boundary plus exact codec custody; it does not claim an RPC is active.
}

#[test]
fn amendment_one_command_vectors_preserve_exact_mutations_frontiers_and_source_counts() {
    use riffdb_storage_api::{
        AuthoritativeNamespaceV1 as N, AuthoritativeTransactionV3 as Receipt,
        ChangelogAttributionV3 as Source,
    };
    let admission = decode_hex(include_str!(
        "../../../fixtures/replication/changelog-receipt-v3-command-admission.hex"
    ));
    let failure = decode_hex(include_str!(
        "../../../fixtures/replication/changelog-receipt-v3-command-execution-failure.hex"
    ));
    let audited = decode_hex(include_str!(
        "../../../fixtures/replication/changelog-receipt-v3-command-execution-failure-audited.hex"
    ));
    let frame = decode_hex(include_str!(
        "../../../fixtures/replication/changelog-frame-v3-command-lifecycle.hex"
    ));
    for (bytes, length, digest) in [
        (
            &admission,
            232,
            "118f28d7e451c594071b3f296969b51bfdfe655cb1f90c17b8702997cdbf76b8",
        ),
        (
            &failure,
            286,
            "1f9ce3092ec2f04d2370a76ecf3613693732ec54874786389704ee585915545b",
        ),
        (
            &audited,
            361,
            "c1844556deba6d90cdaff7eb55eaa20a5ef4a6e201e128a2c217452945d9b83f",
        ),
        (
            &frame,
            872,
            "f9b2696b3982aa3a1d1f7f63cab25d939869f6e5ca61e045e7269c6d34829d8c",
        ),
    ] {
        assert_eq!(bytes.len(), length);
        assert_eq!(
            Sha256::digest(bytes)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            digest
        );
    }
    for bytes in [&admission, &failure, &audited] {
        assert_eq!(Receipt::decode(bytes).unwrap().encode().unwrap(), *bytes);
    }
    let admission = Receipt::decode(&admission).unwrap();
    let failure = Receipt::decode(&failure).unwrap();
    let audited = Receipt::decode(&audited).unwrap();
    assert_eq!(admission.attribution(), Source::CommandAdmission);
    assert_eq!(failure.attribution(), Source::CommandExecutionFailure);
    assert_eq!(audited.attribution(), Source::CommandExecutionFailure);
    for receipt in [&admission, &failure] {
        assert_eq!(
            receipt.binding().predecessor_frontier,
            receipt.binding().covered_frontier
        );
    }
    assert_eq!(audited.binding().covered_frontier.application(), None);
    assert_eq!(
        audited
            .binding()
            .covered_frontier
            .administration()
            .unwrap()
            .get(),
        1
    );
    assert_eq!(
        failure.binding().predecessor,
        Some(admission.binding().sequence)
    );
    assert_eq!(
        failure.binding().prior_history_hash,
        admission.history_hash().unwrap()
    );
    assert_eq!(admission.mutations()[0].namespace(), N::IdempotencyPending);
    assert!(admission.mutations()[0].matches_prior(None));
    assert_eq!(failure.mutations()[0].namespace(), N::Idempotency);
    assert_eq!(failure.mutations()[1].namespace(), N::IdempotencyPending);
    assert!(failure.mutations()[1].matches_prior(admission.mutations()[0].value()));
    assert!(failure.mutations()[1].value().is_none());
    assert_eq!(audited.mutations()[2].namespace(), N::Audit);
    let decoded = ChangelogFrameV3::decode(&frame).unwrap();
    assert_eq!(decoded.receipts(), &[admission.clone(), failure]);
    assert_eq!(decoded.encode().unwrap(), frame);
    // Counts for both newly closed sources are checked even after resealing.
    for offset in [290, 294] {
        let mut forged = frame.clone();
        forged[offset] ^= 1;
        let footer = forged.len() - 48;
        let checksum = Sha256::digest(&forged[..footer]);
        let end = forged.len() - 32;
        forged[end..].copy_from_slice(&checksum);
        assert!(ChangelogFrameV3::decode(&forged).is_err());
    }
    let mut unknown = admission.encode().unwrap();
    unknown[86..88].copy_from_slice(&34_u16.to_be_bytes());
    let end = unknown.len() - 32;
    let checksum = Sha256::digest(&unknown[..end]);
    unknown[end..].copy_from_slice(&checksum);
    assert!(Receipt::decode(&unknown).is_err());
}
