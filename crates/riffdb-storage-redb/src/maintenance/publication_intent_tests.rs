// req: REC-001, REC-004, REC-005, REC-006, REC-008
use super::*;
use riffdb_storage_api::{
    ChangelogHistoryPointV3, ChangelogLineageV3, ChangelogTransactionSequence,
};
use riffdb_types::{DualFrontier, LeadershipEpochV1};

fn sample() -> PublicationIntent {
    let database = DatabaseId::from_unix_milliseconds_and_random(1234, [0x31; 10]).unwrap();
    let frontier = DualFrontier::new(CommitSequence::new(17), None);
    let point = ChangelogHistoryPointV3::new(
        ChangelogTransactionSequence::new(23).unwrap(),
        [0x51; 32],
        frontier,
    );
    let lineage = ChangelogLineageV3::new(database, 3, LeadershipEpochV1::new(1).unwrap()).unwrap();
    let history = ChangelogHistoryStateV3::new(lineage, point, point, point).unwrap();
    PublicationIntent::prepared(
        OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1234, [0x21; 10]).unwrap(),
        OfflineBackupManifestIdentityV1::new(
            BackupIntegrityChecksumV1::new(vec![0x41; 32]).unwrap(),
            database,
            frontier.application(),
        ),
        3,
        history,
        11,
        ArtifactKind::ORDERED.map(|kind| ArtifactSeal {
            kind,
            length: 4096 * u64::from(kind.tag()),
            checksum: BackupIntegrityChecksumV1::new(vec![0x60 + kind.tag(); 32]).unwrap(),
        }),
    )
    .unwrap()
}

#[test]
fn publication_intent_preserves_the_same_seals_across_all_durable_phases() {
    let mut intent = sample();
    let frozen = intent.artifacts.clone();
    for phase in [
        PublicationPhase::Prepared,
        PublicationPhase::Published,
        PublicationPhase::StartupConsumed,
    ] {
        if phase != PublicationPhase::Prepared {
            intent.advance(phase).unwrap();
        }
        let encoded = intent.encode().unwrap();
        assert!(encoded.len() <= MAX_INTENT_BYTES);
        let decoded = PublicationIntent::decode(&encoded).unwrap();
        assert!(decoded == intent);
        assert!(decoded.artifacts == frozen);
        assert_eq!(decoded.encode().unwrap(), encoded);
    }
}

#[test]
fn publication_intent_v1_frozen_vectors_preserve_binding_and_artifact_seals() {
    let vectors = [
        (
            PublicationPhase::Prepared,
            include_str!(
                "../../../../fixtures/compatibility/native-publication-intent-v1/prepared.hex"
            ),
        ),
        (
            PublicationPhase::Published,
            include_str!(
                "../../../../fixtures/compatibility/native-publication-intent-v1/published.hex"
            ),
        ),
        (
            PublicationPhase::StartupConsumed,
            include_str!(
                "../../../../fixtures/compatibility/native-publication-intent-v1/startup-consumed.hex"
            ),
        ),
    ];
    let mut expected = sample();
    for (phase, text) in vectors {
        if phase != PublicationPhase::Prepared {
            expected.advance(phase).unwrap();
        }
        let hex = text.trim();
        assert_eq!(hex.len() % 2, 0);
        assert!(
            hex.bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        );
        let frozen: Vec<u8> = hex
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect();
        let actual = PublicationIntent::decode(&frozen).unwrap();
        assert!(actual == expected);
        assert_eq!(expected.encode().unwrap(), frozen);
    }
}

#[test]
fn publication_intent_refuses_regression_skipped_state_and_identity_substitution() {
    let mut intent = sample();
    let original = intent.clone();
    for phase in [
        PublicationPhase::Prepared,
        PublicationPhase::StartupConsumed,
    ] {
        assert!(intent.advance(phase).is_err());
        assert!(intent == original);
    }
    intent.advance(PublicationPhase::Published).unwrap();
    let published = intent.clone();
    for phase in [PublicationPhase::Prepared, PublicationPhase::Published] {
        assert!(intent.advance(phase).is_err());
        assert!(intent == published);
    }
    intent.advance(PublicationPhase::StartupConsumed).unwrap();
    assert!(intent.advance(PublicationPhase::Published).is_err());
    let mut foreign = sample();
    foreign.incarnation += 1;
    assert!(foreign.encode().is_err());
    let mut foreign = sample();
    foreign.retention_watermark = 18;
    assert!(foreign.encode().is_err());
    let mut foreign = sample();
    foreign.artifacts.swap(0, 1);
    assert!(foreign.encode().is_err());
    let mut foreign = sample();
    foreign.artifacts[0].length = 0;
    assert!(foreign.encode().is_err());
    let mut foreign = sample();
    foreign.manifest = OfflineBackupManifestIdentityV1::new(
        foreign.manifest.manifest_checksum().clone(),
        DatabaseId::from_unix_milliseconds_and_random(1234, [0x32; 10]).unwrap(),
        foreign.manifest.included_application_frontier(),
    );
    assert!(foreign.encode().is_err());
}

fn reseal(mut bytes: Vec<u8>) -> Vec<u8> {
    let body = bytes.len() - SHA256_BYTES;
    let seal = Sha256::digest(&bytes[..body]);
    bytes[body..].copy_from_slice(&seal);
    bytes
}

#[test]
fn publication_intent_refuses_resealed_unknown_fields_and_noncanonical_artifact_sets() {
    let encoded = sample().encode().unwrap();
    for end in 0..encoded.len() {
        assert!(PublicationIntent::decode(&encoded[..end]).is_err());
    }
    for offset in 0..encoded.len() {
        let mut bad = encoded.clone();
        bad[offset] ^= 0x80;
        assert!(PublicationIntent::decode(&bad).is_err());
    }
    let version = MAGIC.len();
    let phase = version + 4;
    for (offset, value) in [(version + 3, 2), (phase, 0), (phase, 4)] {
        let mut bad = encoded.clone();
        bad[offset] = value;
        assert!(PublicationIntent::decode(&reseal(bad)).is_err());
    }
    // The closed final inventory is exactly three tagged fixed-width seals.
    let first = encoded.len() - SHA256_BYTES - (1 + 8 + SHA256_BYTES) * 3;
    for (offset, value) in [
        (first - 1, 2),
        (first - 1, 4),
        (first, 2),
        (first + 41, 1),
        (first + 82, 4),
    ] {
        let mut bad = encoded.clone();
        bad[offset] = value;
        assert!(PublicationIntent::decode(&reseal(bad)).is_err());
    }
    let mut bad = encoded.clone();
    bad.splice(bad.len() - SHA256_BYTES..bad.len() - SHA256_BYTES, [0x75]);
    assert!(PublicationIntent::decode(&reseal(bad)).is_err());
    assert!(PublicationIntent::decode(&vec![0; MAX_INTENT_BYTES + 1]).is_err());
}

#[test]
fn publication_intent_prior_reader_refuses_inventory_before_cleanup_or_database_open() {
    let root = crate::test_path::ScopedDirectory::new("publication-prior-reader");
    let database = root.join("database.redb");
    let backups = root.join("backups");
    let (owner, _) = crate::RedbMaintenanceStorage::open(&database, &backups).unwrap();
    drop(owner);
    let intent = sample();
    let encoded = intent.encode().unwrap();
    let receipts = backups.join(".maintenance/receipts");
    let ordinary_temporary = receipts.join(format!(".{}.receipt-v1.tmp", intent.operation_id));
    let stage = backups.join(".maintenance/staged/unadmitted-evidence");
    // Neither target nor stage is a database. A prior reader must refuse at
    // inventory validation, before trying either open or cleaning these files.
    std::fs::write(&database, b"configured media must remain unchanged").unwrap();
    std::fs::create_dir(&stage).unwrap();
    std::fs::write(stage.join("evidence"), b"unadmitted stage must survive").unwrap();
    std::fs::write(&ordinary_temporary, b"ordinary temporary must survive").unwrap();
    for name in [
        format!("{}.publication-intent-v1", intent.operation_id),
        format!(".{}.publication-intent-v1.tmp", intent.operation_id),
    ] {
        let path = receipts.join(name);
        std::fs::write(&path, &encoded).unwrap();
        let refused = crate::RedbMaintenanceStorage::open(&database, &backups)
            .err()
            .unwrap();
        assert_eq!(refused.kind(), StorageErrorKind::CorruptData);
        assert_eq!(std::fs::read(&path).unwrap(), encoded);
        assert_eq!(
            std::fs::read(&ordinary_temporary).unwrap(),
            b"ordinary temporary must survive"
        );
        assert_eq!(
            std::fs::read(stage.join("evidence")).unwrap(),
            b"unadmitted stage must survive"
        );
        assert_eq!(
            std::fs::read(&database).unwrap(),
            b"configured media must remain unchanged"
        );
        std::fs::remove_file(path).unwrap();
    }
}
