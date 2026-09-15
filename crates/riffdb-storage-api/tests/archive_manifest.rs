#![forbid(unsafe_code)]
//! External archive descriptors bind each whole frame to its exact backup chain.
// req: REP-007, AFC-007
use riffdb_storage_api::*;
#[path = "support/archive_manifest_fixture.rs"]
mod fixture_support;
use fixture_support::{fixture, frame};

struct Capture {
    previous: Option<ArchiveManifestV1>,
}
impl ArchiveFrameSinkV1 for Capture {
    fn persist(&mut self, frame: &ArchiveFrameV1) -> Result<(), ArchiveConsumerErrorV1> {
        let manifest = match &self.previous {
            None => ArchiveManifestV1::first(
                frame,
                [0x55; 32],
                ArchiveEncryptionPostureV1::OperatorManaged,
            ),
            Some(previous) => previous.next(frame),
        }?;
        manifest.verify_frame(frame.as_bytes())?;
        self.previous = Some(manifest);
        Ok(())
    }
}
fn first() -> (ArchiveManifestV1, Vec<u8>) {
    let (lineage, after) = fixture();
    let bytes = frame(lineage, after, 3);
    let mut archive = ArchiveConsumerV1::new(Capture { previous: None }, lineage, after);
    archive.append(bytes.clone()).unwrap();
    (archive.into_sink().previous.unwrap(), bytes)
}
#[test]
fn archive_manifest_roundtrip_binds_backup_identity_range_and_exact_frame() {
    let (manifest, bytes) = first();
    let encoded = manifest.encode();
    assert_eq!(encoded.len(), ARCHIVE_MANIFEST_V1_BYTES);
    assert_eq!(ArchiveManifestV1::decode(&encoded).unwrap(), manifest);
    assert_eq!(manifest.lineage(), fixture().0);
    assert_eq!(manifest.backup_fence(), fixture().1);
    assert_eq!(manifest.before(), fixture().1);
    assert_eq!(
        manifest.covered().frontier().application().unwrap().get(),
        3
    );
    assert_eq!(manifest.full_backup_manifest_digest(), [0x55; 32]);
    assert_eq!(
        manifest.encryption_posture(),
        ArchiveEncryptionPostureV1::OperatorManaged
    );
    assert_eq!(manifest.previous_manifest_digest(), None);
    assert_eq!(manifest.frame_bytes(), bytes.len() as u64);
    assert_eq!(format!("{manifest:?}"), "ArchiveManifestV1([redacted])");
    manifest.verify_frame(&bytes).unwrap();
    let mut corrupt = bytes;
    corrupt[100] ^= 1;
    assert!(manifest.verify_frame(&corrupt).is_err());
    assert!(manifest.verify_frame(&[]).is_err());
}
#[test]
fn archive_manifest_chain_rejects_reorder_duplicate_and_changed_backup_binding() {
    let (first, _) = first();
    let bytes = frame(first.lineage(), first.covered(), 5);
    let mut archive = ArchiveConsumerV1::new(
        Capture {
            previous: Some(first),
        },
        first.lineage(),
        first.covered(),
    );
    archive.append(bytes.clone()).unwrap();
    let second = archive.into_sink().previous.unwrap();
    second.verify_predecessor(Some(&first)).unwrap();
    first.verify_predecessor(None).unwrap();
    assert!(second.verify_predecessor(None).is_err());
    assert!(first.verify_predecessor(Some(&second)).is_err());
    assert!(first.verify_predecessor(Some(&first)).is_err());
    assert_eq!(second.previous_manifest_digest(), Some(first.digest()));
    assert_eq!(second.backup_fence(), first.backup_fence());
    assert!(first.verify_frame(&bytes).is_err());
    let mut wrong = first.encode();
    wrong[74] ^= 1; // Backup digest, then recompute framing checksum.
    resign(&mut wrong);
    let wrong = ArchiveManifestV1::decode(&wrong).unwrap();
    assert!(second.verify_predecessor(Some(&wrong)).is_err());
}
fn resign(bytes: &mut [u8]) {
    use sha2::{Digest, Sha256};
    let end = bytes.len() - 32;
    let checksum: [u8; 32] = Sha256::digest(&bytes[..end]).into();
    bytes[end..].copy_from_slice(&checksum);
}
#[test]
fn archive_manifest_refuses_unknown_noncanonical_truncated_and_oversized_bytes() {
    let encoded = first().0.encode();
    for len in 0..encoded.len() {
        assert!(ArchiveManifestV1::decode(&encoded[..len]).is_err());
    }
    let mut extra = encoded.clone();
    extra.push(0);
    assert!(ArchiveManifestV1::decode(&extra).is_err());
    for offset in 0..encoded.len() {
        let mut corrupt = encoded.clone();
        corrupt[offset] ^= 1;
        assert!(ArchiveManifestV1::decode(&corrupt).is_err());
    }
    // Even checksummed envelopes must have the exact version/catalog/flags.
    for (offset, value) in [(9, 2), (42, 0), (106, 255), (107, 2), (108, 1)] {
        let mut invalid = encoded.clone();
        invalid[offset] = value;
        resign(&mut invalid);
        assert!(
            ArchiveManifestV1::decode(&invalid).is_err(),
            "offset {offset}"
        );
    }
}

#[test]
fn archive_manifest_frozen_vectors_preserve_both_encryption_declarations() {
    let pairs = [
        (
            ArchiveEncryptionPostureV1::Unencrypted,
            include_str!("../../../fixtures/replication/archive-manifest-v1-unencrypted-3.hex"),
            include_str!("../../../fixtures/replication/archive-manifest-v1-unencrypted-5.hex"),
        ),
        (
            ArchiveEncryptionPostureV1::OperatorManaged,
            include_str!(
                "../../../fixtures/replication/archive-manifest-v1-operator-managed-3.hex"
            ),
            include_str!(
                "../../../fixtures/replication/archive-manifest-v1-operator-managed-5.hex"
            ),
        ),
    ];
    for (posture, first, second) in pairs {
        let decode = |text: &str| {
            let bytes = text
                .trim()
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect::<Vec<_>>();
            let manifest = ArchiveManifestV1::decode(&bytes).unwrap();
            assert_eq!(manifest.encode(), bytes);
            manifest
        };
        let first = decode(first);
        let second = decode(second);
        assert_eq!(first.encryption_posture(), posture);
        assert_eq!(second.encryption_posture(), posture);
        first.verify_predecessor(None).unwrap();
        second.verify_predecessor(Some(&first)).unwrap();
        first
            .verify_frame(&frame(first.lineage(), first.before(), 3))
            .unwrap();
        second
            .verify_frame(&frame(second.lineage(), second.before(), 5))
            .unwrap();
    }
}

#[test]
fn archive_manifest_refuses_checksumming_over_impossible_ranges_and_lengths() {
    let encoded = first().0.encode();
    for (offset, bytes) in [
        (140, 0u64.to_be_bytes()),   // Zero backup receipt sequence.
        (198, 4u64.to_be_bytes()),   // Before lies after covered.
        (256, 1u64.to_be_bytes()),   // No receipt covered.
        (256, 258u64.to_be_bytes()), // More than the V3 receipt count bound.
        (314, 0u64.to_be_bytes()),   // Empty frame.
        (314, (MAX_CHANGELOG_FRAME_BYTES as u64 + 1).to_be_bytes()),
    ] {
        let mut invalid = encoded.clone();
        invalid[offset..offset + 8].copy_from_slice(&bytes);
        resign(&mut invalid);
        assert!(
            ArchiveManifestV1::decode(&invalid).is_err(),
            "offset {offset}"
        );
    }
}
