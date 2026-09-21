#![forbid(unsafe_code)]
// req: REP-003, REP-005, STO-012, REC-001
//! Exact catalog identity survives roots and control evidence without enabling a writer.
use riffdb_storage_api::{
    AuthoritativeStateCatalogV1, AuthoritativeStateCatalogV2, ChangelogFrameV3,
    ChangelogHistoryPointV3 as Point, ChangelogHistoryStateV3 as History,
    ChangelogLineageV3 as Lineage, ChangelogTransactionSequence as Sequence, LeadershipEpochV1,
    MAX_CHANGELOG_FRAME_BYTES, MAX_STAGED_COMMANDS, ReplicationFollowerStateV3,
    ReplicationHandshakeV3, ReplicationPrimaryAdmissionV1, ReplicationSourceHoldIdV1,
    ReplicationSourceHoldKindV1, ReplicationSourceHoldV1, ReplicationStreamErrorV3,
    proto_codec::{
        decode_changelog_history_state_v3, decode_replication_follower_state_v3,
        decode_replication_source_hold_v1, encode_changelog_history_state_v3,
        encode_replication_follower_state_v3, encode_replication_source_hold_v1,
    },
};
use riffdb_types::{AdministrationSequence, CommitSequence, DatabaseId, DualFrontier};

fn database() -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10]).unwrap()
}
fn lineage(catalog: [u8; 32]) -> Lineage {
    Lineage::new_with_catalog(database(), 2, LeadershipEpochV1::new(3).unwrap(), catalog).unwrap()
}
fn point() -> Point {
    Point::new(
        Sequence::new(9).unwrap(),
        [0x55; 32],
        DualFrontier::new(CommitSequence::new(5), AdministrationSequence::new(2)),
    )
}

#[test]
fn checked_lineage_preserves_the_selected_catalog_and_refuses_foreign_identity() {
    let old = Lineage::new(database(), 2, LeadershipEpochV1::new(3).unwrap()).unwrap();
    let new = lineage(AuthoritativeStateCatalogV2.digest());
    assert_eq!(old.catalog_digest(), AuthoritativeStateCatalogV1.digest());
    assert_eq!(new.catalog_digest(), AuthoritativeStateCatalogV2.digest());
    assert_ne!(old, new);
    assert!(
        Lineage::new_with_catalog(database(), 2, LeadershipEpochV1::initial(), [0x77; 32]).is_err()
    );
    assert!(
        Lineage::new_with_catalog(
            database(),
            0,
            LeadershipEpochV1::initial(),
            AuthoritativeStateCatalogV2.digest()
        )
        .is_err()
    );
    assert!(ReplicationPrimaryAdmissionV1::active(old).is_err());
    assert_eq!(
        ReplicationPrimaryAdmissionV1::active(new)
            .unwrap()
            .lineage(),
        new
    );
}

#[test]
fn history_follower_and_hold_codecs_preserve_both_catalog_bindings() {
    for catalog in [
        AuthoritativeStateCatalogV1.digest(),
        AuthoritativeStateCatalogV2.digest(),
    ] {
        let source = lineage(catalog);
        let history = History::new(source, point(), point(), point()).unwrap();
        assert_eq!(
            *decode_changelog_history_state_v3(
                encode_changelog_history_state_v3(history)
                    .unwrap()
                    .as_bytes()
            )
            .unwrap()
            .value(),
            history
        );
        let follower =
            ReplicationFollowerStateV3::attached(source, point(), Some(point())).unwrap();
        assert_eq!(
            *decode_replication_follower_state_v3(
                encode_replication_follower_state_v3(follower)
                    .unwrap()
                    .as_bytes()
            )
            .unwrap()
            .value(),
            follower
        );
        let hold = ReplicationSourceHoldV1::new(
            ReplicationSourceHoldIdV1::new([7; 16]).unwrap(),
            ReplicationSourceHoldKindV1::FollowerAcknowledgement,
            source,
            point(),
        );
        assert_eq!(
            *decode_replication_source_hold_v1(
                encode_replication_source_hold_v1(hold).unwrap().as_bytes()
            )
            .unwrap()
            .value(),
            hold
        );
    }
}

#[test]
fn negotiation_requires_the_exact_catalog_of_the_selected_lineage() {
    for selected in [
        AuthoritativeStateCatalogV1.digest(),
        AuthoritativeStateCatalogV2.digest(),
    ] {
        for requested in [
            AuthoritativeStateCatalogV1.digest(),
            AuthoritativeStateCatalogV2.digest(),
            [0; 32],
        ] {
            let result = ReplicationHandshakeV3::new(
                lineage(selected),
                point(),
                ChangelogFrameV3::IDENTITY,
                requested,
                MAX_CHANGELOG_FRAME_BYTES as u64,
                MAX_STAGED_COMMANDS as u64,
            );
            if selected == requested {
                assert_eq!(result.unwrap().lineage(), lineage(selected));
            } else {
                assert_eq!(result, Err(ReplicationStreamErrorV3::UnsupportedCatalog));
            }
        }
    }
}

#[path = "support/archive_manifest_fixture.rs"]
mod archive_fixture;

#[derive(Default)]
struct Capture(Option<riffdb_storage_api::ArchiveManifestV1>);
impl riffdb_storage_api::ArchiveFrameSinkV1 for Capture {
    fn persist(
        &mut self,
        frame: &riffdb_storage_api::ArchiveFrameV1,
    ) -> Result<(), riffdb_storage_api::ArchiveConsumerErrorV1> {
        let manifest = riffdb_storage_api::ArchiveManifestV1::first(
            frame,
            [0x71; 32],
            riffdb_storage_api::ArchiveEncryptionPostureV1::Unencrypted,
        )?;
        manifest.verify_frame(frame.as_bytes())?;
        self.0 = Some(manifest);
        Ok(())
    }
}

#[test]
fn archive_manifest_preserves_catalog_without_relabeling_frame_or_backup_evidence() {
    use riffdb_storage_api::{ArchiveConsumerV1, ArchiveManifestV1};
    use sha2::{Digest, Sha256};
    let (_, before) = archive_fixture::fixture();
    for catalog in [
        AuthoritativeStateCatalogV1.digest(),
        AuthoritativeStateCatalogV2.digest(),
    ] {
        let source = lineage(catalog);
        let bytes = archive_fixture::frame(source, before, 1);
        let mut archive = ArchiveConsumerV1::new(Capture::default(), source, before);
        archive.append(bytes.clone()).unwrap();
        let manifest = archive.into_sink().0.unwrap();
        assert_eq!(
            ArchiveManifestV1::decode(&manifest.encode()).unwrap(),
            manifest
        );
        assert_eq!(manifest.lineage(), source);
        manifest.verify_frame(&bytes).unwrap();
        let other = if catalog == AuthoritativeStateCatalogV1.digest() {
            AuthoritativeStateCatalogV2.digest()
        } else {
            AuthoritativeStateCatalogV1.digest()
        };
        // Substitute the supported catalog and recompute the outer checksum:
        // decoding the descriptor is not proof of its binding to source bytes.
        let mut substituted = manifest.encode();
        substituted[42..74].copy_from_slice(&other);
        let end = substituted.len() - 32;
        let checksum: [u8; 32] = Sha256::digest(&substituted[..end]).into();
        substituted[end..].copy_from_slice(&checksum);
        let foreign_manifest = ArchiveManifestV1::decode(&substituted).unwrap();
        assert_eq!(
            foreign_manifest.verify_frame(&bytes),
            Err(riffdb_storage_api::ArchiveConsumerErrorV1::ForeignLineage)
        );
        assert!(
            foreign_manifest
                .verify_predecessor(Some(&manifest))
                .is_err()
        );
        substituted[42..74].fill(0);
        let checksum: [u8; 32] = Sha256::digest(&substituted[..end]).into();
        substituted[end..].copy_from_slice(&checksum);
        assert!(ArchiveManifestV1::decode(&substituted).is_err());
        let mut foreign = ArchiveConsumerV1::new(Capture::default(), lineage(other), before);
        assert_eq!(
            foreign.append(bytes),
            Err(riffdb_storage_api::ArchiveConsumerErrorV1::ForeignLineage)
        );
        assert_eq!(foreign.position(), before);
        assert!(!foreign.has_pending_frame());
        assert!(foreign.into_sink().0.is_none());
    }
}

#[test]
fn successor_history_validates_fence_and_preserves_exact_catalog_on_roundtrip() {
    let encoded =
        include_str!("../../../fixtures/replication/changelog-frame-v3-primary-fence.hex");
    let bytes: Vec<_> = encoded
        .trim()
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect();
    let frame = ChangelogFrameV3::decode(&bytes).unwrap();
    let receipt = &frame.receipts()[0];
    let source = lineage(AuthoritativeStateCatalogV2.digest());
    let history = History::new(source, point(), point(), point()).unwrap();
    let successor = history.advance(receipt).unwrap();
    successor.validate_terminal_receipt(receipt).unwrap();
    assert_eq!(successor.lineage(), source);
    assert_eq!(
        successor.tail().frontier().application(),
        point().frontier().application()
    );
    let encoded = encode_changelog_history_state_v3(successor).unwrap();
    let decoded = *decode_changelog_history_state_v3(encoded.as_bytes())
        .unwrap()
        .value();
    assert_eq!(decoded, successor);
    decoded.validate_terminal_receipt(receipt).unwrap();
}

#[test]
fn catalog_bound_roots_and_archive_have_canonical_vectors() {
    use riffdb_storage_api::ArchiveConsumerV1;
    use std::fmt::Write;
    let mut vectors = String::new();
    for (name, catalog) in [
        ("v1", AuthoritativeStateCatalogV1.digest()),
        ("v2", AuthoritativeStateCatalogV2.digest()),
    ] {
        let source = lineage(catalog);
        let history = History::new(source, point(), point(), point()).unwrap();
        let follower =
            ReplicationFollowerStateV3::attached(source, point(), Some(point())).unwrap();
        let hold = ReplicationSourceHoldV1::new(
            ReplicationSourceHoldIdV1::new([7; 16]).unwrap(),
            ReplicationSourceHoldKindV1::FollowerAcknowledgement,
            source,
            point(),
        );
        let (_, before) = archive_fixture::fixture();
        let mut archive = ArchiveConsumerV1::new(Capture::default(), source, before);
        archive
            .append(archive_fixture::frame(source, before, 1))
            .unwrap();
        for (role, bytes) in [
            (
                "history",
                encode_changelog_history_state_v3(history)
                    .unwrap()
                    .as_bytes()
                    .to_vec(),
            ),
            (
                "follower",
                encode_replication_follower_state_v3(follower)
                    .unwrap()
                    .as_bytes()
                    .to_vec(),
            ),
            (
                "hold",
                encode_replication_source_hold_v1(hold)
                    .unwrap()
                    .as_bytes()
                    .to_vec(),
            ),
            ("archive", archive.into_sink().0.unwrap().encode()),
            ("bootstrap", bootstrap_manifest(source).encode().unwrap()),
        ] {
            write!(&mut vectors, "{name}-{role} ").unwrap();
            for byte in bytes {
                write!(&mut vectors, "{byte:02x}").unwrap();
            }
            vectors.push('\n');
        }
    }
    if let Some(path) = std::env::var_os("RIFFDB_CATALOG_LINEAGE_VECTOR_OUTPUT") {
        std::fs::write(path, vectors).unwrap();
    } else {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/replication/catalog-lineage-v3.hex");
        assert_eq!(std::fs::read_to_string(path).unwrap(), vectors);
    }
}

fn bootstrap_manifest(source: Lineage) -> riffdb_storage_api::ReplicationBootstrapManifestV1 {
    use riffdb_storage_api::{
        AuthoritativeNamespaceV1 as N, ReplicationAuthorityClassV1 as Class,
        ReplicationBootstrapFenceV3 as Fence, ReplicationBootstrapPageV3 as Page,
        ReplicationBootstrapTranscriptV3 as Transcript,
    };
    let history = History::new(source, point(), point(), point()).unwrap();
    let fence = Fence::new(ReplicationSourceHoldIdV1::new([7; 16]).unwrap(), history);
    let mut transcript = Transcript::new(fence);
    let mut prior = fence.digest();
    for (i, namespace) in N::ALL
        .into_iter()
        .filter(|n| n.class() == Class::ReplicatedAuthoritative)
        .enumerate()
    {
        let page = Page::new(fence.digest(), i as u32 + 1, namespace, true, prior, vec![]).unwrap();
        prior = *page.encode().unwrap().last_chunk::<32>().unwrap();
        transcript.observe(&page).unwrap();
    }
    transcript.manifest().unwrap()
}

#[test]
fn bootstrap_manifest_and_restart_progress_keep_catalog_bound_transcript() {
    use riffdb_storage_api::{
        ReplicationBootstrapManifestV1 as Manifest, ReplicationBootstrapProgressV1 as Progress,
        ReplicationBootstrapTranscriptV3 as Transcript,
    };
    let old = bootstrap_manifest(lineage(AuthoritativeStateCatalogV1.digest()));
    let new = bootstrap_manifest(lineage(AuthoritativeStateCatalogV2.digest()));
    assert_ne!(old.fence().digest(), new.fence().digest());
    for manifest in [old, new] {
        assert_eq!(
            Manifest::decode(&manifest.encode().unwrap()).unwrap(),
            manifest
        );
        let transcript = Transcript::new(manifest.fence());
        let progress = transcript.checkpoint(manifest).unwrap();
        assert_eq!(
            Progress::decode(&progress.encode().unwrap()).unwrap(),
            progress
        );
        let other = if manifest == old { new } else { old };
        assert!(transcript.checkpoint(other).is_err());
    }
}
