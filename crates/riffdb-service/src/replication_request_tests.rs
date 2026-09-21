// req: REP-003, REP-005
use super::*;

fn manifest_bytes() -> Vec<u8> {
    include_str!("../../../fixtures/replication/bootstrap-manifest-v1.hex")
        .trim()
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn attachment() -> ReplicationRequest {
    let bytes = manifest_bytes();
    let manifest = ReplicationBootstrapManifestV1::decode(&bytes).unwrap();
    let history = manifest.fence().history();
    let lineage = history.lineage();
    ReplicationRequest {
        database_id: lineage.database_id(),
        history_incarnation: lineage.history_incarnation(),
        leadership_epoch: lineage.leadership_epoch().get(),
        after_sequence: history.tail().sequence().get(),
        after_hash: history.tail().history_hash(),
        after_frontier: history.tail().frontier(),
        readable_format: "riffdb-changelog-v3".into(),
        catalog_digest: lineage.catalog_digest(),
        maximum_frame_bytes: 32 * 1024 * 1024,
        maximum_transitions: 256,
        phase: ReplicationPhase::Attach { manifest: bytes },
    }
}

#[test]
fn targets_are_empty_for_tail_and_exactly_request_selected_for_every_follower_phase() {
    let mut request = attachment();
    let manifest = ReplicationBootstrapManifestV1::decode(&manifest_bytes()).unwrap();
    let hold = manifest.fence().hold_id();
    let expected = ServiceAuditTargetsV1::new([ServiceAuditTargetV1::ReplicationFollower(
        ReplicationFollowerAuditTargetV1::new(
            request.database_id,
            request.history_incarnation,
            manifest.fence().history().lineage().leadership_epoch(),
            hold,
        )
        .unwrap(),
    )])
    .unwrap();
    assert_eq!(request.checked_audit_targets().unwrap(), expected);
    request.phase = ReplicationPhase::Follower {
        hold_id: *hold.as_bytes(),
    };
    assert_eq!(request.checked_audit_targets().unwrap(), expected);
    request.phase = ReplicationPhase::Tail;
    assert_eq!(
        request.checked_audit_targets().unwrap(),
        ServiceAuditTargetsV1::empty()
    );
    request.after_sequence = 0;
    request.after_hash = [0; 32];
    request.after_frontier = DualFrontier::INITIAL;
    for (resume_manifest, after_page) in [(vec![], 0), (manifest_bytes(), manifest.page_count())] {
        request.phase = ReplicationPhase::Bootstrap {
            hold_id: *hold.as_bytes(),
            resume_manifest,
            after_page,
        };
        assert_eq!(request.checked_audit_targets().unwrap(), expected);
    }
}

#[test]
fn corrupt_or_substituted_manifest_cannot_select_an_audit_target() {
    let original = attachment();
    for change in 0..5 {
        let mut request = original.clone();
        match change {
            0 => {
                request.database_id =
                    riffdb_types::DatabaseId::from_unix_milliseconds_and_random(1, [3; 10]).unwrap()
            }
            1 => request.history_incarnation += 1,
            2 => request.leadership_epoch += 1,
            3 => request.catalog_digest[0] ^= 1,
            _ => request.leadership_epoch = 0,
        }
        assert_eq!(request.checked_audit_targets(), Err(invalid()));
    }
    let bytes = manifest_bytes();
    let mut invalid_manifests = vec![vec![], vec![0; 513]];
    for length in 0..bytes.len() {
        invalid_manifests.push(bytes[..length].to_vec());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    invalid_manifests.push(trailing);
    for index in 0..bytes.len() {
        let mut corrupt = bytes.clone();
        corrupt[index] ^= 1;
        invalid_manifests.push(corrupt);
    }
    for manifest in invalid_manifests {
        let mut request = original.clone();
        request.phase = ReplicationPhase::Attach { manifest };
        assert_eq!(request.checked_audit_targets(), Err(invalid()));
    }
}

#[test]
fn resumed_bootstrap_rejects_substituted_hold_and_out_of_manifest_page() {
    let mut request = attachment();
    let manifest = ReplicationBootstrapManifestV1::decode(&manifest_bytes()).unwrap();
    request.after_sequence = 0;
    request.after_hash = [0; 32];
    request.after_frontier = DualFrontier::INITIAL;
    for (hold_id, resume_manifest, after_page) in [
        ([0; 16], manifest_bytes(), 0),
        ([0xff; 16], manifest_bytes(), 0),
        (
            *manifest.fence().hold_id().as_bytes(),
            manifest_bytes(),
            manifest.page_count() + 1,
        ),
        (*manifest.fence().hold_id().as_bytes(), vec![], 1),
    ] {
        request.phase = ReplicationPhase::Bootstrap {
            hold_id,
            resume_manifest,
            after_page,
        };
        assert_eq!(request.checked_audit_targets(), Err(invalid()));
    }
}
