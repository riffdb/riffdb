// req: REP-005
use super::*;
use riffdb_storage_api::{ChangelogCursorErrorV3, ChangelogHistoryPointV3};

#[test]
fn primary_fence_source_evidence_binds_exact_retained_candidate_on_immutable_pins() {
    for profile in [
        crate::RedbCommitProfile::Standard,
        crate::RedbCommitProfile::Hardened,
    ] {
        let (path, mut ports, request) = attached_source(profile);
        let applied = history(&ports).tail();
        let active = ports.published_changelog_snapshot_v3().unwrap();
        assert!(
            active
                .primary_fence_source_evidence_v1(request, applied)
                .unwrap()
                .is_none()
        );
        let record = finish(&ports, request).unwrap();
        let fenced = ports.published_changelog_snapshot_v3().unwrap();
        let evidence = fenced
            .primary_fence_source_evidence_v1(request, applied)
            .unwrap()
            .unwrap();
        assert_eq!(evidence.fence(), &record);
        assert_eq!(evidence.applied(), applied);
        assert_eq!(evidence.source_history(), history(&ports));
        assert_eq!(evidence.application_rpo(), 0);
        assert!(
            active
                .primary_fence_source_evidence_v1(request, applied)
                .unwrap()
                .is_none()
        );
        submit_required_audit(&mut ports, 96);
        let latest = ports.published_changelog_snapshot_v3().unwrap();
        let after_audit = history(&ports).tail();
        assert!(after_audit.sequence() > evidence.source_history().tail().sequence());
        assert_eq!(
            latest
                .primary_fence_source_evidence_v1(request, after_audit)
                .unwrap()
                .unwrap()
                .application_rpo(),
            0
        );
        assert_eq!(
            fenced
                .primary_fence_source_evidence_v1(request, applied)
                .unwrap()
                .unwrap(),
            evidence
        );
        assert!(matches!(
            fenced.primary_fence_source_evidence_v1(request, after_audit),
            Err(ChangelogCursorErrorV3::InvalidPosition)
        ));
        drop((active, fenced, latest, ports));
        let reopened = crate::startup::open_validated_source_fixture(&path.0, profile, inputs());
        let recovered = reopened
            .published_changelog_snapshot_v3()
            .unwrap()
            .primary_fence_source_evidence_v1(request, applied)
            .unwrap()
            .unwrap();
        assert_eq!(recovered.fence(), &record);
        assert_eq!(recovered.applied(), applied);
    }
}

#[test]
fn primary_fence_source_evidence_refuses_selection_and_position_substitution() {
    for profile in [
        crate::RedbCommitProfile::Standard,
        crate::RedbCommitProfile::Hardened,
    ] {
        let (_path, ports, request) = attached_source(profile);
        let applied = history(&ports).tail();
        finish(&ports, request).unwrap();
        let pin = ports.published_changelog_snapshot_v3().unwrap();
        for case in 0..6 {
            let t = request.target();
            let target = ReplicationFollowerAuditTargetV1::new(
                if case == 0 {
                    riffdb_types::DatabaseId::from_unix_milliseconds_and_random(2, [2; 10]).unwrap()
                } else {
                    t.database_id()
                },
                t.history_incarnation() + u64::from(case == 1),
                riffdb_types::LeadershipEpochV1::new(
                    t.leadership_epoch().get() + u64::from(case == 2),
                )
                .unwrap(),
                if case == 3 {
                    ReplicationSourceHoldIdV1::new([0x7a; 16]).unwrap()
                } else {
                    t.hold_id()
                },
            )
            .unwrap();
            let changed = PrimaryFenceRequestV1::new(
                request.request_id(),
                if case == 4 {
                    ReplicationFenceOperationId::from_bytes(uuid_bytes(90)).unwrap()
                } else {
                    request.operation_id()
                },
                target,
                if case == 5 {
                    request.generation().checked_next().unwrap()
                } else {
                    request.generation()
                },
            );
            assert!(
                pin.primary_fence_source_evidence_v1(changed, applied)
                    .is_err()
            );
        }
        for candidate in [
            ChangelogHistoryPointV3::new(applied.sequence(), [0xa5; 32], applied.frontier()),
            ChangelogHistoryPointV3::new(
                applied.sequence(),
                applied.history_hash(),
                riffdb_types::DualFrontier::new(
                    Some(riffdb_types::CommitSequence::first()),
                    applied.frontier().administration(),
                ),
            ),
            ChangelogHistoryPointV3::new(
                history(&ports).tail().sequence().checked_next().unwrap(),
                applied.history_hash(),
                applied.frontier(),
            ),
        ] {
            assert!(
                pin.primary_fence_source_evidence_v1(request, candidate)
                    .is_err()
            );
        }
    }
}

#[test]
// req: REP-005, REP-006
fn primary_fence_source_evidence_keeps_pruned_registration_evidence_but_requires_retained_candidate()
 {
    use riffdb_storage_api::proto_codec::encode_changelog_history_state_v3;
    for profile in [
        crate::RedbCommitProfile::Standard,
        crate::RedbCommitProfile::Hardened,
    ] {
        let (path, ports, request) = attached_source(profile);
        let applied = history(&ports).tail();
        assert!(
            ports
                .replication_source_control()
                .advance_acknowledgement(Hold::new(
                    request.target().hold_id(),
                    Kind::FollowerAcknowledgement,
                    history(&ports).lineage(),
                    applied,
                ))
                .unwrap()
        );
        let record = finish(&ports, request).unwrap();
        let before = history(&ports);
        drop(ports);
        // Isolated valid retention fixture: no live publication or journal owner.
        // Retain the complete fence receipt and every live hold's exact position.
        let store = RedbStore::open_with_commit_profile(&path.0, profile).unwrap();
        let write = store.shared.database.begin_write().unwrap();
        {
            let mut receipts = write
                .open_table(crate::changelog_v3_activation::HISTORY)
                .unwrap();
            for seq in before.minimum_resume().sequence().get()..applied.sequence().get() {
                assert!(
                    receipts
                        .remove(seq.to_be_bytes().as_slice())
                        .unwrap()
                        .is_some()
                );
            }
        }
        let pruned =
            History::new(before.lineage(), before.anchor(), before.tail(), applied).unwrap();
        let bytes = encode_changelog_history_state_v3(pruned).unwrap();
        write
            .open_table(crate::layout::META)
            .unwrap()
            .insert(
                riffdb_storage_api::AuthoritativeNamespaceV1::ChangelogHistoryState
                    .metadata_key()
                    .unwrap(),
                bytes.as_bytes(),
            )
            .unwrap();
        assert_eq!(
            crate::changelog_v3_roots::validate_retained_history_for_write(&write).unwrap(),
            Some(pruned)
        );
        store.shared.commit_durable(write).unwrap();
        drop(store);
        let reopened = crate::startup::open_validated_source_fixture(&path.0, profile, inputs());
        let pin = reopened.published_changelog_snapshot_v3().unwrap();
        let evidence = pin
            .primary_fence_source_evidence_v1(request, applied)
            .unwrap()
            .unwrap();
        assert_eq!(evidence.fence(), &record);
        assert_eq!(evidence.applied(), applied);
        assert!(
            pin.primary_fence_source_evidence_v1(request, before.anchor())
                .is_err()
        );
    }
}
