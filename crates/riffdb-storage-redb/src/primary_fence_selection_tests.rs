// req: REP-005, STO-012
use super::*;
use riffdb_storage_api::PrimaryFenceRefusalV1 as Refusal;

#[test]
fn primary_fence_selection_refuses_retired_registration_without_new_allocation() {
    let (_path, ports, request) = attached_source(crate::RedbCommitProfile::Standard);
    let retirement = ReplicationAdministrationRequestV1::retire(
        request_id(60),
        request.target(),
        request.generation(),
    );
    let candidate =
        ReplicationAdministrationCandidateV1::new(retirement, principal(capability_id(3)));
    let (awaiting, _) = ports
        .begin_replication_administration(candidate.clone())
        .unwrap()
        .read_transaction_current()
        .unwrap();
    assert!(matches!(
        awaiting
            .commit(ReplicationAdministrationIntentV1::new(
                candidate,
                Timestamp::new(17, 0).unwrap()
            ))
            .unwrap(),
        ReplicationAdministrationResultV1::Applied(_)
    ));
    let before = history(&ports);
    let candidate = PrimaryFenceCandidateV1::new(request, principal(capability_id(3)));
    let (awaiting, _) = ports
        .begin_primary_fence_transaction(candidate.clone())
        .unwrap()
        .read_transaction_current()
        .unwrap();
    assert_eq!(
        awaiting
            .commit(PrimaryFenceIntentV1::new(
                candidate,
                Timestamp::new(18, 0).unwrap()
            ))
            .unwrap(),
        PrimaryFenceResultV1::Refused(Refusal::RegistrationMissingOrStale)
    );
    assert_eq!(history(&ports), before);
    assert!(
        ports
            .read_replication_primary_admission()
            .unwrap()
            .fence()
            .is_none()
    );
}

#[test]
fn primary_fence_selection_never_masks_corrupt_source_as_request_refusal() {
    let (_path, ports, request) = attached_source(crate::RedbCommitProfile::Standard);
    let key = riffdb_storage_api::AuthoritativeNamespaceV2::ReplicationPrimaryAdmission
        .metadata_key()
        .unwrap();
    let before = history(&ports);
    let write = ports.shared.database.begin_write().unwrap();
    write
        .open_table(crate::layout::META)
        .unwrap()
        .remove(key)
        .unwrap();
    ports.shared.commit_durable(write).unwrap();
    let target = request.target();
    let foreign = ReplicationFollowerAuditTargetV1::new(
        target.database_id(),
        target.history_incarnation() + 1,
        target.leadership_epoch(),
        target.hold_id(),
    )
    .unwrap();
    let selected = PrimaryFenceRequestV1::new(
        request.request_id(),
        request.operation_id(),
        foreign,
        request.generation(),
    );
    let error = ports
        .begin_primary_fence_transaction(PrimaryFenceCandidateV1::new(
            selected,
            principal(capability_id(3)),
        ))
        .err()
        .expect("corruption precedes request refusal");
    assert_eq!(error.kind(), StorageErrorKind::CorruptData);
    assert!(!ports.primary_fence_lease_is_held());
    let root = ports.shared.database.begin_read().unwrap();
    let meta = root.open_table(crate::layout::META).unwrap();
    assert!(meta.get(key).unwrap().is_none(), "no admission repair");
    let history_key = riffdb_storage_api::AuthoritativeNamespaceV1::ChangelogHistoryState
        .metadata_key()
        .unwrap();
    let row = meta.get(history_key).unwrap().unwrap();
    assert_eq!(
        *riffdb_storage_api::proto_codec::decode_changelog_history_state_v3(row.value())
            .unwrap()
            .value(),
        before
    );
}

#[test]
fn primary_fence_selection_refusals_preserve_valid_source_and_current_authority() {
    for profile in [
        crate::RedbCommitProfile::Standard,
        crate::RedbCommitProfile::Hardened,
    ] {
        let (_path, ports, request) = attached_source(profile);
        let shared = ports.shared_ports();
        let target = request.target();
        let targets = [
            (
                ReplicationFollowerAuditTargetV1::new(
                    DatabaseId::from_bytes(uuid_bytes(99)).unwrap(),
                    target.history_incarnation(),
                    target.leadership_epoch(),
                    target.hold_id(),
                )
                .unwrap(),
                Refusal::LineageMismatch,
            ),
            (
                ReplicationFollowerAuditTargetV1::new(
                    target.database_id(),
                    target.history_incarnation() + 1,
                    target.leadership_epoch(),
                    target.hold_id(),
                )
                .unwrap(),
                Refusal::LineageMismatch,
            ),
            (
                ReplicationFollowerAuditTargetV1::new(
                    target.database_id(),
                    target.history_incarnation(),
                    riffdb_types::LeadershipEpochV1::new(target.leadership_epoch().get() + 1)
                        .unwrap(),
                    target.hold_id(),
                )
                .unwrap(),
                Refusal::LineageMismatch,
            ),
            (
                ReplicationFollowerAuditTargetV1::new(
                    target.database_id(),
                    target.history_incarnation(),
                    target.leadership_epoch(),
                    ReplicationSourceHoldIdV1::new([0x80; 16]).unwrap(),
                )
                .unwrap(),
                Refusal::RegistrationMissingOrStale,
            ),
        ];
        let before = history(&ports);
        let mut selections = targets
            .into_iter()
            .map(|(target, refusal)| {
                (
                    PrimaryFenceRequestV1::new(
                        request.request_id(),
                        request.operation_id(),
                        target,
                        request.generation(),
                    ),
                    refusal,
                )
            })
            .collect::<Vec<_>>();
        selections.push((
            PrimaryFenceRequestV1::new(
                request.request_id(),
                request.operation_id(),
                target,
                request.generation().checked_next().unwrap(),
            ),
            Refusal::RegistrationMissingOrStale,
        ));
        for (selected, refusal) in selections {
            let candidate = PrimaryFenceCandidateV1::new(selected, principal(capability_id(3)));
            let (awaiting, current) = shared
                .begin_primary_fence_transaction(candidate.clone())
                .unwrap()
                .read_transaction_current()
                .unwrap();
            assert_eq!(
                current.unwrap().database_id(),
                target.database_id(),
                "authority comes from the stored source, never the selected database"
            );
            assert_eq!(
                awaiting
                    .commit(PrimaryFenceIntentV1::new(
                        candidate,
                        Timestamp::new(16, 0).unwrap()
                    ))
                    .unwrap(),
                PrimaryFenceResultV1::Refused(refusal)
            );
            assert!(!ports.primary_fence_lease_is_held());
            assert_eq!(history(&ports), before);
            assert!(
                ports
                    .read_replication_primary_admission()
                    .unwrap()
                    .fence()
                    .is_none()
            );
        }
        let record = finish(&shared, request).unwrap();
        let frozen = history(&ports);
        let conflicting = PrimaryFenceRequestV1::new(
            request_id(90),
            ReplicationFenceOperationId::from_bytes(uuid_bytes(91)).unwrap(),
            target,
            request.generation(),
        );
        let candidate = PrimaryFenceCandidateV1::new(conflicting, principal(capability_id(3)));
        let (awaiting, _) = shared
            .begin_primary_fence_transaction(candidate.clone())
            .unwrap()
            .read_transaction_current()
            .unwrap();
        assert_eq!(
            awaiting
                .commit(PrimaryFenceIntentV1::new(
                    candidate,
                    Timestamp::new(17, 0).unwrap()
                ))
                .unwrap(),
            PrimaryFenceResultV1::Refused(Refusal::FenceConflict)
        );
        assert_eq!(history(&ports), frozen);
        assert_eq!(finish(&shared, request).unwrap(), record);
    }
}
