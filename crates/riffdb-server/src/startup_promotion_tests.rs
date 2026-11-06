//! Production startup over a real materialized follower and exact cutover.
//! The fence below is storage evidence only; authenticated live promotion is a
//! separate runtime drill. No supplied receipt is used as transport trust.
// req: REP-005, REC-001, STO-012
use super::*;
use riffdb_storage_api::*;
use riffdb_storage_redb::{RedbBootstrapMaterializer, RedbBootstrapStage};
use riffdb_types::*;
use std::num::NonZeroU64;

pub(crate) fn inputs() -> StartupValidationInputs {
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
    StartupValidationInputs::new(
        Timestamp::new(2000, 0).unwrap(),
        ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
        ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
    )
}

fn pending(history: ChangelogHistoryStateV3) -> ReplicationPromotionReceiptV1 {
    let lineage = history.lineage();
    let target = ReplicationFollowerAuditTargetV1::new(
        lineage.database_id(),
        lineage.history_incarnation(),
        lineage.leadership_epoch(),
        ReplicationSourceHoldIdV1::new([0x73; 16]).unwrap(),
    )
    .unwrap();
    let principal = AuditPrincipalV1::new(
        ActorId::new("promotion-operator").unwrap(),
        ActorKind::Human,
        CapabilityId::from_unix_milliseconds_and_random(1234, [8; 10]).unwrap(),
        NonZeroU64::new(1).unwrap(),
    );
    let observed = ChangelogHistoryPointV3::new(
        history.tail().sequence().checked_next().unwrap(),
        [9; 32],
        DualFrontier::new(
            history.tail().frontier().application(),
            Some(AdministrationSequence::first()),
        ),
    );
    let fence = StoredPrimaryFenceAdministrationV1::new(
        AdministrationSequence::new(2).unwrap(),
        Timestamp::new(1234, 0).unwrap(),
        ReplicationFenceOperationId::from_unix_milliseconds_and_random(1234, [3; 10]).unwrap(),
        RequestId::from_unix_milliseconds_and_random(1234, [4; 10]).unwrap(),
        principal.clone(),
        None,
        target,
        history.anchor().sequence(),
        observed,
    )
    .unwrap();
    let fenced = ChangelogHistoryPointV3::new(
        observed.sequence().checked_next().unwrap(),
        [5; 32],
        DualFrontier::new(
            history.tail().frontier().application(),
            Some(fence.administration_sequence()),
        ),
    );
    let source =
        ChangelogHistoryStateV3::new(lineage, history.anchor(), fenced, history.minimum_resume())
            .unwrap();
    let request = ReplicationPromotionRequestV1::new(
        ReplicationPromotionOperationId::from_unix_milliseconds_and_random(1234, [6; 10]).unwrap(),
        fence.operation_id(),
        target,
        fence.generation(),
    );
    let selection = ReplicationPromotionSelectionV1::new(
        request,
        ReplicationFollowerStateV3::attached(lineage, history.tail(), Some(history.anchor()))
            .unwrap(),
        PrimaryFenceSourceEvidenceV1::new(fence, history.tail(), source).unwrap(),
    )
    .unwrap();
    let mut attempt = ReplicationPromotionReceiptV1::attempted(
        request,
        RequestId::from_unix_milliseconds_and_random(1234, [7; 10]).unwrap(),
        principal,
        None,
        Timestamp::new(1234, 1).unwrap(),
    );
    for phase in [
        ReplicationPromotionPhaseV1::Draining,
        ReplicationPromotionPhaseV1::Offline,
    ] {
        attempt
            .advance(ReplicationPromotionStepV1::Phase(phase))
            .unwrap();
    }
    attempt.record_selection(selection).unwrap();
    attempt
        .advance(ReplicationPromotionStepV1::Phase(
            ReplicationPromotionPhaseV1::CutoverPending,
        ))
        .unwrap();
    attempt
}

pub(crate) fn fixture(root: &Path) -> (RedbMaintenanceStorage, StoredPromotionAdministrationV1) {
    fixture_with_selection_result(
        root,
        ReplicationPromotionStepV1::Phase(ReplicationPromotionPhaseV1::CutoverPending),
    )
}

pub(crate) fn fixture_with_selection_result(
    root: &Path,
    step: ReplicationPromotionStepV1,
) -> (RedbMaintenanceStorage, StoredPromotionAdministrationV1) {
    let source_path = root.join("source.redb");
    let source = complete_redb_startup(RedbStore::open(&source_path).unwrap(), inputs(), || {
        Ok(DatabaseId::from_unix_milliseconds_and_random(1234, [1; 10]).unwrap())
    })
    .unwrap();
    let held = source
        .operational_ports
        .prepare_replication_bootstrap_v3(
            &root.join("source-transfer"),
            ReplicationSourceHoldIdV1::new([0x73; 16]).unwrap(),
        )
        .unwrap();
    let manifest = held.manifest();
    let mut receiving =
        RedbBootstrapStage::create(&root.join("receiver-transfer"), manifest).unwrap();
    for ordinal in 1..=manifest.page_count() {
        receiving
            .append(&held.read_page(ordinal).unwrap().encode().unwrap())
            .unwrap();
    }
    let mut materializer = RedbBootstrapMaterializer::create(
        &root.join("candidate"),
        receiving.into_materialization_input().unwrap(),
    )
    .unwrap();
    while materializer.copy_next_page().unwrap().is_some() {}
    drop(materializer.finish().unwrap().validate(inputs()).unwrap());
    drop(held);
    drop(source);
    // Recovery has only local committed evidence; the old source file is gone.
    std::fs::remove_file(source_path).unwrap();
    let mut owner = RedbMaintenanceStorage::open_for_promotion_recovery(
        root.join("candidate/follower.redb"),
        root.join("backups"),
    )
    .unwrap();
    let attempt = pending(manifest.fence().history());
    let mut published = ReplicationPromotionReceiptV1::attempted(
        attempt.request(),
        attempt.request_id(),
        attempt.principal().clone(),
        attempt.approval_id().cloned(),
        attempt.timestamp(),
    );
    owner.persist_promotion_receipt(&published).unwrap();
    for phase in [
        ReplicationPromotionPhaseV1::Draining,
        ReplicationPromotionPhaseV1::Offline,
    ] {
        published
            .advance(ReplicationPromotionStepV1::Phase(phase))
            .unwrap();
        owner.persist_promotion_receipt(&published).unwrap();
    }
    published
        .record_selection(attempt.selection().unwrap().clone())
        .unwrap();
    owner.persist_promotion_receipt(&published).unwrap();
    published.advance(step).unwrap();
    owner.persist_promotion_receipt(&published).unwrap();
    let record = StoredPromotionAdministrationV1::new(
        attempt,
        Timestamp::new(1234, 2).unwrap(),
        ServiceIngressKindV1::Grpc,
    )
    .unwrap();
    (owner, record)
}

#[test]
fn promoted_startup_joins_ordinary_proofs_and_seeds_the_new_lineage() {
    for profile in [RedbCommitProfile::Standard, RedbCommitProfile::Hardened] {
        let scope = tempfile::tempdir().unwrap();
        let (mut owner, record) = fixture(scope.path());
        owner.apply_promotion_cutover(&record).unwrap();
        for _ in 0..2 {
            let mut checked = reconcile_promoted_redb_startup(
                &mut owner,
                &record,
                inputs(),
                Arc::new(AtomicBool::new(false)),
                profile,
            )
            .expect("committed promotion must pass the ordinary server proof join");
            let selected = record.attempt().selection().unwrap();
            assert_eq!(
                checked.database_id(),
                selected.published_lineage().database_id()
            );
            assert_eq!(
                checked.retained_metadata().history_incarnation(),
                selected.published_lineage().history_incarnation()
            );
            assert_eq!(
                checked.lifecycle(),
                ValidatedStartupLifecycle::BootstrapRequired
            );
            let mut publications = checked.take_replication_publications().unwrap();
            let snapshot = publications.latest().unwrap().unwrap();
            let history = snapshot.authoritative_state_v3().unwrap().history();
            assert_eq!(history.lineage(), selected.published_lineage());
            assert_eq!(history.anchor().frontier(), record.covered_frontier());
            assert_eq!(
                history.tail().frontier().application(),
                selected.applied().frontier().application()
            );
            assert_eq!(
                owner.promotion_receipts().unwrap().receipts()[0].phase(),
                ReplicationPromotionPhaseV1::Succeeded
            );
            drop(snapshot);
            drop(publications);
            drop(checked);
            assert!(
                RedbStore::open(owner.configured_database_file()).is_err(),
                "ordinary open must still require exact promotion reconciliation"
            );
        }
    }
}

#[test]
fn promoted_startup_refuses_before_cutover_and_preserves_the_pending_attempt() {
    let scope = tempfile::tempdir().unwrap();
    let (mut owner, record) = fixture(scope.path());
    assert!(
        reconcile_promoted_redb_startup(
            &mut owner,
            &record,
            inputs(),
            Arc::new(AtomicBool::new(false)),
            RedbCommitProfile::Standard,
        )
        .is_err()
    );
    assert_eq!(
        owner.promotion_receipts().unwrap().receipts(),
        &[record.attempt().clone()]
    );
    assert!(riffdb_storage_redb::RedbFollowerStore::open(owner.configured_database_file()).is_ok());
}

#[test]
fn promoted_startup_refuses_missing_external_attempt_after_restart() {
    let scope = tempfile::tempdir().unwrap();
    let (mut owner, record) = fixture(scope.path());
    owner.apply_promotion_cutover(&record).unwrap();
    drop(owner);
    let receipt = scope
        .path()
        .join("backups/.maintenance/replication_promotion")
        .join(format!("{}.receipt-v1", record.attempt().request_id()));
    std::fs::remove_file(receipt).unwrap();
    let mut owner = RedbMaintenanceStorage::open_for_promotion_recovery(
        scope.path().join("candidate/follower.redb"),
        scope.path().join("backups"),
    )
    .unwrap();
    assert!(
        reconcile_promoted_redb_startup(
            &mut owner,
            &record,
            inputs(),
            Arc::new(AtomicBool::new(false)),
            RedbCommitProfile::Standard,
        )
        .is_err()
    );
    assert!(owner.promotion_receipts().unwrap().receipts().is_empty());
}

#[test]
fn promoted_startup_cancellation_releases_engine_custody_without_advancing_the_attempt() {
    let scope = tempfile::tempdir().unwrap();
    let (mut owner, record) = fixture(scope.path());
    owner.apply_promotion_cutover(&record).unwrap();
    assert!(
        reconcile_promoted_redb_startup(
            &mut owner,
            &record,
            inputs(),
            Arc::new(AtomicBool::new(true)),
            RedbCommitProfile::Standard,
        )
        .is_err()
    );
    assert_eq!(
        owner.promotion_receipts().unwrap().receipts(),
        &[record.attempt().clone()]
    );
    let checked = reconcile_promoted_redb_startup(
        &mut owner,
        &record,
        inputs(),
        Arc::new(AtomicBool::new(false)),
        RedbCommitProfile::Standard,
    )
    .expect("cancelled startup must release every engine handle for an exact retry");
    assert_eq!(
        checked.database_id(),
        record
            .attempt()
            .selection()
            .unwrap()
            .published_lineage()
            .database_id()
    );
}

#[test]
fn promoted_startup_late_cancellation_releases_the_reconciled_source_for_retry() {
    let scope = tempfile::tempdir().unwrap();
    let (mut owner, record) = fixture(scope.path());
    owner.apply_promotion_cutover(&record).unwrap();
    let cancellation = Arc::new(AtomicBool::new(false));
    let (publication, reader) = crate::replication_publication::ReplicationPublication::channel();
    let store = owner
        .reconcile_committed_promotion(
            &record,
            inputs(),
            Arc::clone(&cancellation),
            RedbCommitProfile::Standard,
            publication.clone(),
        )
        .unwrap();
    cancellation.store(true, Ordering::Release);
    assert!(
        complete_promoted_redb_startup(store, inputs(), cancellation, publication, reader,)
            .is_err()
    );
    assert_eq!(
        owner.promotion_receipts().unwrap().receipts()[0].phase(),
        ReplicationPromotionPhaseV1::Succeeded
    );
    let checked = reconcile_promoted_redb_startup(
        &mut owner,
        &record,
        inputs(),
        Arc::new(AtomicBool::new(false)),
        RedbCommitProfile::Standard,
    )
    .expect("late cancellation must release operational ports and the publication pin");
    assert_eq!(
        checked.retained_metadata().history_incarnation(),
        record
            .attempt()
            .selection()
            .unwrap()
            .published_lineage()
            .history_incarnation()
    );
}

// req: REP-002, REP-003, REP-004, REP-005
#[test]
fn epoch_vote_quorum_fences_stale_primary_and_elects_highest_frontier_candidate() {
    let scope = tempfile::tempdir().unwrap();
    let source_path = scope.path().join("source.redb");
    let source = complete_redb_startup(RedbStore::open(&source_path).unwrap(), inputs(), || {
        Ok(DatabaseId::from_unix_milliseconds_and_random(1234, [1; 10]).unwrap())
    })
    .unwrap();
    let voter_1 = ReplicationSourceHoldIdV1::new([0x71; 16]).unwrap();
    let voter_2 = ReplicationSourceHoldIdV1::new([0x72; 16]).unwrap();
    let voter_3 = ReplicationSourceHoldIdV1::new([0x73; 16]).unwrap();
    let voters = [voter_1, voter_2, voter_3];

    let held = source
        .operational_ports
        .prepare_replication_bootstrap_v3(&scope.path().join("source-transfer"), voter_2)
        .unwrap();
    let manifest = held.manifest();
    let mut receiving =
        RedbBootstrapStage::create(&scope.path().join("receiver-transfer"), manifest.clone())
            .unwrap();
    for ordinal in 1..=manifest.page_count() {
        receiving
            .append(&held.read_page(ordinal).unwrap().encode().unwrap())
            .unwrap();
    }
    let mut materializer = RedbBootstrapMaterializer::create(
        &scope.path().join("candidate"),
        receiving.into_materialization_input().unwrap(),
    )
    .unwrap();
    while materializer.copy_next_page().unwrap().is_some() {}
    let follower_db = scope.path().join("published-follower.redb");
    materializer
        .finish()
        .unwrap()
        .validate(inputs())
        .unwrap()
        .publish(&follower_db)
        .unwrap()
        .release_for_startup()
        .unwrap();
    drop(held);
    drop(source);
    std::fs::remove_file(&source_path).unwrap();

    let history = manifest.fence().history();
    let lineage = history.lineage();
    let applied = history.tail();
    let epoch_1 = lineage.leadership_epoch();
    let epoch_2 = epoch_1.checked_next().unwrap();

    // 1. A lagging candidate (voter_1 behind voter_2/voter_3) is rejected.
    let lagging_pos = ReplicationEpochVotePositionV1::new(
        NonZeroU64::new(1).unwrap(),
        [0x11; 32],
        DualFrontier::new(None, None),
    );
    let current_pos = ReplicationEpochVotePositionV1::new(
        NonZeroU64::new(applied.sequence().get()).unwrap(),
        applied.history_hash(),
        applied.frontier(),
    );
    assert!(!lagging_pos.is_at_least_as_up_to_date_as(current_pos));
    assert!(current_pos.is_at_least_as_up_to_date_as(lagging_pos));

    let rejected_vote = ReplicationEpochVoteV1::new(
        voter_3,
        lineage.database_id(),
        lineage.history_incarnation(),
        epoch_2,
        voter_1,
        lagging_pos,
        current_pos,
        false,
    )
    .unwrap();
    assert!(!rejected_vote.granted());
    assert!(
        QuorumFenceCertificateV1::new(
            lineage.database_id(),
            lineage.history_incarnation(),
            epoch_1,
            epoch_2,
            voter_1,
            lagging_pos,
            &voters,
            vec![rejected_vote],
        )
        .is_err()
    );

    // 2. Durable promised_leadership_epoch persists across reopens and fences out conflicting candidates.
    let voter_3_dir = scope.path().join("voter3");
    std::fs::create_dir_all(&voter_3_dir).unwrap();
    {
        let owner_3 = RedbMaintenanceStorage::open_for_promotion_recovery(
            &follower_db,
            voter_3_dir.join("backups"),
        )
        .unwrap();
        assert_eq!(owner_3.read_promised_leadership_epoch().unwrap(), None);
        assert!(
            owner_3
                .persist_promised_leadership_epoch(epoch_2, voter_2)
                .unwrap()
        );
        // Same candidate in same epoch is idempotent:
        assert!(
            owner_3
                .persist_promised_leadership_epoch(epoch_2, voter_2)
                .unwrap()
        );
        // Different candidate in same epoch is rejected fail-closed:
        assert!(
            !owner_3
                .persist_promised_leadership_epoch(epoch_2, voter_1)
                .unwrap()
        );
        // Lower epoch is rejected fail-closed:
        assert!(
            !owner_3
                .persist_promised_leadership_epoch(epoch_1, voter_2)
                .unwrap()
        );
    }
    let owner_3 = RedbMaintenanceStorage::open_for_promotion_recovery(
        &follower_db,
        voter_3_dir.join("backups"),
    )
    .unwrap();
    assert_eq!(
        owner_3.read_promised_leadership_epoch().unwrap(),
        Some((epoch_2, voter_2))
    );
    drop(owner_3);

    // 3. Follower applier seeded with promised_leadership_epoch = epoch_2 rejects stale epoch_1 frames.
    let mut checked_voter_3 =
        crate::startup::open_redb_follower_startup(&follower_db, inputs()).unwrap();
    checked_voter_3
        .applier
        .record_promised_leadership_epoch(epoch_2);
    assert_eq!(
        checked_voter_3
            .applier
            .promised_epoch_handle()
            .load(Ordering::Acquire),
        epoch_2.get()
    );
    let binding_e1 = ChangelogFrameBindingV3::new(
        lineage.database_id(),
        lineage.history_incarnation(),
        epoch_1.get(),
        lineage.catalog_digest(),
        applied.history_hash(),
    )
    .unwrap();
    let stale_tx = AuthoritativeTransactionV3::new(
        AuthoritativeTransactionBindingV3 {
            database_id: lineage.database_id(),
            history_incarnation: lineage.history_incarnation(),
            sequence: applied.sequence().checked_next().unwrap(),
            predecessor: Some(applied.sequence()),
            prior_history_hash: applied.history_hash(),
            predecessor_frontier: applied.frontier(),
            covered_frontier: applied.frontier(),
        },
        ChangelogAttributionV3::ReplicationSourceHold,
        vec![],
    )
    .unwrap();
    let stale_frame = ChangelogFrameV3::new(binding_e1, vec![stale_tx])
        .unwrap()
        .encode()
        .unwrap();
    assert!(
        checked_voter_3.applier.apply_frame(&stale_frame).is_err(),
        "voter with promised_leadership_epoch = 2 must reject frames from epoch 1"
    );
    drop(checked_voter_3);

    // 4. Highest-frontier candidate (voter_2) collects 2-of-3 majority quorum certificate and promotes cleanly.
    let mut owner_2 = RedbMaintenanceStorage::open_for_promotion_recovery(
        scope.path().join("candidate/follower.redb"),
        scope.path().join("backups"),
    )
    .unwrap();
    assert!(
        owner_2
            .persist_promised_leadership_epoch(epoch_2, voter_2)
            .unwrap()
    );
    let self_vote = ReplicationEpochVoteV1::new(
        voter_2,
        lineage.database_id(),
        lineage.history_incarnation(),
        epoch_2,
        voter_2,
        current_pos,
        current_pos,
        true,
    )
    .unwrap();
    let peer_vote = ReplicationEpochVoteV1::new(
        voter_3,
        lineage.database_id(),
        lineage.history_incarnation(),
        epoch_2,
        voter_2,
        current_pos,
        current_pos,
        true,
    )
    .unwrap();
    let certificate = QuorumFenceCertificateV1::new(
        lineage.database_id(),
        lineage.history_incarnation(),
        epoch_1,
        epoch_2,
        voter_2,
        current_pos,
        &voters,
        vec![self_vote, peer_vote],
    )
    .expect("2 of 3 voters must form a valid majority QuorumFenceCertificateV1");

    let target = ReplicationFollowerAuditTargetV1::new(
        lineage.database_id(),
        lineage.history_incarnation(),
        epoch_1,
        voter_2,
    )
    .unwrap();
    let request = ReplicationPromotionRequestV1::new(
        ReplicationPromotionOperationId::from_unix_milliseconds_and_random(1234, [6; 10]).unwrap(),
        ReplicationFenceOperationId::from_unix_milliseconds_and_random(1234, [3; 10]).unwrap(),
        target,
        history.anchor().sequence(),
    );
    let principal = AuditPrincipalV1::new(
        ActorId::new("promotion-operator").unwrap(),
        ActorKind::Human,
        CapabilityId::from_unix_milliseconds_and_random(1234, [8; 10]).unwrap(),
        NonZeroU64::new(1).unwrap(),
    );
    let req_id = RequestId::from_unix_milliseconds_and_random(1234, [7; 10]).unwrap();
    let ts = Timestamp::new(1234, 1).unwrap();
    let evidence = PrimaryFenceSourceEvidenceV1::from_quorum_fence_certificate(
        &certificate,
        request,
        req_id,
        principal.clone(),
        ts,
        history,
    )
    .expect("quorum fence certificate must produce valid PrimaryFenceSourceEvidenceV1");

    let selection = ReplicationPromotionSelectionV1::new(
        request,
        ReplicationFollowerStateV3::attached(lineage, applied, Some(history.anchor())).unwrap(),
        evidence,
    )
    .unwrap();
    let mut attempt =
        ReplicationPromotionReceiptV1::attempted(request, req_id, principal, None, ts);
    owner_2.persist_promotion_receipt(&attempt).unwrap();
    for phase in [
        ReplicationPromotionPhaseV1::Draining,
        ReplicationPromotionPhaseV1::Offline,
    ] {
        attempt
            .advance(ReplicationPromotionStepV1::Phase(phase))
            .unwrap();
        owner_2.persist_promotion_receipt(&attempt).unwrap();
    }
    attempt.record_selection(selection).unwrap();
    owner_2.persist_promotion_receipt(&attempt).unwrap();
    attempt
        .advance(ReplicationPromotionStepV1::Phase(
            ReplicationPromotionPhaseV1::CutoverPending,
        ))
        .unwrap();
    owner_2.persist_promotion_receipt(&attempt).unwrap();
    let record = StoredPromotionAdministrationV1::new(
        attempt,
        Timestamp::new(1234, 2).unwrap(),
        ServiceIngressKindV1::Grpc,
    )
    .unwrap();
    owner_2.apply_promotion_cutover(&record).unwrap();
    let checked = reconcile_promoted_redb_startup(
        &mut owner_2,
        &record,
        inputs(),
        Arc::new(AtomicBool::new(false)),
        RedbCommitProfile::Standard,
    )
    .expect("quorum-fenced promotion cutover must reconcile into a valid primary startup");
    assert_eq!(
        checked.retained_metadata().history_incarnation(),
        lineage.history_incarnation() + 1
    );
    assert_eq!(
        owner_2.promotion_receipts().unwrap().receipts()[0].phase(),
        ReplicationPromotionPhaseV1::Succeeded
    );
}

// req: REP-002, REP-003, REP-004, REP-005, REP-006, DRV-001, DRV-005
#[test]
fn primary_lease_expiry_self_fences_and_auto_failover_promotes_new_primary() {
    use std::num::NonZeroU32;
    use std::time::{Duration, Instant};

    use crate::replication_source::{AutoFailoverCoordinator, PrimaryQuorumLeaseTracker};
    use crate::runtime_support::{RuntimeRoutingState, RuntimeStopReason};
    use riffdb_storage_api::{ReplicationSourceHoldKindV1, ReplicationSourceHoldV1};
    use riffdb_types::CommitSequence;

    let scope = tempfile::tempdir().unwrap();
    let source_path = scope.path().join("source.redb");
    let source = complete_redb_startup(RedbStore::open(&source_path).unwrap(), inputs(), || {
        Ok(DatabaseId::from_unix_milliseconds_and_random(1234, [1; 10]).unwrap())
    })
    .unwrap();

    let voter_1 = ReplicationSourceHoldIdV1::new([0x71; 16]).unwrap();
    let voter_2 = ReplicationSourceHoldIdV1::new([0x72; 16]).unwrap();
    let voter_3 = ReplicationSourceHoldIdV1::new([0x73; 16]).unwrap();
    let voters = vec![voter_2, voter_3, voter_1];

    let shared = source.operational_ports.shared_ports();
    let initial_progress = shared.observe_replication_source_progress_v3().unwrap();
    let initial_history = initial_progress.history();
    let lineage = initial_history.lineage();

    // Bootstrap voter_2 and publish its validated follower database.
    let held = source
        .operational_ports
        .prepare_replication_bootstrap_v3(&scope.path().join("source-transfer"), voter_2)
        .unwrap();
    let manifest = held.manifest();
    let mut receiving =
        RedbBootstrapStage::create(&scope.path().join("receiver-transfer"), manifest.clone())
            .unwrap();
    for ordinal in 1..=manifest.page_count() {
        receiving
            .append(&held.read_page(ordinal).unwrap().encode().unwrap())
            .unwrap();
    }
    let mut materializer = RedbBootstrapMaterializer::create(
        &scope.path().join("candidate"),
        receiving.into_materialization_input().unwrap(),
    )
    .unwrap();
    while materializer.copy_next_page().unwrap().is_some() {}
    let follower_db = scope.path().join("published-follower.redb");
    materializer
        .finish()
        .unwrap()
        .validate(inputs())
        .unwrap()
        .publish(&follower_db)
        .unwrap()
        .release_for_startup()
        .unwrap();
    drop(held);
    source
        .operational_ports
        .attach_replication_bootstrap_v3(manifest, manifest.fence().history().tail())
        .unwrap();

    // Commit an application transaction (commit_1) on the primary and record quorum acks from voter_2 & voter_3.
    let commit_1 = CommitSequence::new(1).unwrap();
    let commit_1_history = shared
        .record_application_commit_sequence_for_test(commit_1)
        .unwrap();
    assert!(
        shared
            .advance_replication_source_hold_v3(ReplicationSourceHoldV1::new(
                voter_2,
                ReplicationSourceHoldKindV1::FollowerAcknowledgement,
                lineage,
                commit_1_history.tail(),
            ))
            .unwrap()
    );
    assert!(
        shared
            .register_replication_source_hold_v3(ReplicationSourceHoldV1::new(
                voter_1,
                ReplicationSourceHoldKindV1::FollowerAcknowledgement,
                lineage,
                initial_history.tail(),
            ))
            .unwrap()
    );
    assert!(
        shared
            .register_replication_source_hold_v3(ReplicationSourceHoldV1::new(
                voter_3,
                ReplicationSourceHoldKindV1::FollowerAcknowledgement,
                lineage,
                commit_1_history.tail(),
            ))
            .unwrap()
    );
    let progress_after_commit = shared.observe_replication_source_progress_v3().unwrap();
    assert!(
        progress_after_commit
            .has_quorum_acknowledged_application_commit(commit_1, NonZeroU32::new(2).unwrap()),
        "commit_1 must be quorum-acknowledged by voter_2 and voter_3"
    );
    drop(source);

    // Configure monotonic primary quorum lease (200ms) and follower election timeout (500ms > 200ms).
    let t0 = Instant::now();
    let lease_duration = Duration::from_millis(200);
    let election_timeout = Duration::from_millis(500);
    let primary_routing = RuntimeRoutingState::new();
    let primary_lease = PrimaryQuorumLeaseTracker::new(
        voters.clone(),
        lease_duration,
        election_timeout,
        primary_routing.clone(),
        t0,
    )
    .expect("valid primary lease configuration");
    let coordinator_2 = AutoFailoverCoordinator::new(
        voters.clone(),
        voter_2,
        lease_duration,
        election_timeout,
        t0,
    )
    .expect("valid follower 2 coordinator");
    let coordinator_3 = AutoFailoverCoordinator::new(
        voters.clone(),
        voter_3,
        lease_duration,
        election_timeout,
        t0,
    )
    .expect("valid follower 3 coordinator");
    assert_eq!(
        coordinator_2.effective_election_timeout(),
        Duration::from_millis(500)
    );
    assert_eq!(
        coordinator_3.effective_election_timeout(),
        Duration::from_millis(550)
    );

    // 1. At t0 + 100ms (< lease_duration): primary lease is valid and routing is allowed.
    let t_100 = t0 + Duration::from_millis(100);
    assert!(primary_lease.check_or_fence(t_100));
    assert!(primary_routing.is_routing_allowed());
    assert!(!coordinator_2.should_start_election(t_100));

    // 2. At t0 + 250ms (>= lease_duration = 200ms, < election_timeout = 500ms):
    //    Primary immediately self-fences BEFORE any follower's election timeout can expire!
    let t_250 = t0 + Duration::from_millis(250);
    assert!(!coordinator_2.should_start_election(t_250));
    assert!(!primary_lease.check_or_fence(t_250));
    assert!(primary_lease.is_fenced());
    assert!(!primary_routing.is_routing_allowed());
    assert_eq!(
        primary_routing.stop_reason(),
        Some(RuntimeStopReason::CoordinatorFenced)
    );
    // Once self-fenced, late voter contact cannot un-fence the primary:
    assert!(!primary_lease.record_voter_contact(voter_2, t0 + Duration::from_millis(260)));
    assert!(!primary_routing.is_routing_allowed());

    // 3. At t0 + 510ms (>= 500ms for rank-0 voter_2, < 550ms for rank-1 voter_3):
    //    voter_2's election timeout fires first, collects QuorumFenceCertificateV1, and promotes!
    let t_510 = t0 + Duration::from_millis(510);
    assert!(coordinator_2.should_start_election(t_510));
    assert!(!coordinator_3.should_start_election(t_510));

    let history = manifest.fence().history();
    let applied = history.tail();
    let epoch_1 = lineage.leadership_epoch();
    let epoch_2 = epoch_1.checked_next().unwrap();
    let current_pos = ReplicationEpochVotePositionV1::new(
        NonZeroU64::new(applied.sequence().get()).unwrap(),
        applied.history_hash(),
        applied.frontier(),
    );

    let mut owner_2 = RedbMaintenanceStorage::open_for_promotion_recovery(
        &follower_db,
        scope.path().join("backups"),
    )
    .unwrap();
    assert!(
        owner_2
            .persist_promised_leadership_epoch(epoch_2, voter_2)
            .unwrap()
    );
    let self_vote = ReplicationEpochVoteV1::new(
        voter_2,
        lineage.database_id(),
        lineage.history_incarnation(),
        epoch_2,
        voter_2,
        current_pos,
        current_pos,
        true,
    )
    .unwrap();
    let peer_vote = ReplicationEpochVoteV1::new(
        voter_3,
        lineage.database_id(),
        lineage.history_incarnation(),
        epoch_2,
        voter_2,
        current_pos,
        current_pos,
        true,
    )
    .unwrap();
    let certificate = QuorumFenceCertificateV1::new(
        lineage.database_id(),
        lineage.history_incarnation(),
        epoch_1,
        epoch_2,
        voter_2,
        current_pos,
        &voters,
        vec![self_vote, peer_vote],
    )
    .expect("2 of 3 voters must form a valid QuorumFenceCertificateV1");

    let target = ReplicationFollowerAuditTargetV1::new(
        lineage.database_id(),
        lineage.history_incarnation(),
        epoch_1,
        voter_2,
    )
    .unwrap();
    let request = ReplicationPromotionRequestV1::new(
        ReplicationPromotionOperationId::from_unix_milliseconds_and_random(1234, [6; 10]).unwrap(),
        ReplicationFenceOperationId::from_unix_milliseconds_and_random(1234, [3; 10]).unwrap(),
        target,
        history.anchor().sequence(),
    );
    let principal = AuditPrincipalV1::new(
        ActorId::new("auto-failover-coordinator").unwrap(),
        ActorKind::Service,
        CapabilityId::from_unix_milliseconds_and_random(1234, [8; 10]).unwrap(),
        NonZeroU64::new(1).unwrap(),
    );
    let req_id = RequestId::from_unix_milliseconds_and_random(1234, [7; 10]).unwrap();
    let ts = Timestamp::new(1234, 1).unwrap();
    let evidence = PrimaryFenceSourceEvidenceV1::from_quorum_fence_certificate(
        &certificate,
        request,
        req_id,
        principal.clone(),
        ts,
        history,
    )
    .expect("quorum fence certificate must produce valid PrimaryFenceSourceEvidenceV1");

    let selection = ReplicationPromotionSelectionV1::new(
        request,
        ReplicationFollowerStateV3::attached(lineage, applied, Some(history.anchor())).unwrap(),
        evidence,
    )
    .unwrap();
    let mut attempt =
        ReplicationPromotionReceiptV1::attempted(request, req_id, principal, None, ts);
    owner_2.persist_promotion_receipt(&attempt).unwrap();
    for phase in [
        ReplicationPromotionPhaseV1::Draining,
        ReplicationPromotionPhaseV1::Offline,
    ] {
        attempt
            .advance(ReplicationPromotionStepV1::Phase(phase))
            .unwrap();
        owner_2.persist_promotion_receipt(&attempt).unwrap();
    }
    attempt.record_selection(selection).unwrap();
    owner_2.persist_promotion_receipt(&attempt).unwrap();
    attempt
        .advance(ReplicationPromotionStepV1::Phase(
            ReplicationPromotionPhaseV1::CutoverPending,
        ))
        .unwrap();
    owner_2.persist_promotion_receipt(&attempt).unwrap();
    let record = StoredPromotionAdministrationV1::new(
        attempt,
        Timestamp::new(1234, 2).unwrap(),
        ServiceIngressKindV1::Grpc,
    )
    .unwrap();
    owner_2.apply_promotion_cutover(&record).unwrap();
    let checked = reconcile_promoted_redb_startup(
        &mut owner_2,
        &record,
        inputs(),
        Arc::new(AtomicBool::new(false)),
        RedbCommitProfile::Standard,
    )
    .expect("auto-failover promotion must reconcile into a valid primary startup");

    assert_eq!(
        checked.retained_metadata().history_incarnation(),
        lineage.history_incarnation() + 1
    );
    assert_eq!(
        owner_2.promotion_receipts().unwrap().receipts()[0].phase(),
        ReplicationPromotionPhaseV1::Succeeded
    );
}
