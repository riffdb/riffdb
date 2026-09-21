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
