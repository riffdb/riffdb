//! Real committed promotion must choose the source role with its old peer absent.
// req: REP-005, REC-001, STO-012
use super::*;
use crate::startup::promotion::tests::{fixture, inputs};
use riffdb_config::{CanonicalHttpsEndpoint, ProtectedFilePath, TlsClientConfig};
use riffdb_storage_api::{ChangelogLineageV3, ReplicationSourceHoldIdV1};

fn source(lineage: ChangelogLineageV3) -> FollowerSourceConfig {
    FollowerSourceConfig {
        tls: TlsClientConfig::new(
            CanonicalHttpsEndpoint::parse("https://unavailable-primary.example:7443").unwrap(),
            ProtectedFilePath::new(PathBuf::from("/absent/promotion/ca.pem")).unwrap(),
            TlsServerIdentity::parse("unavailable-primary.example").unwrap(),
            Duration::from_secs(5),
            Duration::from_secs(30),
            NonZeroU32::MIN,
            NonZeroU32::MIN,
        )
        .unwrap(),
        credential: ProtectedFilePath::new(PathBuf::from("/absent/promotion/token")).unwrap(),
        database: DatabaseAlias::default_alias(),
        lineage,
        hold: ReplicationSourceHoldIdV1::new([0x73; 16]).unwrap(),
    }
}

#[test]
fn daemon_recovers_committed_promotion_without_reading_old_peer_credentials() {
    for profile in [RedbCommitProfile::Standard, RedbCommitProfile::Hardened] {
        for follower_configured in [true, false] {
            let scope = tempfile::tempdir().unwrap();
            let (mut owner, record) = fixture(scope.path());
            owner.apply_promotion_cutover(&record).unwrap();
            let selection = record.attempt().selection().unwrap();
            let source = source(selection.evidence().fence().lineage());
            drop(owner);
            let mut prepared = prepare(
                &scope.path().join("candidate/follower.redb"),
                &scope.path().join("backups"),
                follower_configured.then_some(&source),
                inputs(),
                profile,
            )
            .unwrap();
            let mut startup = prepared
                .promoted
                .take()
                .expect("committed cutover chooses a source");
            assert_eq!(
                startup.database_id(),
                selection.published_lineage().database_id()
            );
            assert_eq!(
                startup.retained_metadata().history_incarnation(),
                selection.published_lineage().history_incarnation()
            );
            assert!(startup.take_replication_publications().is_some());
            assert!(prepared.owner.reconcile_for_startup().is_ok());
            assert!(prepared.reconciliation.receipts().receipts().is_empty());
        }
    }
}

#[test]
fn daemon_does_not_resume_old_applier_over_an_unfinished_promotion_attempt() {
    let scope = tempfile::tempdir().unwrap();
    let (owner, record) = fixture(scope.path());
    let before = owner.promotion_receipts().unwrap();
    let source = source(
        record
            .attempt()
            .selection()
            .unwrap()
            .evidence()
            .fence()
            .lineage(),
    );
    drop(owner);
    let path = scope.path().join("candidate/follower.redb");
    let backups = scope.path().join("backups");
    assert!(
        prepare(
            &path,
            &backups,
            Some(&source),
            inputs(),
            RedbCommitProfile::Standard
        )
        .is_err()
    );
    let owner = RedbMaintenanceStorage::open_for_promotion_recovery(&path, &backups).unwrap();
    assert_eq!(owner.promotion_receipts().unwrap(), before);
    drop(owner);
    let PreparedReplicationRole::Retry(mut pending) = prepare_role(
        &path,
        &backups,
        Some(&source),
        inputs(),
        RedbCommitProfile::Standard,
    )
    .unwrap() else {
        panic!("pending cutover must select restricted admission")
    };
    assert_eq!(pending.request, record.attempt().request());
    assert_eq!(pending.owner.promotion_receipts().unwrap(), before);
    assert!(pending.owner.reconcile_for_startup().is_err());
    assert!(RedbMaintenanceStorage::open_for_promotion_recovery(&path, &backups).is_err());
}

#[test]
fn daemon_does_not_resume_old_applier_after_a_selected_attempt_fails_or_is_denied() {
    use riffdb_storage_api::{
        ReplicationPromotionFailureV1 as Failure, ReplicationPromotionStepV1 as Step,
    };
    for step in [
        Step::FailedClosed(Failure::StorageUnavailable),
        Step::Denied(Failure::AuthorizationDenied),
    ] {
        for profile in [RedbCommitProfile::Standard, RedbCommitProfile::Hardened] {
            let scope = tempfile::tempdir().unwrap();
            let (owner, record) =
                crate::startup::promotion::tests::fixture_with_selection_result(scope.path(), step);
            let before = owner.promotion_receipts().unwrap();
            let source = source(
                record
                    .attempt()
                    .selection()
                    .unwrap()
                    .evidence()
                    .fence()
                    .lineage(),
            );
            drop(owner);
            let path = scope.path().join("candidate/follower.redb");
            let backups = scope.path().join("backups");
            assert!(prepare(&path, &backups, Some(&source), inputs(), profile).is_err());
            let owner =
                RedbMaintenanceStorage::open_for_promotion_recovery(&path, &backups).unwrap();
            assert_eq!(owner.promotion_receipts().unwrap(), before);
            assert!(owner.discover_committed_promotion().unwrap().is_none());
        }
    }
}

#[test]
fn daemon_refuses_foreign_follower_configuration_before_reconciling_cutover() {
    let scope = tempfile::tempdir().unwrap();
    let (mut owner, record) = fixture(scope.path());
    owner.apply_promotion_cutover(&record).unwrap();
    let before = owner.promotion_receipts().unwrap();
    let mut source = source(
        record
            .attempt()
            .selection()
            .unwrap()
            .evidence()
            .fence()
            .lineage(),
    );
    source.hold = ReplicationSourceHoldIdV1::new([0x74; 16]).unwrap();
    drop(owner);
    let path = scope.path().join("candidate/follower.redb");
    let backups = scope.path().join("backups");
    assert!(
        prepare(
            &path,
            &backups,
            Some(&source),
            inputs(),
            RedbCommitProfile::Standard
        )
        .is_err()
    );
    let owner = RedbMaintenanceStorage::open_for_promotion_recovery(&path, &backups).unwrap();
    assert_eq!(owner.promotion_receipts().unwrap(), before);
}

#[test]
fn daemon_refuses_committed_promotion_when_external_attempt_is_missing() {
    let scope = tempfile::tempdir().unwrap();
    let (mut owner, record) = fixture(scope.path());
    owner.apply_promotion_cutover(&record).unwrap();
    let source = source(
        record
            .attempt()
            .selection()
            .unwrap()
            .evidence()
            .fence()
            .lineage(),
    );
    drop(owner);
    std::fs::remove_file(
        scope
            .path()
            .join("backups/.maintenance/replication_promotion")
            .join(format!("{}.receipt-v1", record.attempt().request_id())),
    )
    .unwrap();
    assert!(
        prepare(
            &scope.path().join("candidate/follower.redb"),
            &scope.path().join("backups"),
            Some(&source),
            inputs(),
            RedbCommitProfile::Standard,
        )
        .is_err()
    );
}
