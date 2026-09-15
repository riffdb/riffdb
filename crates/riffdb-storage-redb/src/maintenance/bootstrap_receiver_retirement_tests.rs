//! Physical scratch retirement under a real follower engine lock. Production
//! attachment and the complete startup proof join are tested by the server.
// req: REP-002, REP-003, REC-001, PERF-007
use super::*;
use crate::{
    RedbBootstrapReceiverRepository as Repository, RedbFollowerApplier, RedbFollowerStore,
};
use std::path::Path;
use std::sync::atomic::AtomicBool;

fn fixture(
    scope: &crate::test_path::ScopedDirectory,
) -> (Repository, ReplicationBootstrapManifestV1) {
    let ports = source(&scope.join("source.redb"));
    let held = ports
        .prepare_replication_bootstrap_v3(
            &scope.join("source-transfer"),
            ReplicationSourceHoldIdV1::new([0x65; 16]).unwrap(),
        )
        .unwrap();
    let repository = Repository::open(&scope.join("receiver")).unwrap();
    let manifest = held.manifest();
    let mut stage = repository.begin_transfer(manifest).unwrap();
    for n in 1..=manifest.page_count() {
        stage
            .append(&held.read_page(n).unwrap().encode().unwrap())
            .unwrap();
    }
    let mut candidate = repository
        .materialize_transfer(stage, &AtomicBool::new(false))
        .unwrap();
    while candidate.copy_next_page().unwrap().is_some() {}
    candidate
        .finish()
        .unwrap()
        .validate(inputs())
        .unwrap()
        .publish(&scope.join("live.redb"))
        .unwrap()
        .release_for_startup()
        .unwrap();
    (repository, manifest)
}

fn physical_applier(path: &Path) -> RedbFollowerApplier {
    crate::startup::open_validated_follower_fixture(path, inputs())
}

#[test]
fn receiver_retirement_preserves_live_bytes_and_exclusion_through_exact_retry() {
    let scope = crate::test_path::ScopedDirectory::new("receiver-retirement");
    let (repository, manifest) = fixture(&scope);
    let path = scope.join("live.redb");
    let mut applier = physical_applier(&path);
    let before = std::fs::read(&path).unwrap();
    let marker = std::fs::read(crate::durable_format_marker_path(&path)).unwrap();
    for _ in 0..3 {
        applier.retire_bootstrap_scratch(&repository).unwrap();
        assert!(!scope.join("receiver/candidate").exists());
        assert!(!scope.join("receiver/transfer").exists());
        assert!(RedbFollowerStore::open(&path).is_err());
        assert_eq!(
            applier.durable_position().unwrap(),
            manifest.fence().history().tail()
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert_eq!(
            std::fs::read(crate::durable_format_marker_path(&path)).unwrap(),
            marker
        );
    }
    applier.close().unwrap();
    crate::startup::RedbOfflineIntegrityScrub::from_inputs(&path, inputs())
        .run_follower()
        .unwrap();
}

#[test]
fn receiver_retirement_preflights_both_inventories_and_locks_before_unlink() {
    for location in ["candidate", "transfer"] {
        let scope = crate::test_path::ScopedDirectory::new("receiver-retirement-refusal");
        let (repository, _) = fixture(&scope);
        let mut applier = physical_applier(&scope.join("live.redb"));
        let unknown = scope.join(&format!("receiver/{location}/unknown"));
        std::fs::write(&unknown, b"preserve me").unwrap();
        assert!(applier.retire_bootstrap_scratch(&repository).is_err());
        assert_eq!(std::fs::read(&unknown).unwrap(), b"preserve me");
        assert!(scope.join("receiver/candidate/follower.redb").exists());
        assert!(scope.join("receiver/transfer/transfer.redb").exists());
        std::fs::remove_file(unknown).unwrap();
        let locked = scope.join(if location == "candidate" {
            "receiver/candidate/construction.lock"
        } else {
            "receiver/transfer/transfer.redb"
        });
        let lock = std::fs::File::open(locked).unwrap();
        lock.try_lock().unwrap();
        assert!(applier.retire_bootstrap_scratch(&repository).is_err());
        assert!(scope.join("receiver/candidate/follower.redb").exists());
        assert!(scope.join("receiver/transfer/transfer.redb").exists());
        drop(lock);
        applier.retire_bootstrap_scratch(&repository).unwrap();
    }
}

#[test]
fn receiver_retirement_refuses_live_or_scratch_substitution_without_deleting_either() {
    for relative in [
        "live.redb",
        "live.redb.riffdb-format-v1",
        "receiver/candidate/follower.redb",
        "receiver/candidate/follower.redb.riffdb-format-v1",
    ] {
        let scope = crate::test_path::ScopedDirectory::new("receiver-retirement-substitution");
        let (repository, _) = fixture(&scope);
        let mut applier = physical_applier(&scope.join("live.redb"));
        let path = scope.join(relative);
        std::fs::rename(&path, scope.join("original")).unwrap();
        std::fs::write(&path, b"preserve unrelated replacement").unwrap();
        assert!(applier.retire_bootstrap_scratch(&repository).is_err());
        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"preserve unrelated replacement"
        );
        assert!(scope.join("receiver/candidate/construction.lock").exists());
        assert!(scope.join("receiver/transfer/transfer.redb").exists());
    }
}

#[test]
fn receiver_retirement_refuses_to_delete_the_live_directory_itself() {
    let scope = crate::test_path::ScopedDirectory::new("receiver-retirement-live-dir");
    let (repository, _) = fixture(&scope);
    let mut applier = physical_applier(&scope.join("receiver/candidate/follower.redb"));
    assert!(applier.retire_bootstrap_scratch(&repository).is_err());
    assert!(scope.join("receiver/candidate/follower.redb").exists());
    assert!(scope.join("receiver/transfer/transfer.redb").exists());
}

#[test]
fn receiver_retirement_crash_child() {
    let Some(root) = std::env::var_os("RIFFDB_RECEIVER_RETIREMENT_ROOT") else {
        return;
    };
    let root = Path::new(&root);
    let repository = Repository::open(&root.join("receiver")).unwrap();
    let path = root.join("live.redb");
    crate::startup::RedbOfflineIntegrityScrub::from_inputs(&path, inputs())
        .run_follower()
        .unwrap();
    physical_applier(&path)
        .retire_bootstrap_scratch(&repository)
        .unwrap();
}

#[test]
fn receiver_retirement_repeated_process_crashes_resume_without_losing_the_live_follower() {
    for edge in [
        "retiring-candidate-file",
        "retiring-candidate-marker",
        "retiring-candidate-lock",
        "retiring-transfer-file",
        "retired-candidate-file",
        "retired-candidate-marker",
        "retired-candidate-lock",
        "retired-candidate-directory",
        "retired-transfer-file",
        "retired-transfer-directory",
    ] {
        let scope = crate::test_path::ScopedDirectory::new("receiver-retirement-crash");
        let (repository, manifest) = fixture(&scope);
        drop(repository);
        for attempt in 0..3 {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "maintenance::bootstrap_materialize_tests::receiver_retirement::receiver_retirement_crash_child"])
                .env("RIFFDB_RECEIVER_RETIREMENT_ROOT", scope.join("live.redb").parent().unwrap())
                .env("RIFFDB_RECEIVER_REPOSITORY_EDGE", edge).status().unwrap();
            assert_eq!(
                status.code(),
                Some(if attempt == 0 { 95 } else { 0 }),
                "{edge} attempt {attempt}"
            );
            let path = scope.join("live.redb");
            crate::startup::RedbOfflineIntegrityScrub::from_inputs(&path, inputs())
                .run_follower()
                .unwrap();
            assert_eq!(
                physical_applier(&path).durable_position().unwrap(),
                manifest.fence().history().tail()
            );
            assert!(path.exists());
            assert!(crate::durable_format_marker_path(&path).exists());
            let repository = Repository::open(&scope.join("receiver")).unwrap();
            assert_eq!(
                repository.attachment_manifest().unwrap(),
                scope
                    .join("receiver/transfer/transfer.redb")
                    .exists()
                    .then_some(manifest)
            );
            if attempt > 0 {
                assert!(!scope.join("receiver/candidate").exists());
                assert!(!scope.join("receiver/transfer").exists());
            }
        }
    }
}
