//! Bounded persistent source inventory and source-hold preservation.
// req: REP-002, REP-003, REC-001, PERF-007
use super::*;

#[test]
fn bootstrap_repository_bounds_live_and_abandoned_artifacts_across_reopen() {
    let (scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let root = scope.join("source-artifacts");
    let repository = ports.bootstrap_repository(&root).unwrap();
    let mut builds = Vec::new();
    for seed in 1..=4 {
        builds.push(repository.begin(HoldId::new([seed; 16]).unwrap()).unwrap());
    }
    assert!(repository.begin(HoldId::new([5; 16]).unwrap()).is_err());
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 5);
    drop(repository);
    assert!(
        ports.bootstrap_repository(&root).is_err(),
        "builders retain repository exclusion"
    );
    drop(builds);
    let repository = ports.bootstrap_repository(&root).unwrap();
    for seed in 5..=12 {
        let mut build = repository.begin(HoldId::new([seed; 16]).unwrap()).unwrap();
        assert!(!build.advance().unwrap());
        drop(build);
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 5);
    }
}

#[test]
fn bootstrap_repository_cleanup_follows_durable_attachment_and_exact_retries() {
    let (scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let root = scope.join("source-artifacts");
    let repository = ports.bootstrap_repository(&root).unwrap();
    let id = HoldId::new([0x73; 16]).unwrap();
    let mut build = repository.begin(id).unwrap();
    while !build.advance().unwrap() {}
    let held = build.finish().unwrap();
    let manifest = held.manifest();
    assert!(
        repository
            .attach(manifest, manifest.fence().history().tail())
            .is_err(),
        "a live transfer remains excluded"
    );
    drop(held);
    let mut resumed = repository.resume(id).unwrap();
    while !resumed.advance().unwrap() {}
    let held = resumed.finish().unwrap();
    assert_eq!(held.manifest(), manifest);
    drop(held);
    repository
        .attach(manifest, manifest.fence().history().tail())
        .unwrap();
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
    let epoch = ports.shared.durable_commit_epoch();
    repository
        .attach(manifest, manifest.fence().history().tail())
        .unwrap();
    assert_eq!(ports.shared.durable_commit_epoch(), epoch);
    assert!(
        repository.begin(id).is_err(),
        "attached identity must not recreate bootstrap artifacts"
    );
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
}

#[test]
fn bootstrap_repository_never_reclaims_a_registered_hold_or_a_live_fourth_artifact() {
    let (scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let root = scope.join("source-artifacts");
    let repository = ports.bootstrap_repository(&root).unwrap();
    for seed in 1..=4 {
        let mut build = repository.begin(HoldId::new([seed; 16]).unwrap()).unwrap();
        while !build.advance().unwrap() {}
        drop(build.finish().unwrap());
    }
    let epoch = ports.shared.durable_commit_epoch();
    assert!(repository.begin(HoldId::new([5; 16]).unwrap()).is_err());
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 5);
    assert_eq!(ports.shared.durable_commit_epoch(), epoch);
    // Retrying begin before receiving the initial manifest reuses the held
    // artifact; it cannot silently choose a newer source fence.
    let mut resumed = repository.begin(HoldId::new([1; 16]).unwrap()).unwrap();
    while !resumed.advance().unwrap() {}
    drop(resumed.finish().unwrap());
    assert_eq!(ports.shared.durable_commit_epoch(), epoch);
}

#[test]
fn bootstrap_repository_concurrent_admission_cannot_overbook_the_persistent_ceiling() {
    let (scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let root = scope.join("source-artifacts");
    let repository = ports.bootstrap_repository(&root).unwrap();
    let live = (1..=3)
        .map(|seed| repository.begin(HoldId::new([seed; 16]).unwrap()).unwrap())
        .collect::<Vec<_>>();
    let barrier = std::sync::Barrier::new(3);
    std::thread::scope(|scope| {
        let attempt = |seed| {
            barrier.wait();
            repository.begin(HoldId::new([seed; 16]).unwrap())
        };
        let left = scope.spawn(move || attempt(4));
        let right = scope.spawn(move || attempt(5));
        barrier.wait();
        let results = [left.join().unwrap(), right.join().unwrap()];
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 5);
    });
    drop(live);
}

#[test]
fn bootstrap_repository_refuses_substituted_locks_files_and_unclassified_inventory() {
    for fault in 0..3 {
        let (scope, ports, _) = crate::changelog_v3_control_tests::fixture();
        let root = scope.join("source-artifacts");
        let repository = ports.bootstrap_repository(&root).unwrap();
        let id = HoldId::new([0x61; 16]).unwrap();
        drop(repository.begin(id).unwrap());
        let path = root.join(encode_name(id));
        match fault {
            0 => {
                let artifact = repository.inner.artifact(id).unwrap().unwrap();
                std::fs::rename(path.join(TRANSFER), path.join("retained.redb")).unwrap();
                std::fs::write(path.join(TRANSFER), b"replacement bytes").unwrap();
                assert!(repository.inner.remove(artifact).is_err());
                assert_eq!(
                    std::fs::read(path.join(TRANSFER)).unwrap(),
                    b"replacement bytes"
                );
            }
            1 => {
                std::fs::rename(root.join(LOCK), root.join("retained.lock")).unwrap();
                std::fs::write(root.join(LOCK), b"").unwrap();
                assert!(repository.resume(id).is_err());
                assert!(path.join(TRANSFER).exists());
            }
            _ => {
                std::fs::write(path.join("unclassified"), b"preserve").unwrap();
                assert!(repository.begin(id).is_err());
                assert_eq!(
                    std::fs::read(path.join("unclassified")).unwrap(),
                    b"preserve"
                );
                assert!(path.join(TRANSFER).exists());
            }
        }
    }
}

#[test]
fn bootstrap_repository_crash_child() {
    let Some(path) = std::env::var_os("RIFFDB_BOOTSTRAP_REPOSITORY_SOURCE") else {
        return;
    };
    let path = Path::new(&path);
    let root = path.parent().unwrap();
    let manifest =
        Manifest::decode(&std::fs::read(root.join("expected-bootstrap.bin")).unwrap()).unwrap();
    let store = crate::RedbStore::open(path).unwrap();
    let ports = RedbOperationalPorts {
        shared: store.shared,
    };
    let repository = ports
        .bootstrap_repository(&root.join("source-artifacts"))
        .unwrap();
    repository
        .attach(manifest, manifest.fence().history().tail())
        .unwrap();
}

#[test]
fn bootstrap_repository_repeated_cleanup_crashes_preserve_the_follower_fence() {
    use redb::ReadableDatabase;
    use riffdb_storage_api::{
        ReplicationSourceHoldKindV1 as Kind, ReplicationSourceHoldV1 as Hold,
    };
    for edge in [
        "repository-attached",
        "repository-before-delete",
        "repository-file-deleted",
        "repository-directory-deleted",
        "repository-parent-synced",
    ] {
        let (scope, ports, _) = crate::changelog_v3_control_tests::fixture();
        let root = scope.join("source-artifacts");
        let repository = ports.bootstrap_repository(&root).unwrap();
        let mut build = repository.begin(HoldId::new([0x77; 16]).unwrap()).unwrap();
        while !build.advance().unwrap() {}
        let held = build.finish().unwrap();
        let manifest = held.manifest();
        std::fs::write(
            scope.join("expected-bootstrap.bin"),
            manifest.encode().unwrap(),
        )
        .unwrap();
        drop(held);
        drop(repository);
        drop(ports);
        for repeat in 0..3 {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "maintenance::bootstrap_repository::tests::bootstrap_repository_crash_child",
                    "--nocapture",
                ])
                .env("RIFFDB_BOOTSTRAP_REPOSITORY_SOURCE", scope.join("db.redb"))
                .env("RIFFDB_BOOTSTRAP_REPOSITORY_EDGE", edge)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .unwrap();
            if repeat == 0 {
                assert_eq!(status.code(), Some(94), "{edge}");
            } else {
                assert!(matches!(status.code(), Some(0 | 94)), "{edge}");
            }
        }
        let store = crate::RedbStore::open(scope.join("db.redb")).unwrap();
        let ports = RedbOperationalPorts {
            shared: store.shared,
        };
        let repository = ports.bootstrap_repository(&root).unwrap();
        repository
            .attach(manifest, manifest.fence().history().tail())
            .unwrap();
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
        let read = ports.shared.database.begin_read().unwrap();
        let table = read
            .open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
            .unwrap();
        let fence = manifest.fence();
        let expected = Hold::new(
            fence.hold_id(),
            Kind::FollowerAcknowledgement,
            fence.history().lineage(),
            fence.history().tail(),
        );
        assert_eq!(
            *riffdb_storage_api::proto_codec::decode_replication_source_hold_v1(
                table
                    .get(expected.storage_key().as_slice())
                    .unwrap()
                    .unwrap()
                    .value()
            )
            .unwrap()
            .value(),
            expected
        );
        assert!(
            table
                .get(Hold::storage_key_for(fence.hold_id(), Kind::Bootstrap).as_slice())
                .unwrap()
                .is_none()
        );
    }
}
