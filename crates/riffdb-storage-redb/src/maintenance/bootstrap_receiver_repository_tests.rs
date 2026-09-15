//! Managed receiver scratch ownership and initial publication recovery.
// req: REP-002, REP-003, REC-001, PERF-007
use super::*;
use crate::RedbBootstrapReceiverRepository as Repository;

#[test]
fn receiver_repository_recovers_initial_creation_without_losing_committed_pages() {
    let scope = crate::test_path::ScopedDirectory::new("receiver-repository");
    let root = scope.join("receiver");
    let repository = Repository::open(&root).unwrap();
    assert!(Repository::open(&root).is_err());
    let pending = root.join("transfer.creating");
    std::fs::create_dir(&pending).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&pending, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    std::fs::write(pending.join("transfer.redb"), []).unwrap();
    assert!(repository.recover_transfer().unwrap().is_none());
    assert!(!pending.exists());
    let (manifest, pages) = fixture();
    let mut stage = repository.begin_transfer(manifest).unwrap();
    assert!(root.join("transfer/transfer.redb").exists());
    stage.append(&pages[0].encode().unwrap()).unwrap();
    drop(repository);
    assert!(
        Repository::open(&root).is_err(),
        "the stage retains its root exclusion"
    );
    drop(stage);
    let repository = Repository::open(&root).unwrap();
    let stage = repository.recover_transfer().unwrap().unwrap();
    assert_eq!(stage.progress().manifest(), manifest);
    assert_eq!(stage.progress().page_count(), 1);
}

fn private_directory(path: &Path) {
    std::fs::create_dir(path).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
}

#[test]
fn receiver_repository_unknown_or_locked_initial_files_refuse_before_cleanup() {
    let scope = crate::test_path::ScopedDirectory::new("receiver-repository-refusal");
    let root = scope.join("receiver");
    let repository = Repository::open(&root).unwrap();
    let pending = root.join("transfer.creating");
    private_directory(&pending);
    let path = pending.join("transfer.redb");
    std::fs::write(&path, b"unpublished scratch").unwrap();
    let file = std::fs::File::open(&path).unwrap();
    file.try_lock().unwrap();
    assert!(repository.recover_transfer().is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"unpublished scratch");
    drop(file);
    let unknown = pending.join("unrecognized");
    std::fs::write(&unknown, b"must remain").unwrap();
    assert!(repository.recover_transfer().is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"unpublished scratch");
    assert_eq!(std::fs::read(&unknown).unwrap(), b"must remain");
    std::fs::remove_file(unknown).unwrap();
    assert!(repository.recover_transfer().unwrap().is_none());
    assert!(!pending.exists());
    drop(repository);
    let foreign = root.join("unrecognized");
    std::fs::write(&foreign, b"must remain").unwrap();
    assert!(Repository::open(&root).is_err());
    assert_eq!(std::fs::read(foreign).unwrap(), b"must remain");
}

#[test]
fn receiver_repository_crash_child() {
    let Some(root) = std::env::var_os("RIFFDB_RECEIVER_REPOSITORY_ROOT") else {
        return;
    };
    let repository = Repository::open(Path::new(&root)).unwrap();
    if std::env::var_os("RIFFDB_RECEIVER_REPOSITORY_CANDIDATE").is_some() {
        let stage = repository.recover_transfer().unwrap().unwrap();
        let candidate = repository
            .materialize_transfer(stage, &std::sync::atomic::AtomicBool::new(false))
            .unwrap();
        drop(candidate);
    } else {
        drop(repository.begin_transfer(fixture().0).unwrap());
    }
}

#[test]
fn receiver_repository_repeated_initial_creation_crashes_resume_at_one_durable_boundary() {
    for candidate in [false, true] {
        let mut edges = vec![
            (
                if candidate {
                    "RIFFDB_BOOTSTRAP_MATERIALIZE_EDGE"
                } else {
                    "RIFFDB_BOOTSTRAP_STAGE_CRASH_EDGE"
                },
                "initial-directory-created",
            ),
            (
                if candidate {
                    "RIFFDB_BOOTSTRAP_MATERIALIZE_EDGE"
                } else {
                    "RIFFDB_BOOTSTRAP_STAGE_CRASH_EDGE"
                },
                "initial-file-created",
            ),
            (
                if candidate {
                    "RIFFDB_BOOTSTRAP_MATERIALIZE_EDGE"
                } else {
                    "RIFFDB_BOOTSTRAP_STAGE_CRASH_EDGE"
                },
                "initial-engine-created",
            ),
            (
                if candidate {
                    "RIFFDB_BOOTSTRAP_MATERIALIZE_EDGE"
                } else {
                    "RIFFDB_BOOTSTRAP_STAGE_CRASH_EDGE"
                },
                "initial-progress-staged",
            ),
            (
                if candidate {
                    "RIFFDB_BOOTSTRAP_MATERIALIZE_EDGE"
                } else {
                    "RIFFDB_BOOTSTRAP_STAGE_CRASH_EDGE"
                },
                "initial-progress-committed",
            ),
            (
                "RIFFDB_RECEIVER_REPOSITORY_EDGE",
                if candidate {
                    "candidate-initialized"
                } else {
                    "transfer-initialized"
                },
            ),
            (
                "RIFFDB_RECEIVER_REPOSITORY_EDGE",
                if candidate {
                    "candidate-renamed"
                } else {
                    "transfer-renamed"
                },
            ),
        ];
        if candidate {
            edges.push((
                "RIFFDB_BOOTSTRAP_MATERIALIZE_EDGE",
                "initial-marker-published",
            ));
        }
        for (variable, edge) in edges {
            let scope = crate::test_path::ScopedDirectory::new("receiver-repository-crash");
            let root = scope.join("receiver");
            let (manifest, pages) = fixture();
            if candidate {
                let repository = Repository::open(&root).unwrap();
                let mut stage = repository.begin_transfer(manifest).unwrap();
                for page in &pages {
                    stage.append(&page.encode().unwrap()).unwrap();
                }
            }
            for attempt in 0..3 {
                let mut child = std::process::Command::new(std::env::current_exe().unwrap());
                child.args(["--exact", "maintenance::bootstrap_stage_tests::receiver_repository::receiver_repository_crash_child"])
                    .env("RIFFDB_RECEIVER_REPOSITORY_ROOT", &root).env(variable, edge);
                if candidate {
                    child.env("RIFFDB_RECEIVER_REPOSITORY_CANDIDATE", "1");
                }
                let status = child.status().unwrap();
                let renamed = edge.ends_with("renamed");
                assert_eq!(
                    status.code(),
                    Some(if renamed && attempt > 0 {
                        0
                    } else if variable == "RIFFDB_RECEIVER_REPOSITORY_EDGE" {
                        95
                    } else {
                        93
                    }),
                    "{edge}, attempt {attempt}"
                );
                let repository = Repository::open(&root).unwrap();
                let stage = repository.recover_transfer().unwrap();
                if candidate {
                    let stage = stage.unwrap();
                    assert_eq!(stage.progress().manifest(), manifest);
                    assert_eq!(stage.progress().page_count(), manifest.page_count());
                    assert_eq!(root.join("candidate").exists(), renamed);
                } else {
                    assert_eq!(stage.is_some(), renamed);
                    if let Some(stage) = stage {
                        assert_eq!(stage.progress().page_count(), 0);
                    }
                }
            }
            let repository = Repository::open(&root).unwrap();
            if candidate {
                drop(
                    repository
                        .materialize_transfer(
                            repository.recover_transfer().unwrap().unwrap(),
                            &std::sync::atomic::AtomicBool::new(false),
                        )
                        .unwrap(),
                );
            } else {
                let mut stage = repository.begin_transfer(manifest).unwrap();
                stage.append(&pages[0].encode().unwrap()).unwrap();
                assert_eq!(stage.progress().page_count(), 1);
            }
        }
    }
}

#[test]
fn receiver_repository_preserves_unrecognized_files_beside_a_published_transfer() {
    let scope = crate::test_path::ScopedDirectory::new("receiver-ready-inventory");
    let root = scope.join("receiver");
    let repository = Repository::open(&root).unwrap();
    drop(repository.begin_transfer(fixture().0).unwrap());
    let transfer = root.join("transfer/transfer.redb");
    let before = std::fs::read(&transfer).unwrap();
    let foreign = root.join("transfer/unrecognized");
    std::fs::write(&foreign, b"must remain").unwrap();
    assert!(repository.recover_transfer().is_err());
    assert_eq!(std::fs::read(transfer).unwrap(), before);
    assert_eq!(std::fs::read(&foreign).unwrap(), b"must remain");
    std::fs::remove_file(foreign).unwrap();
    private_directory(&root.join("transfer.creating"));
    assert!(repository.recover_transfer().is_err());
    drop(repository);
    assert!(Repository::open(&root).is_err());
}

#[test]
fn receiver_repository_interrupted_initial_cleanup_retries_only_its_known_files() {
    for candidate in [false, true] {
        for edge in ["initial-file-removed", "initial-directory-removed"] {
            let scope = crate::test_path::ScopedDirectory::new("receiver-cleanup-crash");
            let root = scope.join("receiver");
            let repository = Repository::open(&root).unwrap();
            let (manifest, pages) = fixture();
            if candidate {
                let mut stage = repository.begin_transfer(manifest).unwrap();
                for page in &pages {
                    stage.append(&page.encode().unwrap()).unwrap();
                }
            }
            let pending = root.join(if candidate {
                "candidate.creating"
            } else {
                "transfer.creating"
            });
            private_directory(&pending);
            if candidate {
                for name in [
                    "construction.lock",
                    "follower.redb",
                    "follower.redb.riffdb-format-v1.staging",
                ] {
                    std::fs::write(pending.join(name), []).unwrap();
                }
            } else {
                std::fs::write(pending.join("transfer.redb"), []).unwrap();
            }
            drop(repository);
            let mut finished = false;
            for attempt in 0..5 {
                let mut child = std::process::Command::new(std::env::current_exe().unwrap());
                child.args(["--exact", "maintenance::bootstrap_stage_tests::receiver_repository::receiver_repository_crash_child"])
                    .env("RIFFDB_RECEIVER_REPOSITORY_ROOT", &root).env("RIFFDB_RECEIVER_REPOSITORY_EDGE", edge);
                if candidate {
                    child.env("RIFFDB_RECEIVER_REPOSITORY_CANDIDATE", "1");
                }
                let status = child.status().unwrap();
                if attempt == 0 {
                    assert_eq!(status.code(), Some(95));
                }
                if status.success() {
                    finished = true;
                    break;
                }
                assert_eq!(status.code(), Some(95));
            }
            assert!(finished, "{edge}");
            assert!(!pending.exists());
            let repository = Repository::open(&root).unwrap();
            let stage = repository.recover_transfer().unwrap().unwrap();
            assert_eq!(stage.progress().manifest(), manifest);
            assert_eq!(
                stage.progress().page_count(),
                if candidate { manifest.page_count() } else { 0 }
            );
        }
    }
}

#[test]
fn receiver_repository_initial_cleanup_respects_the_actual_transfer_engine_lock() {
    let scope = crate::test_path::ScopedDirectory::new("receiver-engine-exclusion");
    let root = scope.join("receiver");
    let repository = Repository::open(&root).unwrap();
    let stage = RedbBootstrapStage::create(&root.join("transfer.creating"), fixture().0).unwrap();
    assert!(repository.recover_transfer().is_err());
    assert!(root.join("transfer.creating/transfer.redb").exists());
    drop(stage);
    assert!(repository.recover_transfer().unwrap().is_none());
    assert!(!root.join("transfer.creating").exists());
}
