//! Transfer durability evidence, not a structural or readiness proof.
// req: REP-002, REP-003, REC-001
use super::bootstrap_stage::*;
use riffdb_storage_api::*;

fn fixture() -> (
    ReplicationBootstrapManifestV1,
    Vec<ReplicationBootstrapPageV3>,
) {
    fn unhex(line: &str) -> Vec<u8> {
        line.as_bytes()
            .chunks_exact(2)
            .map(|b| u8::from_str_radix(std::str::from_utf8(b).unwrap(), 16).unwrap())
            .collect()
    }
    let manifest = ReplicationBootstrapManifestV1::decode(&unhex(
        include_str!("../../../../fixtures/replication/bootstrap-manifest-v1.hex").trim(),
    ))
    .unwrap();
    let pages = include_str!("../../../../fixtures/replication/bootstrap-pages-v1.hex")
        .lines()
        .map(|l| ReplicationBootstrapPageV3::decode(&unhex(l)).unwrap())
        .collect();
    (manifest, pages)
}

#[test]
fn bootstrap_stage_reopens_every_committed_page_and_retries_exact_bytes() {
    let scope = crate::test_path::ScopedDirectory::new("bootstrap-stage-resume");
    let path = scope.join("stage");
    let (manifest, pages) = fixture();
    let mut stage = RedbBootstrapStage::create(&path, manifest).unwrap();
    assert!(RedbBootstrapStage::open(&path, manifest).is_err());
    for page in &pages {
        let progress = stage.append(&page.encode().unwrap()).unwrap();
        assert_eq!(progress.page_count(), page.ordinal());
        drop(stage);
        stage = RedbBootstrapStage::open(&path, manifest).unwrap();
        assert_eq!(stage.append(&page.encode().unwrap()).unwrap(), progress);
    }
    let verified = stage.verify_complete().unwrap();
    assert_eq!(verified.manifest(), manifest);
    for page in &pages {
        assert_eq!(verified.read_page(page.ordinal()).unwrap(), *page);
    }
}

#[test]
fn bootstrap_stage_cannot_verify_a_partial_transfer_or_rebind_manifest() {
    let scope = crate::test_path::ScopedDirectory::new("bootstrap-stage-refusals");
    let path = scope.join("stage");
    let (manifest, pages) = fixture();
    let mut stage = RedbBootstrapStage::create(&path, manifest).unwrap();
    stage.append(&pages[0].encode().unwrap()).unwrap();
    assert!(stage.verify_complete().is_err());
    let mut stage = RedbBootstrapStage::open(&path, manifest).unwrap();
    let mut changed = pages[0].encode().unwrap();
    changed[60] ^= 1;
    assert!(stage.append(&changed).is_err());
    assert!(stage.append(&pages[1].encode().unwrap()).is_err());
}

#[test]
fn bootstrap_stage_crash_child() {
    let Some(path) = std::env::var_os("RIFFDB_BOOTSTRAP_STAGE_CRASH_PATH") else {
        return;
    };
    let (manifest, pages) = fixture();
    let mut stage = RedbBootstrapStage::open(Path::new(&path), manifest).unwrap();
    let already_durable = stage.progress().page_count() == 2;
    stage.append(&pages[1].encode().unwrap()).unwrap();
    assert!(already_durable, "requested stage crash did not fire");
}
use std::path::Path;

#[test]
fn bootstrap_source_build_cancellation_releases_private_handles_without_a_hold() {
    use redb::{ReadableDatabase, ReadableTableMetadata};
    let (scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let id = ReplicationSourceHoldIdV1::new([0x75; 16]).unwrap();
    let path = scope.join("cancelled-source");
    let epoch = ports.shared.durable_commit_epoch();
    let mut build = ports.begin_replication_bootstrap_v3(&path, id).unwrap();
    assert!(!build.advance().unwrap());
    drop(build);
    // One bounded advance committed exactly one page, with no releasable
    // manifest. Cancellation released the exclusive engine lock.
    let database = redb::Database::open(path.join("transfer.redb")).unwrap();
    let read = database.begin_read().unwrap();
    assert_eq!(
        read.open_table(redb::TableDefinition::<u32, &[u8]>::new(
            "bootstrap_pages_v1"
        ))
        .unwrap()
        .len()
        .unwrap(),
        1
    );
    drop(read);
    drop(database);
    assert!(ports.resume_replication_bootstrap_v3(&path, id).is_err());
    let read = ports.shared.database.begin_read().unwrap();
    assert_eq!(
        read.open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
            .unwrap()
            .len()
            .unwrap(),
        0
    );
    assert_eq!(ports.shared.durable_commit_epoch(), epoch);
}

#[test]
fn bootstrap_source_steps_verify_every_page_before_registering_the_exact_hold() {
    let (scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let id = ReplicationSourceHoldIdV1::new([0x76; 16]).unwrap();
    let path = scope.join("bounded-source");
    let epoch = ports.shared.durable_commit_epoch();
    let mut build = ports.begin_replication_bootstrap_v3(&path, id).unwrap();
    let mut steps = 1;
    while !build.advance().unwrap() {
        steps += 1;
        assert!(steps < 200);
        assert_eq!(ports.shared.durable_commit_epoch(), epoch);
    }
    assert_eq!(ports.shared.durable_commit_epoch(), epoch);
    let source = build.finish().unwrap();
    let manifest = source.manifest();
    // Every page has one copy step and a separate verification step; exact
    // source EOF and the durable manifest use one additional bounded step.
    assert_eq!(steps, manifest.page_count() * 2 + 1);
    assert!(ports.shared.durable_commit_epoch() > epoch);
    drop(source);
    let epoch = ports.shared.durable_commit_epoch();
    let mut resumed = ports
        .begin_replication_bootstrap_resume_v3(&path, id)
        .unwrap();
    for ordinal in 1..=manifest.page_count() {
        assert_eq!(resumed.advance().unwrap(), ordinal == manifest.page_count());
    }
    assert_eq!(resumed.finish().unwrap().manifest(), manifest);
    assert_eq!(ports.shared.durable_commit_epoch(), epoch);
}

#[test]
fn bootstrap_source_pin_is_released_on_cancel_failure_and_before_artifact_verification() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    struct ObservedCursor {
        inner: Box<dyn AuthoritativeStateCursorV3>,
        dropped: Arc<AtomicBool>,
        calls: Arc<AtomicUsize>,
        refuse: bool,
    }
    impl AuthoritativeStateCursorV3 for ObservedCursor {
        fn history(&self) -> ChangelogHistoryStateV3 {
            self.inner.history()
        }
        fn next_item(
            &mut self,
        ) -> Result<Option<AuthoritativeStateStepV3>, ChangelogCursorErrorV3> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.refuse {
                return Err(StorageError::new(StorageErrorKind::Unavailable, None).into());
            }
            self.inner.next_item()
        }
    }
    impl Drop for ObservedCursor {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }
    for scenario in ["cancel", "failure", "complete"] {
        let (scope, ports, _) = crate::changelog_v3_control_tests::fixture();
        let pin = ports.published_changelog_snapshot_v3().unwrap();
        let input = pin.authoritative_state_v3().unwrap();
        let fence = ReplicationBootstrapFenceV3::new(
            ReplicationSourceHoldIdV1::new([0x74; 16]).unwrap(),
            input.history(),
        );
        let dropped = Arc::new(AtomicBool::new(false));
        let calls = Arc::new(AtomicUsize::new(0));
        let mut build = SourceBuild::begin(
            &scope.join("source"),
            fence,
            Box::new(ObservedCursor {
                inner: input,
                dropped: Arc::clone(&dropped),
                calls: Arc::clone(&calls),
                refuse: scenario == "failure",
            }),
        )
        .unwrap();
        drop(pin);
        assert_eq!(calls.load(Ordering::SeqCst), 0, "begin performs no scan");
        assert!(!dropped.load(Ordering::SeqCst));
        match scenario {
            "cancel" => {
                assert!(!build.advance().unwrap());
                drop(build);
            }
            "failure" => {
                assert!(build.advance().is_err());
                assert!(
                    dropped.load(Ordering::SeqCst),
                    "failure immediately releases the pin"
                );
                let after_failure = calls.load(Ordering::SeqCst);
                assert!(build.advance().is_err());
                assert_eq!(calls.load(Ordering::SeqCst), after_failure);
                assert!(build.finish().is_err());
            }
            _ => {
                let mut steps = 0;
                while !dropped.load(Ordering::SeqCst) {
                    let before = calls.load(Ordering::SeqCst);
                    assert!(
                        !build.advance().unwrap(),
                        "verification remains after cursor EOF"
                    );
                    assert!(
                        calls.load(Ordering::SeqCst) - before
                            <= MAX_REPLICATION_BOOTSTRAP_PAGE_ROWS + 1
                    );
                    steps += 1;
                    assert!(steps < 100);
                }
                let after_eof = calls.load(Ordering::SeqCst);
                while !build.advance().unwrap() {
                    steps += 1;
                    assert!(steps < 200);
                }
                build.finish().unwrap();
                assert_eq!(calls.load(Ordering::SeqCst), after_eof);
            }
        }
        assert!(dropped.load(Ordering::SeqCst));
    }
}

#[test]
fn bootstrap_incremental_verification_cannot_release_an_unchecked_prefix() {
    let scope = crate::test_path::ScopedDirectory::new("bootstrap-incremental-verification");
    let path = scope.join("stage");
    let (manifest, pages) = fixture();
    let mut stage = RedbBootstrapStage::create(&path, manifest).unwrap();
    for page in &pages {
        stage.append(&page.encode().unwrap()).unwrap();
    }
    let mut verification = stage.begin_verification().unwrap();
    assert!(!verification.advance().unwrap());
    assert!(verification.finish().is_err());
    let mut verification = RedbBootstrapStage::open(&path, manifest)
        .unwrap()
        .begin_verification()
        .unwrap();
    for ordinal in 1..=manifest.page_count() {
        assert_eq!(
            verification.advance().unwrap(),
            ordinal == manifest.page_count()
        );
    }
    assert_eq!(verification.finish().unwrap().manifest(), manifest);
}

#[test]
fn bootstrap_source_releases_only_complete_artifacts_with_a_durable_exact_hold() {
    use redb::ReadableDatabase;
    let (scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let id = ReplicationSourceHoldIdV1::new([0x77; 16]).unwrap();
    let path = scope.join("source-stage");
    let source = ports.prepare_replication_bootstrap_v3(&path, id).unwrap();
    let manifest = source.manifest();
    let hold = ReplicationSourceHoldV1::new(
        id,
        ReplicationSourceHoldKindV1::Bootstrap,
        manifest.fence().history().lineage(),
        manifest.fence().history().tail(),
    );
    let read = ports.shared.database.begin_read().unwrap();
    let holds = read
        .open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
        .unwrap();
    let retained = holds.get(hold.storage_key().as_slice()).unwrap().unwrap();
    assert_eq!(
        *proto_codec::decode_replication_source_hold_v1(retained.value())
            .unwrap()
            .value(),
        hold
    );
    let mut transcript = ReplicationBootstrapTranscriptV3::new(manifest.fence());
    for ordinal in 1..=manifest.page_count() {
        transcript
            .observe(&source.read_page(ordinal).unwrap())
            .unwrap();
    }
    transcript.verify_manifest(manifest).unwrap();
    drop(retained);
    drop(holds);
    drop(read);
    drop(source);
    let epoch = ports.shared.durable_commit_epoch();
    let resumed = ports.resume_replication_bootstrap_v3(&path, id).unwrap();
    assert_eq!(resumed.manifest(), manifest);
    assert_eq!(
        ports.shared.durable_commit_epoch(),
        epoch,
        "hold retry creates no new receipt"
    );
    drop(resumed);
    assert!(
        ports
            .resume_replication_bootstrap_v3(
                &path,
                ReplicationSourceHoldIdV1::new([0x78; 16]).unwrap()
            )
            .is_err()
    );
}

#[test]
fn bootstrap_source_crash_child() {
    let Some(directory) = std::env::var_os("RIFFDB_BOOTSTRAP_SOURCE_CRASH_PATH") else {
        return;
    };
    let directory = Path::new(&directory);
    let store = crate::RedbStore::open(directory.join("db.redb")).unwrap();
    let ports = crate::RedbOperationalPorts {
        shared: store.shared,
    };
    ports
        .prepare_replication_bootstrap_v3(
            &directory.join("source-stage"),
            ReplicationSourceHoldIdV1::new([0x79; 16]).unwrap(),
        )
        .unwrap();
    panic!("requested source crash did not fire");
}

#[test]
fn bootstrap_source_crashes_cannot_release_partial_artifacts_or_lose_registered_holds() {
    use redb::{ReadableDatabase, ReadableTableMetadata};
    for edge in [
        "source-page-committed",
        "source-manifest-committed",
        "hold-staged",
        "hold-receipted",
        "hold-committed",
    ] {
        let (scope, ports, _) = crate::changelog_v3_control_tests::fixture();
        let database_path = scope.join("db.redb");
        drop(ports);
        let mut child = std::process::Command::new(std::env::current_exe().unwrap());
        child
            .args([
                "--exact",
                "maintenance::bootstrap_stage_tests::bootstrap_source_crash_child",
                "--nocapture",
            ])
            .env(
                "RIFFDB_BOOTSTRAP_SOURCE_CRASH_PATH",
                database_path.parent().unwrap(),
            );
        if edge.starts_with("source-") {
            child.env("RIFFDB_BOOTSTRAP_STAGE_CRASH_EDGE", edge);
        } else {
            child.env("RIFFDB_V3_SOURCE_CONTROL_EDGE", edge);
        }
        assert_eq!(child.status().unwrap().code(), Some(93), "edge {edge}");
        let store = crate::RedbStore::open(&database_path).unwrap();
        let ports = crate::RedbOperationalPorts {
            shared: store.shared,
        };
        let read = ports.shared.database.begin_read().unwrap();
        let holds = read
            .open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
            .unwrap();
        assert_eq!(holds.len().unwrap(), u64::from(edge == "hold-committed"));
        drop(holds);
        drop(read);
        let path = scope.join("source-stage");
        let id = ReplicationSourceHoldIdV1::new([0x79; 16]).unwrap();
        if edge == "source-page-committed" {
            assert!(ports.resume_replication_bootstrap_v3(&path, id).is_err());
        } else {
            let source = ports.resume_replication_bootstrap_v3(&path, id).unwrap();
            let manifest = source.manifest();
            drop(source);
            let epoch = ports.shared.durable_commit_epoch();
            let source = ports.resume_replication_bootstrap_v3(&path, id).unwrap();
            assert_eq!(source.manifest(), manifest);
            assert_eq!(ports.shared.durable_commit_epoch(), epoch);
        }
    }
}

#[test]
fn bootstrap_stage_process_crashes_commit_page_and_progress_atomically() {
    for edge in ["before-commit", "after-commit"] {
        let scope = crate::test_path::ScopedDirectory::new("bootstrap-stage-crash");
        let path = scope.join("stage");
        let (manifest, pages) = fixture();
        let mut stage = RedbBootstrapStage::create(&path, manifest).unwrap();
        stage.append(&pages[0].encode().unwrap()).unwrap();
        drop(stage);
        for attempt in 0..3 {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "maintenance::bootstrap_stage_tests::bootstrap_stage_crash_child",
                    "--nocapture",
                ])
                .env("RIFFDB_BOOTSTRAP_STAGE_CRASH_PATH", &path)
                .env("RIFFDB_BOOTSTRAP_STAGE_CRASH_EDGE", edge)
                .status()
                .unwrap();
            // After the first after-commit crash, the exact retry is read-only
            // and returns without reaching a durable write edge.
            if edge == "after-commit" && attempt > 0 {
                assert_eq!(status.code(), Some(0));
            } else {
                assert_eq!(status.code(), Some(93));
            }
            let stage = RedbBootstrapStage::open(&path, manifest).unwrap();
            assert_eq!(
                stage.progress().page_count(),
                if edge == "before-commit" { 1 } else { 2 }
            );
            drop(stage);
        }
        let mut stage = RedbBootstrapStage::open(&path, manifest).unwrap();
        for page in &pages[1..] {
            stage.append(&page.encode().unwrap()).unwrap();
        }
        stage.verify_complete().unwrap();
    }
}

#[test]
fn bootstrap_full_verification_detects_lost_or_changed_earlier_pages() {
    use redb::{Database, Durability, TableDefinition};
    for remove in [false, true] {
        let scope = crate::test_path::ScopedDirectory::new("bootstrap-stage-corruption");
        let path = scope.join("stage");
        let (manifest, pages) = fixture();
        let mut stage = RedbBootstrapStage::create(&path, manifest).unwrap();
        for page in &pages {
            stage.append(&page.encode().unwrap()).unwrap();
        }
        drop(stage);
        let database = Database::open(path.join("transfer.redb")).unwrap();
        let mut write = database.begin_write().unwrap();
        write.set_durability(Durability::Immediate).unwrap();
        {
            let mut table = write
                .open_table(TableDefinition::<u32, &[u8]>::new("bootstrap_pages_v1"))
                .unwrap();
            if remove {
                table.remove(1).unwrap();
            } else {
                table
                    .insert(1, pages[1].encode().unwrap().as_slice())
                    .unwrap();
            }
        }
        write.commit().unwrap();
        drop(database);
        if remove {
            assert!(RedbBootstrapStage::open(&path, manifest).is_err());
        } else {
            assert!(
                RedbBootstrapStage::open(&path, manifest)
                    .unwrap()
                    .verify_complete()
                    .is_err()
            );
        }
    }
}

#[test]
fn bootstrap_stage_rejects_unknown_tables_and_multimaps() {
    use redb::{Database, MultimapTableDefinition, TableDefinition};
    for multimap in [false, true] {
        let scope = crate::test_path::ScopedDirectory::new("bootstrap-stage-inventory");
        let path = scope.join("stage");
        let (manifest, _) = fixture();
        drop(RedbBootstrapStage::create(&path, manifest).unwrap());
        let database = Database::open(path.join("transfer.redb")).unwrap();
        let write = database.begin_write().unwrap();
        if multimap {
            write
                .open_multimap_table(MultimapTableDefinition::<u8, u8>::new("unknown"))
                .unwrap();
        } else {
            write
                .open_table(TableDefinition::<u8, u8>::new("unknown"))
                .unwrap();
        }
        write.commit().unwrap();
        drop(database);
        assert!(RedbBootstrapStage::open(&path, manifest).is_err());
    }
}

#[cfg(unix)]
#[test]
fn bootstrap_stage_refuses_symlinks_path_replacement_and_public_directory() {
    use std::{
        fs,
        os::unix::fs::{PermissionsExt, symlink},
    };
    let scope = crate::test_path::ScopedDirectory::new("bootstrap-stage-paths");
    let path = scope.join("stage");
    let (manifest, pages) = fixture();
    let mut stage = RedbBootstrapStage::create(&path, manifest).unwrap();
    assert_eq!(fs::metadata(&path).unwrap().permissions().mode() & 0o077, 0);
    let moved = scope.join("moved");
    fs::rename(&path, &moved).unwrap();
    symlink(&moved, &path).unwrap();
    assert!(stage.append(&pages[0].encode().unwrap()).is_err());
    assert!(RedbBootstrapStage::open(&path, manifest).is_err());
    drop(stage);
    fs::remove_file(&path).unwrap();
    fs::rename(&moved, &path).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(RedbBootstrapStage::open(&path, manifest).is_err());
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    let file = path.join("transfer.redb");
    let moved_file = scope.join("moved.redb");
    fs::rename(&file, &moved_file).unwrap();
    symlink(&moved_file, &file).unwrap();
    assert!(RedbBootstrapStage::open(&path, manifest).is_err());
}

#[test]
fn bootstrap_stage_recovers_its_own_manifest_and_durable_page_without_a_peer_claim() {
    let scope = crate::test_path::ScopedDirectory::new("bootstrap-stage-discovery");
    let path = scope.join("stage");
    let (manifest, pages) = fixture();
    let mut stage = RedbBootstrapStage::create(&path, manifest).unwrap();
    stage.append(&pages[0].encode().unwrap()).unwrap();
    assert!(
        RedbBootstrapStage::recover(&path).is_err(),
        "live transfer retains engine exclusion"
    );
    drop(stage);
    let mut recovered = RedbBootstrapStage::recover(&path).unwrap();
    assert_eq!(recovered.progress().manifest(), manifest);
    assert_eq!(recovered.progress().page_count(), 1);
    assert_eq!(
        recovered
            .append(&pages[0].encode().unwrap())
            .unwrap()
            .page_count(),
        1
    );
    assert!(
        recovered.verify_complete().is_err(),
        "local discovery is not complete verification"
    );
}

#[path = "bootstrap_receiver_repository_tests.rs"]
mod receiver_repository;
