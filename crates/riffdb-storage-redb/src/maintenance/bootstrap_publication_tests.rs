//! Physical publication evidence. Server composition additionally requires its
//! sealed semantic projection rebuild before invoking this storage boundary.
// req: REP-002, REP-003, REC-001, STO-012
use super::*;

fn candidate(scope: &crate::test_path::ScopedDirectory) -> RedbValidatedBootstrapCandidate {
    let mut materializer =
        RedbBootstrapMaterializer::create(&scope.join("candidate"), transfer(scope)).unwrap();
    while materializer.copy_next_page().unwrap().is_some() {}
    materializer.finish().unwrap().validate(inputs()).unwrap()
}

#[test]
fn bootstrap_publication_preserves_lineage_and_holds_engine_exclusion_until_release() {
    let scope = crate::test_path::ScopedDirectory::new("bootstrap-publication");
    let candidate = candidate(&scope);
    let manifest = candidate.manifest();
    let path = scope.join("live.redb");
    let published = candidate.publish(&path).unwrap();
    assert_eq!(published.manifest(), manifest);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    assert!(crate::RedbFollowerStore::open(&path).is_err());
    assert!(crate::RedbStore::open(&path).is_err());
    assert_eq!(published.release_for_startup().unwrap(), manifest);
    crate::startup::RedbOfflineIntegrityScrub::from_inputs(&path, inputs())
        .run_follower()
        .unwrap();
    drop(crate::RedbFollowerStore::open(&path).unwrap());
    assert!(!crate::journal::journal_path(&path).exists());
    // The private candidate remains available after publication for crash
    // retries. Both names retain the same engine lock and exact source roots.
    let candidate = RedbBootstrapMaterializer::open(
        &scope.join("candidate"),
        reopen_transfer(path.parent().unwrap(), manifest),
    )
    .unwrap()
    .finish()
    .unwrap()
    .validate(inputs())
    .unwrap();
    assert_eq!(
        candidate
            .publish(&path)
            .unwrap()
            .release_for_startup()
            .unwrap(),
        manifest
    );
}

#[test]
fn follower_startup_cannot_release_a_store_after_an_uncertain_namespace_sync() {
    let scope = crate::test_path::ScopedDirectory::new("bootstrap-open-sync-failure");
    let candidate = candidate(&scope);
    let manifest = candidate.manifest();
    let path = scope.join("live.redb");
    candidate
        .publish(&path)
        .unwrap()
        .release_for_startup()
        .unwrap();
    assert_eq!(
        crate::RedbFollowerStore::open_with_failed_namespace_sync(&path)
            .unwrap_err()
            .kind(),
        StorageErrorKind::Unavailable
    );
    // Refusal releases the real engine lock and no source journal/lifecycle is
    // introduced. A fresh open must establish its own successful sync boundary.
    drop(crate::RedbFollowerStore::open(&path).unwrap());
    assert!(!crate::journal::journal_path(&path).exists());
    let database = redb::ReadOnlyDatabase::open(&path).unwrap();
    use redb::ReadableDatabase;
    assert_eq!(
        crate::changelog_v3_roots::read_checkpoint_roots(&database.begin_read().unwrap()).unwrap(),
        Some(manifest.fence().history())
    );
}

#[test]
fn bootstrap_publication_refuses_a_primary_and_a_live_follower_then_replaces_the_closed_follower() {
    let scope = crate::test_path::ScopedDirectory::new("bootstrap-publication-refusals");
    let first = candidate(&scope);
    let manifest = first.manifest();
    let source_path = scope.join("source.redb");
    let original = std::fs::read(&source_path).unwrap();
    assert!(first.publish(&source_path).is_err());
    assert!(
        std::fs::read(&source_path).unwrap() == original,
        "a refused source file stays byte-exact"
    );
    let retry = RedbBootstrapMaterializer::open(
        &scope.join("candidate"),
        reopen_transfer(scope.join("candidate").parent().unwrap(), manifest),
    )
    .unwrap()
    .finish()
    .unwrap()
    .validate(inputs())
    .unwrap();
    let path = scope.join("live.redb");
    retry.publish(&path).unwrap().release_for_startup().unwrap();
    let active = crate::RedbFollowerStore::open(&path).unwrap();
    let second_scope = crate::test_path::ScopedDirectory::new("bootstrap-publication-replacement");
    let second = candidate(&second_scope);
    let second_manifest = second.manifest();
    assert!(second.publish(&path).is_err());
    drop(active);
    let second = RedbBootstrapMaterializer::open(
        &second_scope.join("candidate"),
        reopen_transfer(
            second_scope.join("candidate").parent().unwrap(),
            second_manifest,
        ),
    )
    .unwrap()
    .finish()
    .unwrap()
    .validate(inputs())
    .unwrap();
    assert_eq!(
        second
            .publish(&path)
            .unwrap()
            .release_for_startup()
            .unwrap(),
        second_manifest
    );
    crate::startup::RedbOfflineIntegrityScrub::from_inputs(&path, inputs())
        .run_follower()
        .unwrap();
}

#[test]
fn bootstrap_publication_crash_child() {
    let Some(directory) = std::env::var_os("RIFFDB_BOOTSTRAP_PUBLICATION_PATH") else {
        return;
    };
    let directory = std::path::Path::new(&directory);
    let manifest = ReplicationBootstrapManifestV1::decode(
        &std::fs::read(directory.join("manifest.bin")).unwrap(),
    )
    .unwrap();
    let candidate = RedbBootstrapMaterializer::open(
        &directory.join("candidate"),
        reopen_transfer(directory, manifest),
    )
    .unwrap()
    .finish()
    .unwrap()
    .validate(inputs())
    .unwrap();
    candidate.publish(&directory.join("live.redb")).unwrap();
    panic!("requested publication crash did not fire");
}

fn candidate_from_source(
    scope: &crate::test_path::ScopedDirectory,
    ports: &crate::RedbOperationalPorts,
    id: u8,
) -> RedbValidatedBootstrapCandidate {
    let held = ports
        .prepare_replication_bootstrap_v3(
            &scope.join("source-transfer"),
            ReplicationSourceHoldIdV1::new([id; 16]).unwrap(),
        )
        .unwrap();
    let mut stage =
        RedbBootstrapStage::create(&scope.join("receiver-transfer"), held.manifest()).unwrap();
    for ordinal in 1..=held.manifest().page_count() {
        stage
            .append(&held.read_page(ordinal).unwrap().encode().unwrap())
            .unwrap();
    }
    let mut materializer = RedbBootstrapMaterializer::create(
        &scope.join("candidate"),
        stage.into_materialization_input().unwrap(),
    )
    .unwrap();
    while materializer.copy_next_page().unwrap().is_some() {}
    materializer.finish().unwrap().validate(inputs()).unwrap()
}

#[test]
fn bootstrap_publication_cannot_roll_a_follower_back_to_an_older_source_fence() {
    use redb::ReadableDatabase;
    let primary = crate::test_path::ScopedDirectory::new("bootstrap-publication-source");
    let ports = source(&primary.join("source.redb"));
    let old_scope = crate::test_path::ScopedDirectory::new("bootstrap-publication-older");
    let new_scope = crate::test_path::ScopedDirectory::new("bootstrap-publication-newer");
    let older = candidate_from_source(&old_scope, &ports, 0x41);
    let newer = candidate_from_source(&new_scope, &ports, 0x42);
    let expected = newer.manifest();
    assert!(
        older.manifest().fence().history().tail().sequence()
            < expected.fence().history().tail().sequence()
    );
    let target = primary.join("live.redb");
    newer
        .publish(&target)
        .unwrap()
        .release_for_startup()
        .unwrap();
    let before = std::fs::read(&target).unwrap();
    assert!(older.publish(&target).is_err());
    assert!(
        std::fs::read(&target).unwrap() == before,
        "refused rollback does not touch the existing follower"
    );
    let database = redb::ReadOnlyDatabase::open(&target).unwrap();
    assert_eq!(
        crate::changelog_v3_roots::read_checkpoint_roots(&database.begin_read().unwrap()).unwrap(),
        Some(expected.fence().history())
    );
}

#[test]
fn bootstrap_publication_rejects_substituted_candidates_markers_and_temporary_files() {
    for what in ["candidate", "marker", "temporary"] {
        let scope = crate::test_path::ScopedDirectory::new("bootstrap-publication-substitution");
        let candidate = candidate(&scope);
        let path = scope.join("live.redb");
        let original = match what {
            "candidate" => scope.join("candidate/follower.redb"),
            "marker" => crate::durable_format_marker_path(&scope.join("candidate/follower.redb")),
            _ => scope.join(&format!(".riffdb-bootstrap-{}.redb", "73".repeat(16))),
        };
        if what == "candidate" {
            std::fs::rename(&original, scope.join("retained-candidate.redb")).unwrap();
        }
        std::fs::write(&original, b"unrelated-private-file").unwrap();
        assert!(candidate.publish(&path).is_err());
        assert!(!path.exists());
        assert_eq!(std::fs::read(&original).unwrap(), b"unrelated-private-file");
    }
}

#[test]
fn bootstrap_publication_repeated_crashes_keep_a_recoverable_candidate_and_gate_partial_replacements()
 {
    for existing in [false, true] {
        for edge in [
            "publication-prepared",
            "publication-marker-removed",
            "publication-database-renamed",
            "publication-marker-renamed",
            "publication-parent-synced",
        ] {
            let scope = crate::test_path::ScopedDirectory::new("bootstrap-publication-crash");
            let candidate = candidate(&scope);
            let manifest = candidate.manifest();
            drop(candidate);
            std::fs::write(scope.join("manifest.bin"), manifest.encode().unwrap()).unwrap();
            let old_scope =
                crate::test_path::ScopedDirectory::new("bootstrap-publication-old-target");
            let path = scope.join("live.redb");
            if existing {
                self::candidate(&old_scope)
                    .publish(&path)
                    .unwrap()
                    .release_for_startup()
                    .unwrap();
            }
            for _ in 0..3 {
                let status = std::process::Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", "maintenance::bootstrap_materialize_tests::publication::bootstrap_publication_crash_child"])
                    .env("RIFFDB_BOOTSTRAP_PUBLICATION_PATH", path.parent().unwrap())
                    .env("RIFFDB_BOOTSTRAP_PUBLICATION_EDGE", edge).status().unwrap();
                assert_eq!(status.code(), Some(93), "{edge}, existing={existing}");
                assert!(scope.join("candidate/follower.redb").is_file());
                if matches!(
                    edge,
                    "publication-marker-removed" | "publication-database-renamed"
                ) || (!existing && edge == "publication-prepared")
                {
                    assert!(crate::RedbFollowerStore::open(&path).is_err());
                } else {
                    crate::startup::RedbOfflineIntegrityScrub::from_inputs(&path, inputs())
                        .run_follower()
                        .unwrap();
                }
            }
            let candidate = RedbBootstrapMaterializer::open(
                &scope.join("candidate"),
                reopen_transfer(path.parent().unwrap(), manifest),
            )
            .unwrap()
            .finish()
            .unwrap()
            .validate(inputs())
            .unwrap();
            assert_eq!(
                candidate
                    .publish(&path)
                    .unwrap()
                    .release_for_startup()
                    .unwrap(),
                manifest
            );
            crate::startup::RedbOfflineIntegrityScrub::from_inputs(&path, inputs())
                .run_follower()
                .unwrap();
        }
    }
}
