//! Actual fresh follower-file construction; no transfer fixture is treated as
//! a catalog or startup proof.
// req: REP-002, REP-003, REC-001, STO-012
use super::*;
use riffdb_storage_api::*;
use riffdb_types::{DatabaseId, DigestKeyId, DualFrontier, Timestamp};

#[path = "bootstrap_publication_tests.rs"]
mod publication;
#[path = "bootstrap_receiver_retirement_tests.rs"]
mod receiver_retirement;

fn inputs() -> StartupValidationInputs {
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
    StartupValidationInputs::new(
        Timestamp::new(1, 0).unwrap(),
        ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
        ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
    )
}
fn source(path: &std::path::Path) -> crate::RedbOperationalPorts {
    let mut store = crate::RedbStore::open(path).unwrap();
    let id = DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10]).unwrap();
    store.initialize_database(id).unwrap();
    crate::changelog_v3_activation::activate_validated(
        store.shared.database.begin_write().unwrap(),
        ChangelogLineageV3::new(id, 1, LeadershipEpochV1::initial()).unwrap(),
        DualFrontier::INITIAL,
    )
    .unwrap();
    crate::RedbOperationalPorts {
        shared: store.shared,
    }
}
fn transfer(scope: &crate::test_path::ScopedDirectory) -> RedbBootstrapMaterializationInput {
    let ports = source(&scope.join("source.redb"));
    transfer_from_ports(scope, ports)
}

fn transfer_from_ports(
    scope: &crate::test_path::ScopedDirectory,
    ports: crate::RedbOperationalPorts,
) -> RedbBootstrapMaterializationInput {
    let held = ports
        .prepare_replication_bootstrap_v3(
            &scope.join("source-transfer"),
            ReplicationSourceHoldIdV1::new([0x73; 16]).unwrap(),
        )
        .unwrap();
    let mut receiver =
        RedbBootstrapStage::create(&scope.join("receiver-transfer"), held.manifest()).unwrap();
    for ordinal in 1..=held.manifest().page_count() {
        receiver
            .append(&held.read_page(ordinal).unwrap().encode().unwrap())
            .unwrap();
    }
    receiver.into_materialization_input().unwrap()
}

#[test]
fn bootstrap_constructs_a_new_exact_follower_and_passes_complete_startup_scrub() {
    let scope = crate::test_path::ScopedDirectory::new("bootstrap-materialize");
    let transfer = transfer(&scope);
    let manifest = transfer.manifest();
    let path = scope.join("candidate");
    let mut materializer = RedbBootstrapMaterializer::create(&path, transfer).unwrap();
    assert!(crate::RedbFollowerStore::open(path.join("follower.redb")).is_err());
    while materializer.copy_next_page().unwrap().is_some() {}
    let candidate = materializer.finish().unwrap();
    assert_eq!(candidate.manifest(), manifest);
    assert!(crate::RedbStore::open(path.join("follower.redb")).is_err());
    let validated = candidate.validate(inputs()).unwrap();
    assert_eq!(validated.manifest(), manifest);
    drop(validated);
    assert!(crate::RedbFollowerStore::open(path.join("follower.redb")).is_ok());
}

fn reopen_transfer(
    directory: &std::path::Path,
    manifest: ReplicationBootstrapManifestV1,
) -> RedbBootstrapMaterializationInput {
    RedbBootstrapStage::open(&directory.join("receiver-transfer"), manifest)
        .unwrap()
        .into_materialization_input()
        .unwrap()
}

#[test]
fn bootstrap_materialization_process_child() {
    let Some(directory) = std::env::var_os("RIFFDB_BOOTSTRAP_MATERIALIZE_PATH") else {
        return;
    };
    let directory = std::path::Path::new(&directory);
    let manifest = ReplicationBootstrapManifestV1::decode(
        &std::fs::read(directory.join("manifest.bin")).unwrap(),
    )
    .unwrap();
    let mut materializer = RedbBootstrapMaterializer::open(
        &directory.join("candidate"),
        reopen_transfer(directory, manifest),
    )
    .unwrap();
    if std::env::var("RIFFDB_BOOTSTRAP_MATERIALIZE_EDGE")
        .unwrap()
        .starts_with("rows-")
    {
        materializer.copy_next_page().unwrap();
    } else {
        materializer.finish().unwrap();
    }
    panic!("requested materialization crash did not fire");
}

#[test]
fn bootstrap_materialization_crashes_preserve_atomic_rows_progress_and_seal() {
    use redb::ReadableDatabase;
    for edge in [
        "rows-staged",
        "rows-committed",
        "seal-staged",
        "seal-committed",
    ] {
        let scope = crate::test_path::ScopedDirectory::new("bootstrap-materialize-crash");
        let transfer = transfer(&scope);
        let manifest = transfer.manifest();
        let target = (1..=manifest.page_count())
            .find(|n| {
                transfer.read_page(*n).unwrap().namespace()
                    == AuthoritativeNamespaceV1::DatabaseIdentity
            })
            .unwrap();
        std::fs::write(scope.join("manifest.bin"), manifest.encode().unwrap()).unwrap();
        let candidate_path = scope.join("candidate");
        let mut materializer =
            RedbBootstrapMaterializer::create(&candidate_path, transfer).unwrap();
        let count = if edge.starts_with("rows-") {
            target - 1
        } else {
            manifest.page_count()
        };
        for _ in 0..count {
            materializer.copy_next_page().unwrap();
        }
        drop(materializer);
        let directory = candidate_path.parent().unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "maintenance::bootstrap_materialize_tests::bootstrap_materialization_process_child",
                "--nocapture",
            ])
            .env("RIFFDB_BOOTSTRAP_MATERIALIZE_PATH", directory)
            .env("RIFFDB_BOOTSTRAP_MATERIALIZE_EDGE", edge)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(93), "edge {edge}");
        let database = redb::Database::open(candidate_path.join("follower.redb")).unwrap();
        let read = database.begin_read().unwrap();
        let meta = read.open_table(crate::layout::META).unwrap();
        assert_eq!(
            meta.get("database_id").unwrap().is_some(),
            edge != "rows-staged"
        );
        drop(meta);
        drop(read);
        drop(database);
        let mut materializer =
            RedbBootstrapMaterializer::open(&candidate_path, reopen_transfer(directory, manifest))
                .unwrap();
        assert_eq!(
            materializer.progress().page_count(),
            if edge == "rows-staged" {
                target - 1
            } else if edge == "rows-committed" {
                target
            } else {
                manifest.page_count()
            }
        );
        while materializer.copy_next_page().unwrap().is_some() {}
        materializer.finish().unwrap().validate(inputs()).unwrap();
    }
}

#[test]
fn bootstrap_materialization_rejects_lost_earlier_authority() {
    let scope = crate::test_path::ScopedDirectory::new("bootstrap-materialize-corruption");
    let transfer = transfer(&scope);
    let manifest = transfer.manifest();
    let path = scope.join("candidate");
    let mut materializer = RedbBootstrapMaterializer::create(&path, transfer).unwrap();
    while materializer.copy_next_page().unwrap().is_some() {}
    drop(materializer);
    let database = redb::Database::open(path.join("follower.redb")).unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(crate::layout::META)
        .unwrap()
        .remove("database_id")
        .unwrap();
    write.commit().unwrap();
    drop(database);
    let directory = path.parent().unwrap();
    let materializer =
        RedbBootstrapMaterializer::open(&path, reopen_transfer(directory, manifest)).unwrap();
    assert!(materializer.finish().is_err());
}

#[test]
fn complete_bootstrap_framing_cannot_bypass_authoritative_semantic_validation() {
    let scope = crate::test_path::ScopedDirectory::new("bootstrap-invalid-authority");
    let ports = source(&scope.join("source.redb"));
    // Deliberately forge a physical fixture below the application boundary. Its
    // checksum/receipt is real, but its orphan outbox status is not valid state.
    let write = crate::changelog_v3_write::CapturedImmediateWrite::begin(
        &ports.shared.database,
        crate::RedbCommitProfile::Hardened,
        ChangelogAttributionV3::OutboxTransition,
    )
    .unwrap();
    write
        .open_table(crate::layout::OUTBOX_STATUS)
        .unwrap()
        .insert(b"orphan".as_slice(), b"invalid".as_slice())
        .unwrap();
    write.finish().unwrap().commit(&ports.shared).unwrap();
    let transfer = transfer_from_ports(&scope, ports);
    let mut materializer =
        RedbBootstrapMaterializer::create(&scope.join("candidate"), transfer).unwrap();
    while materializer.copy_next_page().unwrap().is_some() {}
    let candidate = materializer.finish().unwrap();
    assert!(candidate.validate(inputs()).is_err());
}

#[cfg(unix)]
#[test]
fn bootstrap_construction_exclusion_survives_validation_and_paths_cannot_be_rebound() {
    let scope = crate::test_path::ScopedDirectory::new("bootstrap-materialize-exclusion");
    let transfer = transfer(&scope);
    let manifest = transfer.manifest();
    let second_transfer = || {
        RedbBootstrapStage::open(&scope.join("source-transfer"), manifest)
            .unwrap()
            .into_materialization_input()
            .unwrap()
    };
    let path = scope.join("candidate");
    let mut materializer = RedbBootstrapMaterializer::create(&path, transfer).unwrap();
    assert!(RedbBootstrapMaterializer::open(&path, second_transfer()).is_err());
    while materializer.copy_next_page().unwrap().is_some() {}
    let candidate = materializer.finish().unwrap().validate(inputs()).unwrap();
    assert!(RedbBootstrapMaterializer::open(&path, second_transfer()).is_err());
    let moved = scope.join("moved");
    std::fs::rename(&path, &moved).unwrap();
    std::os::unix::fs::symlink(&moved, &path).unwrap();
    assert!(candidate.verify_private_identity().is_err());
    assert!(RedbBootstrapMaterializer::open(&path, second_transfer()).is_err());
}

#[test]
fn bootstrap_materialization_accepts_valid_page_boundaries_without_repacking() {
    let scope = crate::test_path::ScopedDirectory::new("bootstrap-materialize-page-boundaries");
    let ports = source(&scope.join("source.redb"));
    let source = ports
        .prepare_replication_bootstrap_v3(
            &scope.join("source-transfer"),
            ReplicationSourceHoldIdV1::new([0x75; 16]).unwrap(),
        )
        .unwrap();
    let fence = source.manifest().fence();
    let mut transcript = ReplicationBootstrapTranscriptV3::new(fence);
    let mut hash = fence.digest();
    let mut pages = Vec::new();
    let mut split = false;
    for ordinal in 1..=source.manifest().page_count() {
        let page = source.read_page(ordinal).unwrap();
        let split_here = !split && !page.rows().is_empty();
        for (rows, end) in if split_here {
            split = true;
            vec![(page.rows().to_vec(), false), (vec![], true)]
        } else {
            vec![(page.rows().to_vec(), page.ends_namespace())]
        } {
            let repaged = ReplicationBootstrapPageV3::new(
                fence.digest(),
                pages.len() as u32 + 1,
                page.namespace(),
                end,
                hash,
                rows,
            )
            .unwrap();
            transcript.observe(&repaged).unwrap();
            hash = *repaged.encode().unwrap().last_chunk::<32>().unwrap();
            pages.push(repaged);
        }
    }
    assert!(split);
    let manifest = transcript.manifest().unwrap();
    assert_eq!(manifest.page_count(), source.manifest().page_count() + 1);
    let mut receiver =
        RedbBootstrapStage::create(&scope.join("receiver-transfer"), manifest).unwrap();
    for page in pages {
        receiver.append(&page.encode().unwrap()).unwrap();
    }
    let mut materializer = RedbBootstrapMaterializer::create(
        &scope.join("candidate"),
        receiver.into_materialization_input().unwrap(),
    )
    .unwrap();
    while materializer.copy_next_page().unwrap().is_some() {}
    materializer.finish().unwrap().validate(inputs()).unwrap();
}

#[test]
fn bootstrap_catalog_preflight_retains_exclusion_and_cannot_grant_startup_ports() {
    let scope = crate::test_path::ScopedDirectory::new("bootstrap-catalog-preflight");
    let transfer = transfer(&scope);
    let mut materializer =
        RedbBootstrapMaterializer::create(&scope.join("candidate"), transfer).unwrap();
    while materializer.copy_next_page().unwrap().is_some() {}
    let candidate = materializer.finish().unwrap();
    let mut session = candidate.begin_catalog_preflight(inputs()).unwrap();
    assert!(crate::RedbFollowerStore::open(scope.join("candidate/follower.redb")).is_err());
    let id = session.open_session_id();
    assert!(
        session
            .read_structural_evidence(
                StructuralEvidenceCursor::start(session.database_id(), id),
                EvidencePageLimit::new(1).unwrap()
            )
            .is_err()
    );
    let mut cursor = HistoricalEvidenceCursor::start(session.database_id(), id);
    let end = loop {
        match session
            .read_historical_evidence(cursor, EvidencePageLimit::new(1).unwrap())
            .unwrap()
        {
            HistoricalEvidencePage::Page { next, .. } => cursor = next,
            HistoricalEvidencePage::ExactEnd(end) => break end,
        }
    };
    let candidate = session.finish_preflight(end).unwrap();
    assert_eq!(candidate.catalog_validation_session(), Some(id));
    candidate.validate(inputs()).unwrap();
}

// req: REP-002, REP-003, REC-001
#[test]
fn bootstrap_catalog_preflight_cancellation_releases_the_candidate_and_final_scrub_refuses_cancel()
{
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    let scope = crate::test_path::ScopedDirectory::new("bootstrap-catalog-cancel");
    let transfer = transfer(&scope);
    let manifest = transfer.manifest();
    let path = scope.join("candidate");
    let mut materializer = RedbBootstrapMaterializer::create(&path, transfer).unwrap();
    while materializer.copy_next_page().unwrap().is_some() {}
    let flag = Arc::new(AtomicBool::new(false));
    let mut session = materializer
        .finish()
        .unwrap()
        .begin_catalog_preflight(inputs())
        .unwrap()
        .with_cancellation(Arc::clone(&flag));
    let cursor = HistoricalEvidenceCursor::start(session.database_id(), session.open_session_id());
    flag.store(true, Ordering::Release);
    assert_eq!(
        session
            .read_historical_evidence(cursor, EvidencePageLimit::new(1).unwrap())
            .unwrap_err()
            .kind(),
        StorageErrorKind::Unavailable
    );
    drop(session);
    let candidate =
        RedbBootstrapMaterializer::open(&path, reopen_transfer(path.parent().unwrap(), manifest))
            .unwrap()
            .finish()
            .unwrap();
    assert!(candidate.validate_cancellable(inputs(), flag).is_err());
    let candidate =
        RedbBootstrapMaterializer::open(&path, reopen_transfer(path.parent().unwrap(), manifest))
            .unwrap()
            .finish()
            .unwrap();
    candidate.validate(inputs()).unwrap();
}
