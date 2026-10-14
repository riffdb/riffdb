//! Native publication intent fences actual engine open and complete restart.
// req: REC-001, REC-004, REC-005, REC-006, REC-008
use super::*;
use crate::maintenance::codec::publication_intent::{PublicationIntent, PublicationPhase};
use crate::maintenance::staged::transfer_tests::prepared_transfer_fixture;
use crate::test_path::ScopedDirectory;
use redb::ReadableTable;
use riffdb_storage_api::{
    NoChangelogPublicationPort, OfflineMaintenanceReceiptTransitionV1,
    ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
    StructuralEvidenceOpen,
};
use riffdb_types::{DigestKeyId, Timestamp};

fn inputs() -> StartupValidationInputs {
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
    StartupValidationInputs::new(
        Timestamp::new(1, 0).unwrap(),
        ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
        ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
    )
}

#[cfg(unix)]
fn prepare_native_transfer_source(store: crate::RedbStore) {
    let session = store.begin_structural_evidence(inputs()).unwrap();
    drop(
        finish_native_session(session)
            .into_operational_after_catalog_validation()
            .unwrap(),
    );
}

#[cfg(unix)]
fn fresh_published(
    root: &Path,
) -> (
    RedbMaintenanceStorage,
    OfflineMaintenanceOperationId,
    Vec<(PathBuf, riffdb_storage_api::BackupIntegrityChecksumV1)>,
) {
    fresh_published_observing(root, None, None)
}

#[cfg(unix)]
fn fresh_published_observing(
    root: &Path,
    observe_materialization: Option<fn(usize)>,
    observe_publication: Option<fn(usize)>,
) -> (
    RedbMaintenanceStorage,
    OfflineMaintenanceOperationId,
    Vec<(PathBuf, riffdb_storage_api::BackupIntegrityChecksumV1)>,
) {
    use crate::maintenance::staged::transfer_tests::{
        NativeTransferFixture, native_transfer_fixture_with_prepared_source,
    };
    let NativeTransferFixture {
        mut storage,
        sealed,
        operation,
        named,
        ..
    } = native_transfer_fixture_with_prepared_source(
        root,
        true,
        prepare_native_transfer_source,
        observe_materialization,
    );
    let originals: Vec<_> = fs::read_dir(&named)
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            let digest = crate::backup::sha256_file(&path).unwrap();
            (path, digest)
        })
        .collect();
    let before_publication = crate::backup::BACKUP_CONTENT_PASSES.with(std::cell::Cell::get);
    storage
        .publish_sealed_restore(sealed, OfflineRestoreOverwritePolicyV1::RefuseNonEmpty)
        .unwrap();
    if let Some(observe) = observe_publication {
        observe(
            crate::backup::BACKUP_CONTENT_PASSES.with(std::cell::Cell::get) - before_publication,
        );
    }
    let mut receipt = read_receipt_file(&storage.receipt_path(operation)).unwrap();
    for phase in [
        OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
        OfflineMaintenanceReceiptPhaseV1::Validating,
    ] {
        receipt
            .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
            .unwrap();
        storage.replace_receipt(&receipt).unwrap();
    }
    (storage, operation, originals)
}

#[cfg(unix)]
#[test]
// req: END-001, END-008, REC-001, REC-006, REC-008
fn fresh_native_publication_startup_retains_the_original_verified_backup_owner() {
    let scope = ScopedDirectory::new("fresh-publication-original-backup-owner");
    let root = scope.join("fixture");
    fs::create_dir(&root).unwrap();
    let (storage, operation, originals) = fresh_published(&root);
    let acquired = crate::backup::OwnedBackupHistory::test_acquisition_count();
    let session = storage
        .begin_published_restore_readiness(
            operation,
            inputs(),
            crate::RedbCommitProfile::Standard,
            Arc::new(NoChangelogPublicationPort),
        )
        .unwrap();
    assert_eq!(
        crate::backup::OwnedBackupHistory::test_acquisition_count(),
        acquired,
        "fresh publication must retain its actual original backup owner through startup consumption"
    );
    assert_eq!(
        current(&storage, operation).phase,
        PublicationPhase::StartupConsumed
    );
    drop(session);
    for (path, expected) in originals {
        assert_eq!(crate::backup::sha256_file(&path).unwrap(), expected);
    }
}

fn published(root: &Path) -> (RedbMaintenanceStorage, OfflineMaintenanceOperationId) {
    let (mut storage, _, _, operation) = prepared_transfer_fixture(root);
    storage
        .prepare_native_publication_recovery()
        .unwrap()
        .unwrap()
        .resume_publication(&storage)
        .unwrap();
    let mut receipt = read_receipt_file(&storage.receipt_path(operation)).unwrap();
    for phase in [
        OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
        OfflineMaintenanceReceiptPhaseV1::Validating,
    ] {
        receipt
            .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
            .unwrap();
        storage.replace_receipt(&receipt).unwrap();
    }
    (storage, operation)
}

#[cfg(unix)]
#[test]
// req: END-001, END-008, REC-001, REC-006, REC-008
fn retained_publication_has_one_consumer_and_abandoned_consumption_cannot_reconstruct() {
    for abandon in [false, true] {
        let scope = ScopedDirectory::new("retained-publication-single-consumer");
        let root = scope.join("fixture");
        fs::create_dir(&root).unwrap();
        let (storage, operation, originals) = fresh_published(&root);
        let acquired = crate::backup::OwnedBackupHistory::test_acquisition_count();
        let first = storage
            .retained_publication
            .take(operation)
            .unwrap()
            .unwrap();
        // Deterministic real-thread schedule: the first consuming grant is
        // retained before the contender runs; neither holds the mutex for I/O.
        std::thread::scope(|threads| {
            threads
                .spawn(|| {
                    assert!(storage.retained_publication.take(operation).is_err());
                })
                .join()
                .unwrap();
        });
        if abandon {
            drop(first);
            assert!(
                storage
                    .begin_published_restore_readiness(
                        operation,
                        inputs(),
                        crate::RedbCommitProfile::Standard,
                        Arc::new(NoChangelogPublicationPort),
                    )
                    .is_err()
            );
            assert_eq!(
                current(&storage, operation).phase,
                PublicationPhase::Published
            );
        } else {
            let startup = first.consume(&storage).unwrap();
            assert!(!startup.requires_complete_validation());
            assert_eq!(
                current(&storage, operation).phase,
                PublicationPhase::StartupConsumed
            );
            let repeated = storage
                .consume_native_publication_for_startup(operation)
                .unwrap()
                .unwrap();
            assert!(
                repeated.requires_complete_validation(),
                "a consumed original owner cannot grant a second fresh startup shortcut"
            );
        }
        assert_eq!(
            crate::backup::OwnedBackupHistory::test_acquisition_count(),
            acquired
        );
        for (path, expected) in originals {
            assert_eq!(crate::backup::sha256_file(&path).unwrap(), expected);
        }
    }
}

#[cfg(unix)]
#[test]
// req: END-001, END-008, REC-001, REC-006, REC-008
fn retained_publication_refuses_changed_inputs_and_never_launders_a_failed_owner() {
    use std::os::unix::fs::FileExt;
    for mode in 0..9 {
        let scope = ScopedDirectory::new("retained-publication-sticky-custody");
        let root = scope.join("fixture");
        fs::create_dir(&root).unwrap();
        let (storage, operation, originals) = fresh_published(&root);
        let acquired = crate::backup::OwnedBackupHistory::test_acquisition_count();
        let extra = storage
            .staged_directory
            .join(operation.to_string())
            .join("unexpected");
        let selected = match mode {
            0 | 6 | 7 => storage.database_file.clone(),
            1 => crate::journal::journal_path(&storage.database_file),
            2 => crate::durable_format_marker_path(&storage.database_file),
            3 => originals
                .iter()
                .find(|(p, _)| {
                    p.file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .starts_with("journal.sealed.")
                })
                .unwrap()
                .0
                .clone(),
            4 => originals
                .iter()
                .find(|(p, _)| p.file_name().unwrap() == "manifest.riffdb")
                .unwrap()
                .0
                .clone(),
            _ => extra.clone(),
        };
        if mode < 5 {
            let original = fs::read(&selected).unwrap();
            let metadata = fs::metadata(&selected).unwrap();
            let file = OpenOptions::new().write(true).open(&selected).unwrap();
            file.write_all_at(&[original[0] ^ 1], 0).unwrap();
            file.write_all_at(&original[..1], 0).unwrap();
            file.set_modified(metadata.modified().unwrap()).unwrap();
            file.sync_all().unwrap();
        } else if mode == 5 {
            fs::write(&extra, b"unexpected private entry").unwrap();
        } else if mode == 6 {
            let original = fs::read(&selected).unwrap();
            fs::rename(&selected, root.join("replaced-original-primary")).unwrap();
            fs::write(&selected, original).unwrap();
        } else if mode == 7 {
            fs::hard_link(&selected, root.join("extra-primary-link")).unwrap();
        }
        let requested = if mode == 8 {
            OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1234, [0x91; 10])
                .unwrap()
        } else {
            operation
        };
        assert!(
            storage
                .begin_published_restore_readiness(
                    requested,
                    inputs(),
                    crate::RedbCommitProfile::Standard,
                    Arc::new(NoChangelogPublicationPort),
                )
                .is_err(),
            "changed original handoff must refuse: {mode}"
        );
        if mode == 5 {
            fs::remove_file(&extra).unwrap();
        }
        assert!(
            storage
                .begin_published_restore_readiness(
                    operation,
                    inputs(),
                    crate::RedbCommitProfile::Standard,
                    Arc::new(NoChangelogPublicationPort),
                )
                .is_err(),
            "failure cannot select a new cold owner: {mode}"
        );
        assert_eq!(
            crate::backup::OwnedBackupHistory::test_acquisition_count(),
            acquired
        );
        assert_eq!(
            current(&storage, operation).phase,
            PublicationPhase::Published
        );
        assert_eq!(
            read_receipt_file(&storage.receipt_path(operation))
                .unwrap()
                .current_phase(),
            OfflineMaintenanceReceiptPhaseV1::Validating
        );
        for (path, expected) in originals {
            assert_eq!(crate::backup::sha256_file(&path).unwrap(), expected);
        }
    }
}

fn current(
    storage: &RedbMaintenanceStorage,
    operation: OfflineMaintenanceOperationId,
) -> PublicationIntent {
    PublicationIntent::decode(
        &fs::read(
            storage
                .receipts_directory
                .join(format!("{operation}.publication-intent-v1")),
        )
        .unwrap(),
    )
    .unwrap()
}

#[test]
fn native_publication_startup_consumes_intent_before_engine_open() {
    let scope = ScopedDirectory::new("native-startup-consumption");
    let root = scope.join("fixture");
    fs::create_dir(&root).unwrap();
    let (storage, operation) = published(&root);
    let session = storage
        .begin_published_restore_readiness(
            operation,
            inputs(),
            crate::RedbCommitProfile::Standard,
            Arc::new(NoChangelogPublicationPort),
        )
        .unwrap();
    assert_eq!(
        current(&storage, operation).phase,
        PublicationPhase::StartupConsumed,
        "native startup must durably consume Published before opening its engine"
    );
    assert_eq!(
        read_receipt_file(&storage.receipt_path(operation))
            .unwrap()
            .current_phase(),
        OfflineMaintenanceReceiptPhaseV1::Validating
    );
    drop(session);
}

fn consume_without_open(
    storage: &mut RedbMaintenanceStorage,
    operation: OfflineMaintenanceOperationId,
) {
    let before = current(storage, operation);
    storage.test_controller = Some(RedbMaintenanceTestController::return_at(
        RedbMaintenanceFailpoint::BeforeNativeStartupOpen,
    ));
    let error = storage
        .begin_published_restore_readiness(
            operation,
            inputs(),
            crate::RedbCommitProfile::Standard,
            Arc::new(NoChangelogPublicationPort),
        )
        .err()
        .unwrap();
    assert_eq!(error.kind(), StorageErrorKind::CommitStatusUnknown);
    assert_eq!(
        current(storage, operation).phase,
        PublicationPhase::StartupConsumed
    );
    let paths = [
        storage.database_file.clone(),
        crate::journal::journal_path(&storage.database_file),
        crate::durable_format_marker_path(&storage.database_file),
    ];
    for (path, seal) in paths.iter().zip(before.artifacts) {
        assert_eq!(
            crate::backup::sha256_file(path).unwrap(),
            seal.checksum,
            "durable consumption must precede every native startup write"
        );
    }
    storage.test_controller = None;
}

#[test]
fn native_publication_startup_fence_is_durable_before_any_native_write() {
    let scope = ScopedDirectory::new("native-startup-before-open");
    let root = scope.join("fixture");
    fs::create_dir(&root).unwrap();
    let (mut storage, operation) = published(&root);
    consume_without_open(&mut storage, operation);
    assert_eq!(
        read_receipt_file(&storage.receipt_path(operation))
            .unwrap()
            .current_phase(),
        OfflineMaintenanceReceiptPhaseV1::Validating
    );
}

#[test]
fn native_publication_startup_consumed_valid_native_writes_require_complete_current_validation() {
    use riffdb_storage_api::{
        EvidencePageLimit, StructuralEvidenceCursor, StructuralEvidencePage,
        StructuralEvidenceSession, StructuralOpenOutcome,
    };
    let scope = ScopedDirectory::new("native-startup-complete-current");
    let root = scope.join("fixture");
    fs::create_dir(&root).unwrap();
    let (mut storage, operation) = published(&root);
    consume_without_open(&mut storage, operation);
    let old = current(&storage, operation);
    {
        // A real native COW commit changes bytes without changing application
        // semantics. Once consumed, recovery must validate this actual target.
        let database = redb::Database::open(&storage.database_file).unwrap();
        let write = database.begin_write().unwrap();
        {
            let mut meta = write.open_table(crate::layout::META).unwrap();
            let same = meta
                .get(crate::layout::META_APPLICATION_SEQUENCE)
                .unwrap()
                .unwrap()
                .value()
                .to_vec();
            meta.insert(crate::layout::META_APPLICATION_SEQUENCE, same.as_slice())
                .unwrap();
        }
        write.commit().unwrap();
    }
    assert_ne!(
        crate::backup::sha256_file(&storage.database_file).unwrap(),
        old.artifacts[0].checksum
    );
    crate::startup::POPULATION_COUNTS.with(|count| count.set(0));
    let mut session = storage
        .begin_published_restore_readiness(
            operation,
            inputs(),
            crate::RedbCommitProfile::Standard,
            Arc::new(NoChangelogPublicationPort),
        )
        .unwrap();
    assert!(!session.clean_close_fast_path());
    assert!(!session.bounded_dirty_startup_path());
    assert!(!session.checkpoint_verified());
    assert!(crate::startup::POPULATION_COUNTS.with(|count| count.get()) > 0);
    let limit = EvidencePageLimit::new(64).unwrap();
    let mut cursor =
        StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
    let structural = loop {
        match session.read_structural_evidence(cursor, limit).unwrap() {
            StructuralEvidencePage::Page { findings, next, .. } => {
                assert!(findings.is_empty());
                cursor = next;
            }
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };
    let (catalog, historical) = session
        .validate_startup_catalog_history()
        .unwrap()
        .into_parts();
    assert!(matches!(
        catalog,
        riffdb_catalog::CatalogHistoryOutcome::Ready(_)
    ));
    assert!(matches!(
        session.finish(structural, historical).unwrap(),
        StructuralOpenOutcome::Clean(_)
    ));
    assert_eq!(
        current(&storage, operation).phase,
        PublicationPhase::StartupConsumed
    );
    assert_eq!(
        read_receipt_file(&storage.receipt_path(operation))
            .unwrap()
            .current_phase(),
        OfflineMaintenanceReceiptPhaseV1::Validating,
        "complete native evidence alone cannot complete a maintenance operation"
    );
}

#[test]
fn native_publication_startup_consumed_corruption_cannot_take_bounded_readiness() {
    use riffdb_storage_api::{
        EvidencePageLimit, StructuralEvidenceCursor, StructuralEvidencePage,
        StructuralEvidenceSession,
    };
    let scope = ScopedDirectory::new("native-startup-complete-corruption");
    let root = scope.join("fixture");
    fs::create_dir(&root).unwrap();
    let (mut storage, operation) = published(&root);
    consume_without_open(&mut storage, operation);
    {
        let database = redb::Database::open(&storage.database_file).unwrap();
        let write = database.begin_write().unwrap();
        write
            .open_table(crate::layout::ENTITIES)
            .unwrap()
            .insert(b"malformed-key".as_slice(), b"malformed-entity".as_slice())
            .unwrap();
        write.commit().unwrap();
    }
    let mut session = storage
        .begin_published_restore_readiness(
            operation,
            inputs(),
            crate::RedbCommitProfile::Standard,
            Arc::new(NoChangelogPublicationPort),
        )
        .unwrap();
    assert!(!session.clean_close_fast_path());
    assert!(!session.bounded_dirty_startup_path());
    assert!(!session.checkpoint_verified());
    let limit = EvidencePageLimit::new(64).unwrap();
    let mut cursor =
        StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
    let refused = loop {
        match session.read_structural_evidence(cursor, limit) {
            Err(error) => {
                assert_eq!(error.kind(), StorageErrorKind::CorruptData);
                break true;
            }
            Ok(StructuralEvidencePage::Page { findings, next, .. }) => {
                if !findings.is_empty() {
                    break true;
                }
                cursor = next;
            }
            Ok(StructuralEvidencePage::ExactEnd(_)) => break false,
        }
    };
    assert!(
        refused,
        "complete recovery must find post-consumption nonroot corruption before success"
    );
    assert_eq!(
        current(&storage, operation).phase,
        PublicationPhase::StartupConsumed
    );
    assert_eq!(
        read_receipt_file(&storage.receipt_path(operation))
            .unwrap()
            .current_phase(),
        OfflineMaintenanceReceiptPhaseV1::Validating
    );
}

const CONSUMED_CUTS: [RedbMaintenanceFailpoint; 6] = [
    RedbMaintenanceFailpoint::BeforeReceiptFileSync,
    RedbMaintenanceFailpoint::AfterReceiptFileSync,
    RedbMaintenanceFailpoint::BeforeReceiptRename,
    RedbMaintenanceFailpoint::AfterReceiptRename,
    RedbMaintenanceFailpoint::AfterReceiptParentSync,
    RedbMaintenanceFailpoint::BeforeNativeStartupOpen,
];

#[test]
fn native_publication_startup_process_child() {
    let Some(root) = std::env::var_os("RIFFDB_TEST_NATIVE_STARTUP_ROOT") else {
        return;
    };
    let root = Path::new(&root);
    let selection: usize = std::env::var("RIFFDB_TEST_NATIVE_STARTUP_CUT")
        .unwrap()
        .parse()
        .unwrap();
    assert!(selection <= CONSUMED_CUTS.len() + 1);
    let (mut storage, operation) =
        if std::env::var("RIFFDB_TEST_NATIVE_STARTUP_ORIGINAL").as_deref() == Ok("1") {
            let (storage, operation, _) = fresh_published(root);
            (storage, operation)
        } else {
            published(root)
        };
    if selection < CONSUMED_CUTS.len() {
        storage.test_controller = Some(RedbMaintenanceTestController::abort_at(
            CONSUMED_CUTS[selection],
        ));
    }
    let session = storage
        .begin_published_restore_readiness(
            operation,
            inputs(),
            crate::RedbCommitProfile::Standard,
            Arc::new(NoChangelogPublicationPort),
        )
        .unwrap();
    if selection == CONSUMED_CUTS.len() {
        // The engine is live and may have made its ordinary native startup
        // writes, while the ordinary maintenance receipt is still Validating.
        assert_eq!(
            current(&storage, operation).phase,
            PublicationPhase::StartupConsumed
        );
        assert_eq!(
            read_receipt_file(&storage.receipt_path(operation))
                .unwrap()
                .current_phase(),
            OfflineMaintenanceReceiptPhaseV1::Validating
        );
        let _retain_session = session;
        std::process::abort();
    }
    if selection == CONSUMED_CUTS.len() + 1 {
        let _retain_validated_engine = finish_native_session(session);
        // finish() durably consumes the dirty generation but the ordinary
        // receipt remains Validating. Keep the actual engine alive at abort.
        assert_eq!(
            read_receipt_file(&storage.receipt_path(operation))
                .unwrap()
                .current_phase(),
            OfflineMaintenanceReceiptPhaseV1::Validating
        );
        std::process::abort();
    }
    panic!("native startup child must abort at the selected boundary");
}

fn abort_startup_child(root: &Path, selection: usize) {
    abort_startup_child_with_original(root, selection, false);
}

fn abort_startup_child_with_original(root: &Path, selection: usize, original: bool) {
    use std::os::unix::process::ExitStatusExt;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};
    let mut child=Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "maintenance::store::restored_readiness::native_transfer_tests::native_publication_startup_process_child", "--nocapture"])
        .env("RIFFDB_TEST_NATIVE_STARTUP_ROOT", root)
        .env("RIFFDB_TEST_NATIVE_STARTUP_CUT", selection.to_string())
        .env("RIFFDB_TEST_NATIVE_STARTUP_ORIGINAL", if original { "1" } else { "0" })
        .stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::inherit()).spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("native startup child exceeded its deadline");
        }
        std::thread::yield_now();
    };
    assert_eq!(
        status.signal(),
        Some(6),
        "must observe the actual startup abort boundary"
    );
}

#[test]
fn native_publication_startup_process_cuts_preserve_the_preopen_seals_and_recover() {
    assert_startup_process_cuts(false);
}

#[test]
// req: END-001, END-008, REC-001, REC-004, REC-006, REC-008
fn retained_original_publication_startup_process_cuts_preserve_seals_and_cold_recovery() {
    assert_startup_process_cuts(true);
}

fn assert_startup_process_cuts(original: bool) {
    for selection in 0..CONSUMED_CUTS.len() {
        let scope = ScopedDirectory::new("native-startup-crash-consumption");
        let root = scope.join("fixture");
        fs::create_dir(&root).unwrap();
        abort_startup_child_with_original(&root, selection, original);
        let storage = RedbMaintenanceStorage::acquire_owner(
            &root.join("configured.redb"),
            &root.join("backups"),
            None,
        )
        .unwrap();
        let inventory = super::super::native_publication::IntentInventory::read(
            &storage.receipts_directory_guard,
        )
        .unwrap();
        let intent = inventory.current.unwrap();
        let operation = intent.operation_id;
        let was_consumed = intent.phase == PublicationPhase::StartupConsumed;
        assert_eq!(was_consumed, selection >= 3);
        let paths = [
            storage.database_file.clone(),
            crate::journal::journal_path(&storage.database_file),
            crate::durable_format_marker_path(&storage.database_file),
        ];
        for (path, seal) in paths.iter().zip(&intent.artifacts) {
            assert_eq!(crate::backup::sha256_file(path).unwrap(), seal.checksum);
        }
        let session = storage
            .begin_published_restore_readiness(
                operation,
                inputs(),
                crate::RedbCommitProfile::Standard,
                Arc::new(NoChangelogPublicationPort),
            )
            .unwrap();
        if was_consumed {
            assert!(!session.bounded_dirty_startup_path());
            assert!(!session.clean_close_fast_path());
            assert!(!session.checkpoint_verified());
        }
        assert_eq!(
            current(&storage, operation).phase,
            PublicationPhase::StartupConsumed
        );
        assert_eq!(
            read_receipt_file(&storage.receipt_path(operation))
                .unwrap()
                .current_phase(),
            OfflineMaintenanceReceiptPhaseV1::Validating
        );
        drop(session);
    }
}

#[test]
fn native_publication_startup_interrupted_engine_open_recovers_the_consumed_current_target() {
    assert_interrupted_engine_open(false);
}

#[test]
// req: END-001, END-008, REC-001, REC-004, REC-006, REC-008
fn retained_original_publication_interrupted_engine_open_recovers_the_consumed_current_target() {
    assert_interrupted_engine_open(true);
}

fn assert_interrupted_engine_open(original: bool) {
    let scope = ScopedDirectory::new("native-startup-crash-live-engine");
    let root = scope.join("fixture");
    fs::create_dir(&root).unwrap();
    abort_startup_child_with_original(&root, CONSUMED_CUTS.len(), original);
    let storage = RedbMaintenanceStorage::acquire_owner(
        &root.join("configured.redb"),
        &root.join("backups"),
        None,
    )
    .unwrap();
    let intent =
        super::super::native_publication::IntentInventory::read(&storage.receipts_directory_guard)
            .unwrap()
            .current
            .unwrap();
    assert_eq!(intent.phase, PublicationPhase::StartupConsumed);
    let session = storage.begin_published_restore_readiness(
        intent.operation_id,
        inputs(),
        crate::RedbCommitProfile::Standard,
        Arc::new(NoChangelogPublicationPort),
    );
    assert!(
        session.is_ok(),
        "a healthy native engine interrupted after consumption must recover the actual published target"
    );
    let session = session.unwrap();
    assert!(!session.bounded_dirty_startup_path());
    assert!(!session.clean_close_fast_path());
    assert!(!session.checkpoint_verified());
    drop(session);
}

fn finish_native_session(
    mut session: crate::RedbStructuralEvidenceSession,
) -> crate::RedbDormantPorts {
    use riffdb_storage_api::{
        EvidencePageLimit, StructuralEvidenceCursor, StructuralEvidencePage,
        StructuralEvidenceSession, StructuralOpenOutcome,
    };
    let limit = EvidencePageLimit::new(64).unwrap();
    let mut cursor =
        StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
    let structural = loop {
        match session.read_structural_evidence(cursor, limit).unwrap() {
            StructuralEvidencePage::Page { findings, next, .. } => {
                assert!(findings.is_empty());
                cursor = next;
            }
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };
    let (catalog, historical) = session
        .validate_startup_catalog_history()
        .unwrap()
        .into_parts();
    let riffdb_catalog::CatalogHistoryOutcome::Ready(catalog) = catalog else {
        panic!("native catalog must validate");
    };
    let StructuralOpenOutcome::Clean(opened) = session.finish(structural, historical).unwrap()
    else {
        panic!("native startup must complete");
    };
    let (database, open, _, dormant) = opened.into_parts();
    assert!(catalog.matches(database, open));
    dormant
}

#[test]
fn native_publication_startup_interrupted_dirty_generation_recovers_the_consumed_current_target() {
    use std::os::unix::fs::MetadataExt;
    let scope = ScopedDirectory::new("native-startup-crash-dirty-generation");
    let root = scope.join("fixture");
    fs::create_dir(&root).unwrap();
    abort_startup_child(&root, CONSUMED_CUTS.len() + 1);
    let (storage, reconciliation) =
        RedbMaintenanceStorage::open(root.join("configured.redb"), root.join("backups")).unwrap();
    assert!(
        reconciliation
            .operations()
            .iter()
            .any(|e| e.native_publication_target_owned())
    );
    let recovered = storage
        .prepare_native_publication_recovery()
        .unwrap()
        .unwrap();
    drop(recovered);
    let intent =
        super::super::native_publication::IntentInventory::read(&storage.receipts_directory_guard)
            .unwrap()
            .current
            .unwrap();
    assert_eq!(intent.phase, PublicationPhase::StartupConsumed);
    let metadata = fs::metadata(&storage.database_file).unwrap();
    let identity = (metadata.dev(), metadata.ino());
    assert_ne!(
        crate::backup::sha256_file(&storage.database_file).unwrap(),
        intent.artifacts[0].checksum
    );
    crate::reset_last_repair_progress_for_tests();
    let session = storage.begin_published_restore_readiness(
        intent.operation_id,
        inputs(),
        crate::RedbCommitProfile::Standard,
        Arc::new(NoChangelogPublicationPort),
    );
    let observed = fs::metadata(&storage.database_file).unwrap();
    assert_eq!(
        (observed.dev(), observed.ino()),
        identity,
        "consumed startup can never replace its actual target from backup"
    );
    assert!(
        session.is_ok(),
        "durable native startup writes interrupted before Succeeded must recover the current target; kind={:?}; repair_callback={}",
        session.as_ref().err().map(|error| error.kind()),
        crate::last_repair_progress_basis_points() != crate::REPAIR_PROGRESS_SENTINEL
    );
    let session = session.unwrap();
    assert!(!session.bounded_dirty_startup_path());
    assert!(!session.clean_close_fast_path());
    assert!(!session.checkpoint_verified());
    drop(finish_native_session(session));
    assert_eq!(
        current(&storage, intent.operation_id).phase,
        PublicationPhase::StartupConsumed
    );
    assert_eq!(
        read_receipt_file(&storage.receipt_path(intent.operation_id))
            .unwrap()
            .current_phase(),
        OfflineMaintenanceReceiptPhaseV1::Validating
    );
}

#[test]
fn native_publication_startup_diagnostic_ordinary_engine_recovery_validates_the_consumed_target() {
    use riffdb_storage_api::StructuralEvidenceSession;
    use std::os::unix::fs::MetadataExt;
    let scope = ScopedDirectory::new("native-startup-native-recovery-diagnostic");
    let root = scope.join("fixture");
    fs::create_dir(&root).unwrap();
    abort_startup_child(&root, CONSUMED_CUTS.len() + 1);
    let storage = RedbMaintenanceStorage::acquire_owner(
        &root.join("configured.redb"),
        &root.join("backups"),
        None,
    )
    .unwrap();
    let intent =
        super::super::native_publication::IntentInventory::read(&storage.receipts_directory_guard)
            .unwrap()
            .current
            .unwrap();
    assert_eq!(intent.phase, PublicationPhase::StartupConsumed);
    let before = fs::metadata(&storage.database_file).unwrap();
    let original_backup =
        validate_immutable_backup(&root.join("backups/ordinary-transfer")).unwrap();
    assert_eq!(original_backup.1, intent.manifest);
    // Diagnostic only: use the already existing ordinary engine recovery,
    // then the mandatory complete native validation. The native publication
    // API is independently qualified by the actual dirty-generation regression.
    crate::reset_last_repair_progress_for_tests();
    let store = crate::RedbStore::open_with_changelog_publication_port(
        &storage.database_file,
        crate::RedbCommitProfile::Standard,
        Arc::new(NoChangelogPublicationPort),
    )
    .unwrap();
    assert_ne!(
        crate::last_repair_progress_basis_points(),
        crate::REPAIR_PROGRESS_SENTINEL
    );
    let session = store
        .begin_native_published_restore_recovery(inputs())
        .unwrap();
    assert_eq!(session.database_id(), intent.manifest.database_id());
    assert!(!session.bounded_dirty_startup_path());
    assert!(!session.clean_close_fast_path());
    assert!(!session.checkpoint_verified());
    let current_metadata = fs::metadata(&storage.database_file).unwrap();
    assert_eq!(
        (current_metadata.dev(), current_metadata.ino()),
        (before.dev(), before.ino())
    );
    drop(finish_native_session(session));
    let store = crate::RedbStore::open_private_stage_authorization(
        &storage.database_file,
        crate::RedbCommitProfile::Standard,
        Arc::new(NoChangelogPublicationPort),
    )
    .unwrap();
    let read = store.shared.database.begin_read().unwrap();
    let retained = crate::startup::read_retained_metadata_pub(&read).unwrap();
    assert_eq!(retained.database_id(), intent.manifest.database_id());
    assert_eq!(retained.history_incarnation(), intent.incarnation);
    assert_eq!(
        crate::retention::durable_application_head(&read).unwrap(),
        intent
            .manifest
            .included_application_frontier()
            .map_or(0, |sequence| sequence.get())
    );
    drop(read);
    drop(store);
    assert_eq!(
        validate_immutable_backup(&root.join("backups/ordinary-transfer")).unwrap(),
        original_backup
    );
    assert_eq!(
        current(&storage, intent.operation_id).phase,
        PublicationPhase::StartupConsumed
    );
    assert_eq!(
        read_receipt_file(&storage.receipt_path(intent.operation_id))
            .unwrap()
            .current_phase(),
        OfflineMaintenanceReceiptPhaseV1::Validating
    );
}

#[test]
fn native_publication_success_restart_cleans_intent_without_republishing_the_backup() {
    let scope = ScopedDirectory::new("native-success-cleanup-restart");
    let root = scope.join("fixture");
    fs::create_dir(&root).unwrap();
    let (mut storage, operation) = published(&root);
    let session = storage
        .begin_published_restore_readiness(
            operation,
            inputs(),
            crate::RedbCommitProfile::Standard,
            Arc::new(NoChangelogPublicationPort),
        )
        .unwrap();
    let validated = finish_native_session(session);
    let mut receipt = read_receipt_file(&storage.receipt_path(operation)).unwrap();
    receipt
        .advance(OfflineMaintenanceReceiptTransitionV1::phase(
            OfflineMaintenanceReceiptPhaseV1::Succeeded,
        ))
        .unwrap();
    storage.replace_receipt(&receipt).unwrap();
    drop(validated);
    let paths = [
        storage.database_file.clone(),
        crate::journal::journal_path(&storage.database_file),
        crate::durable_format_marker_path(&storage.database_file),
    ];
    let after: Vec<_> = paths
        .iter()
        .map(|p| crate::backup::sha256_file(p).unwrap())
        .collect();
    let intent_path = storage
        .receipts_directory
        .join(format!("{operation}.publication-intent-v1"));
    assert!(intent_path.exists());
    storage.reconcile().expect(
        "a completed consumed restore must finish intent cleanup without backup republication",
    );
    assert!(!intent_path.exists());
    for (path, checksum) in paths.iter().zip(after) {
        assert_eq!(crate::backup::sha256_file(path).unwrap(), checksum);
    }
}

const CLEANUP_CUTS: [RedbMaintenanceFailpoint; 3] = [
    RedbMaintenanceFailpoint::BeforeNativeIntentRemoval,
    RedbMaintenanceFailpoint::AfterNativeIntentRemoval,
    RedbMaintenanceFailpoint::AfterNativeIntentDirectorySync,
];

fn successful_live(
    root: &Path,
) -> (
    RedbMaintenanceStorage,
    OfflineMaintenanceOperationId,
    crate::RedbDormantPorts,
) {
    let (mut storage, operation) = published(root);
    let session = storage
        .begin_published_restore_readiness(
            operation,
            inputs(),
            crate::RedbCommitProfile::Standard,
            Arc::new(NoChangelogPublicationPort),
        )
        .unwrap();
    let validated = finish_native_session(session);
    let mut receipt = read_receipt_file(&storage.receipt_path(operation)).unwrap();
    receipt
        .advance(OfflineMaintenanceReceiptTransitionV1::phase(
            OfflineMaintenanceReceiptPhaseV1::Succeeded,
        ))
        .unwrap();
    storage.replace_receipt(&receipt).unwrap();
    (storage, operation, validated)
}

#[test]
fn native_publication_success_cleanup_uncertainty_retries_without_touching_live_artifacts() {
    for (index, cut) in CLEANUP_CUTS.into_iter().enumerate() {
        let scope = ScopedDirectory::new("native-success-cleanup-return");
        let root = scope.join("fixture");
        fs::create_dir(&root).unwrap();
        let (mut storage, operation, validated) = successful_live(&root);
        let paths = [
            storage.database_file.clone(),
            crate::journal::journal_path(&storage.database_file),
            crate::durable_format_marker_path(&storage.database_file),
        ];
        let before: Vec<_> = paths
            .iter()
            .map(|p| crate::backup::sha256_file(p).unwrap())
            .collect();
        storage.test_controller = Some(RedbMaintenanceTestController::return_at(cut));
        let error = storage
            .complete_native_publication_cleanup(operation)
            .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::CommitStatusUnknown);
        let intent_path = storage
            .receipts_directory
            .join(format!("{operation}.publication-intent-v1"));
        assert_eq!(intent_path.exists(), index == 0);
        storage.test_controller = None;
        storage
            .complete_native_publication_cleanup(operation)
            .unwrap();
        storage
            .complete_native_publication_cleanup(operation)
            .unwrap();
        assert!(!intent_path.exists());
        for (path, checksum) in paths.iter().zip(before) {
            assert_eq!(crate::backup::sha256_file(path).unwrap(), checksum);
        }
        assert_eq!(
            read_receipt_file(&storage.receipt_path(operation))
                .unwrap()
                .current_phase(),
            OfflineMaintenanceReceiptPhaseV1::Succeeded
        );
        drop(validated);
    }
}

#[test]
fn native_publication_success_cleanup_refuses_nonterminal_or_foreign_inventory() {
    let scope = ScopedDirectory::new("native-success-cleanup-refusal");
    let root = scope.join("fixture");
    fs::create_dir(&root).unwrap();
    let (mut storage, operation) = published(&root);
    consume_without_open(&mut storage, operation);
    let intent_path = storage
        .receipts_directory
        .join(format!("{operation}.publication-intent-v1"));
    let intent_bytes = fs::read(&intent_path).unwrap();
    assert!(
        storage
            .complete_native_publication_cleanup(operation)
            .is_err()
    );
    assert_eq!(fs::read(&intent_path).unwrap(), intent_bytes);
    drop(storage);

    let scope = ScopedDirectory::new("native-success-cleanup-inventory");
    let root = scope.join("fixture");
    fs::create_dir(&root).unwrap();
    let (storage, operation, validated) = successful_live(&root);
    let intent_path = storage
        .receipts_directory
        .join(format!("{operation}.publication-intent-v1"));
    let intent_bytes = fs::read(&intent_path).unwrap();
    for name in ["foreign-record", ".invalid.receipt-v1.tmp"] {
        let foreign = storage.receipts_directory.join(name);
        fs::write(&foreign, b"preserved foreign evidence").unwrap();
        assert!(
            storage
                .complete_native_publication_cleanup(operation)
                .is_err()
        );
        assert_eq!(fs::read(&intent_path).unwrap(), intent_bytes);
        assert_eq!(fs::read(&foreign).unwrap(), b"preserved foreign evidence");
        fs::remove_file(foreign).unwrap();
    }
    let other =
        OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1234, [0x71; 10]).unwrap();
    assert!(storage.complete_native_publication_cleanup(other).is_err());
    assert_eq!(fs::read(&intent_path).unwrap(), intent_bytes);
    storage
        .complete_native_publication_cleanup(operation)
        .unwrap();
    drop(validated);
}

#[path = "publication_cleanup_tests.rs"]
mod cleanup_tests;

#[test]
fn native_publication_promotion_discovery_cannot_open_or_change_the_unconsumed_target() {
    let scope = ScopedDirectory::new("native-startup-promotion-probe");
    let root = scope.join("fixture");
    fs::create_dir(&root).unwrap();
    let (storage, operation) = published(&root);
    let intent = current(&storage, operation);
    assert_eq!(intent.phase, PublicationPhase::Published);
    assert!(storage.discover_committed_promotion().unwrap().is_none());
    assert_eq!(
        current(&storage, operation).encode().unwrap(),
        intent.encode().unwrap()
    );
    for (path, seal) in [
        storage.database_file.clone(),
        crate::journal::journal_path(&storage.database_file),
        crate::durable_format_marker_path(&storage.database_file),
    ]
    .iter()
    .zip(&intent.artifacts)
    {
        assert_eq!(
            crate::backup::sha256_file(path).unwrap(),
            seal.checksum,
            "optional promotion discovery cannot perform a native open before durable consumption"
        );
    }
    let session = storage
        .begin_published_restore_readiness(
            operation,
            inputs(),
            crate::RedbCommitProfile::Standard,
            Arc::new(NoChangelogPublicationPort),
        )
        .unwrap();
    assert_eq!(
        current(&storage, operation).phase,
        PublicationPhase::StartupConsumed
    );
    drop(finish_native_session(session));
}

#[cfg(unix)]
#[test]
// req: REC-001, REC-004, REC-006, REC-008, END-001, END-008
fn ordinary_materialization_verifies_original_backup_once_before_first_use() {
    let scope = ScopedDirectory::new("materialization-one-backup-content-pass");
    let root = scope.join("fixture");
    fs::create_dir(&root).unwrap();
    fresh_published_observing(
        &root,
        Some(|passes| {
            assert_eq!(
                passes, 1,
                "first use must fully validate the owned backup without a preceding duplicate path hash"
            )
        }),
        None,
    );
}

#[cfg(unix)]
#[test]
// req: REC-001, REC-004, REC-006, REC-008, END-001, END-008
fn ordinary_publication_revalidates_original_backup_once_at_the_full_boundary() {
    let scope = ScopedDirectory::new("publication-one-backup-content-pass");
    let root = scope.join("fixture");
    fs::create_dir(&root).unwrap();
    fresh_published_observing(
        &root,
        None,
        Some(|passes| {
            assert_eq!(
                passes, 1,
                "publication must revalidate every original artifact once through its retained owner"
            )
        }),
    );
}

#[cfg(unix)]
#[test]
// req: REC-001, REC-004, REC-006, REC-008, END-001, END-008
fn ordinary_publication_without_a_retained_backup_keeps_full_content_validation() {
    use crate::maintenance::staged::transfer_tests::native_transfer_fixture;
    let scope = ScopedDirectory::new("publication-fallback-full-original-content");
    let root = scope.join("fixture");
    fs::create_dir(&root).unwrap();
    let fixture = native_transfer_fixture(&root);
    let before = crate::backup::BACKUP_CONTENT_PASSES.with(std::cell::Cell::get);
    fixture
        .storage
        .publish_sealed_restore(
            fixture.sealed,
            OfflineRestoreOverwritePolicyV1::RefuseNonEmpty,
        )
        .unwrap();
    assert_eq!(
        crate::backup::BACKUP_CONTENT_PASSES.with(std::cell::Cell::get) - before,
        1,
        "absence of the actual retained owner requires the original complete fallback check"
    );
}

#[cfg(unix)]
#[test]
// req: REC-001, REC-004, REC-006, REC-008, END-001, END-008
fn ordinary_publication_refuses_changed_original_artifacts_before_durable_intent() {
    use crate::maintenance::staged::transfer_tests::native_transfer_fixture_with_prepared_source;
    use std::os::unix::fs::FileExt;
    for artifact in 0..5 {
        let scope = ScopedDirectory::new("publication-changed-original-content");
        let root = scope.join("fixture");
        fs::create_dir(&root).unwrap();
        let fixture = native_transfer_fixture_with_prepared_source(
            &root,
            true,
            prepare_native_transfer_source,
            None,
        );
        let mut originals: Vec<_> = fs::read_dir(&fixture.named)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        originals.sort();
        assert_eq!(
            originals.len(),
            5,
            "exercise primary, active journal, marker, sealed extent and manifest"
        );
        let original = &originals[artifact];
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(original)
            .unwrap();
        let modified = file.metadata().unwrap().modified().unwrap();
        let mut byte = [0];
        assert_eq!(file.read_at(&mut byte, 0).unwrap(), 1);
        file.write_all_at(&[byte[0] ^ 1], 0).unwrap();
        file.set_modified(modified).unwrap();
        file.sync_all().unwrap();
        drop(file);
        let error = fixture
            .storage
            .publish_sealed_restore(
                fixture.sealed,
                OfflineRestoreOverwritePolicyV1::RefuseNonEmpty,
            )
            .unwrap_err();
        assert_eq!(
            error.kind(),
            StorageErrorKind::CorruptData,
            "changed original artifact {artifact}"
        );
        assert!(
            original.is_file(),
            "failure preserves the original backup evidence"
        );
        assert!(!fixture.configured.exists());
        assert!(!crate::journal::journal_path(&fixture.configured).exists());
        assert!(!crate::durable_format_marker_path(&fixture.configured).exists());
        assert!(
            !root
                .join("backups/.maintenance/receipts")
                .join(format!("{}.publication-intent-v1", fixture.operation))
                .exists()
        );
        assert_eq!(
            read_receipt_file(&fixture.storage.receipt_path(fixture.operation))
                .unwrap()
                .current_phase(),
            OfflineMaintenanceReceiptPhaseV1::Offline
        );
    }
}
