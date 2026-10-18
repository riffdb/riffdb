use crate::layout::{META, META_CLEAN_CLOSE_LIFECYCLE};
use crate::media::{
    JournalMedia, JournalMediaDirectory, MediaFile, MediaFileMetadata, RealJournalMedia,
};
use crate::startup::open_cleanly_for_test as open_cleanly;
use crate::store::{
    read_administration_tail, read_commit_tail, read_identity_from_read_transaction,
};
use crate::{RedbOperationalPorts, RedbStore};
use redb::ReadableDatabase;
use riffdb_storage_api::DatabaseInitializationPort;
use riffdb_types::DatabaseId;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
struct SpareCreationMedia {
    preparing: PathBuf,
    creates: AtomicU64,
}

impl JournalMedia for SpareCreationMedia {
    fn open_directory(&self, path: &Path) -> io::Result<Box<dyn JournalMediaDirectory>> {
        RealJournalMedia.open_directory(path)
    }
    fn try_exists(&self, path: &Path) -> io::Result<bool> {
        RealJournalMedia.try_exists(path)
    }
    fn metadata(&self, path: &Path) -> io::Result<MediaFileMetadata> {
        RealJournalMedia.metadata(path)
    }
    fn open_read(&self, path: &Path) -> io::Result<MediaFile> {
        RealJournalMedia.open_read(path)
    }
    fn open_read_write(&self, path: &Path) -> io::Result<MediaFile> {
        RealJournalMedia.open_read_write(path)
    }
    fn create_new_read_write(&self, path: &Path) -> io::Result<MediaFile> {
        if path == self.preparing {
            self.creates.fetch_add(1, Ordering::Relaxed);
        }
        RealJournalMedia.create_new_read_write(path)
    }
    fn create_new_write_only(&self, path: &Path) -> io::Result<MediaFile> {
        RealJournalMedia.create_new_write_only(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        RealJournalMedia.rename(from, to)
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        RealJournalMedia.remove_file(path)
    }
    fn sync_parent_data(&self, path: &Path) -> io::Result<()> {
        RealJournalMedia.sync_parent_data(path)
    }
    fn sync_parent_all(&self, path: &Path) -> io::Result<()> {
        RealJournalMedia.sync_parent_all(path)
    }
}

fn fixture(
    label: &str,
) -> (
    crate::test_path::ScopedDirectory,
    RedbOperationalPorts,
    Arc<SpareCreationMedia>,
) {
    let scope = crate::test_path::ScopedDirectory::new(label);
    let path = scope.join("db.redb");
    let mut store = RedbStore::open(&path).unwrap();
    let database_id =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x74; 10]).unwrap();
    store.initialize_database(database_id).unwrap();
    let media = Arc::new(SpareCreationMedia {
        preparing: crate::journal::preparing_spare_journal_path(&path),
        creates: AtomicU64::new(0),
    });
    Arc::get_mut(&mut store.shared).unwrap().journal_media = media.clone();
    let ports = open_cleanly(store);
    (scope, ports, media)
}

fn assert_clean_without_spare(ports: &RedbOperationalPorts) {
    let before = ports.shared.database.begin_read().unwrap();
    let application = read_commit_tail(&before).unwrap();
    let administration = read_administration_tail(&before).unwrap();
    let identity = read_identity_from_read_transaction(&before).unwrap();
    drop(before);
    let receipt = ports.complete_graceful_close();
    assert_eq!(
        receipt.lifecycle(),
        crate::GracefulLifecycleOutcomeV1::CleanCommitted
    );
    assert!(!crate::journal::spare_journal_path(&ports.shared.path).exists());
    assert!(!crate::journal::preparing_spare_journal_path(&ports.shared.path).exists());
    let after = ports.shared.database.begin_read().unwrap();
    assert_eq!(read_commit_tail(&after).unwrap(), application);
    assert_eq!(read_administration_tail(&after).unwrap(), administration);
    crate::journal::verify_clean_close_header_digest_with_media(
        ports.shared.journal_media.as_ref(),
        &ports.shared.path,
        identity,
        application,
        administration,
    )
    .unwrap();
    let meta = after.open_table(META).unwrap();
    let value = meta.get(META_CLEAN_CLOSE_LIFECYCLE).unwrap().unwrap();
    assert!(matches!(
        crate::clean_close::CleanCloseLifecycle::decode(value.value())
            .unwrap()
            .state(),
        crate::clean_close::CleanCloseState::Clean(_)
    ));
}

// req: REC-001, REC-002, REC-004, STO-032, PERF-019
#[test]
fn graceful_close_does_not_prepare_a_spare_it_immediately_deletes() {
    let (_scope, ports, media) = fixture("graceful-no-unused-spare");
    assert_clean_without_spare(&ports);
    assert_eq!(
        media.creates.load(Ordering::Relaxed),
        0,
        "closing must durably establish its active extent without creating future writer scratch"
    );
}

// req: REC-001, REC-002, REC-004, STO-032, PERF-019
#[test]
fn ordinary_journal_runtime_still_prepares_its_spare_before_close() {
    let (_scope, ports, media) = fixture("writer-retains-spare-preparation");
    // Ordinary writers install their durable root before opening the lane.
    let root = ports.shared.capture_checkpoint_root().unwrap();
    *ports.shared.durable_read_frontier.write().unwrap() = Some(root);
    drop(ports.shared.journal_runtime().unwrap());
    ports.shared.join_journal_sealer();
    assert_eq!(media.creates.load(Ordering::Relaxed), 1);
    assert!(crate::journal::spare_journal_path(&ports.shared.path).is_file());
    assert_clean_without_spare(&ports);
    assert_eq!(media.creates.load(Ordering::Relaxed), 1);
}

// req: REC-001, REC-002, REC-004, STO-032, PERF-019
#[test]
fn failed_close_leaves_later_writer_spare_preparation_intact() {
    let (_scope, mut ports, media) = fixture("failed-close-writer-spare");
    Arc::get_mut(&mut ports.shared).unwrap().test_controller =
        Some(crate::RedbTestController::return_before_commit(
            crate::RedbTestOperation::GracefulCheckpointClassification,
        ));
    let receipt = ports.complete_graceful_close();
    assert_eq!(
        receipt.disposition(),
        crate::GracefulCheckpointDispositionV1::ClassificationFailed
    );
    assert_eq!(
        receipt.lifecycle(),
        crate::GracefulLifecycleOutcomeV1::CleanNotAttempted
    );
    assert_eq!(media.creates.load(Ordering::Relaxed), 0);
    assert!(ports.shared.journal_runtime.lock().unwrap().is_none());
    let read = ports.shared.database.begin_read().unwrap();
    let meta = read.open_table(META).unwrap();
    let value = meta.get(META_CLEAN_CLOSE_LIFECYCLE).unwrap().unwrap();
    assert_eq!(
        crate::clean_close::CleanCloseLifecycle::decode(value.value())
            .unwrap()
            .state(),
        crate::clean_close::CleanCloseState::Dirty
    );
    drop(value);
    drop(meta);
    drop(read);
    drop(ports.shared.journal_runtime().unwrap());
    ports.shared.join_journal_sealer();
    assert_eq!(media.creates.load(Ordering::Relaxed), 1);
    assert!(crate::journal::spare_journal_path(&ports.shared.path).is_file());
}

fn compact_audit_intent(ordinal: u64) -> riffdb_storage_api::ServiceAuditAppendIntentV1 {
    use riffdb_storage_api::{AuditPrincipalV1, ServiceAuditAppendIntentV1};
    use riffdb_types::{
        ActorId, ActorKind, CapabilityId, RequestId, ServiceAuditLinkV1, ServiceAuditPhaseV1,
        ServiceAuditTargetsV1, ServiceIngressKindV1, ServiceOperationV1, Timestamp,
    };
    ServiceAuditAppendIntentV1::new(
        RequestId::from_unix_milliseconds_and_random(100 + ordinal, [0x61; 10]).unwrap(),
        Timestamp::new(1_700_000_000, 0).unwrap(),
        ServiceOperationV1::GetHealth,
        ServiceAuditPhaseV1::Denied,
        AuditPrincipalV1::new(
            ActorId::new("compact-extent-proof").unwrap(),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(100, [0x62; 10]).unwrap(),
            std::num::NonZeroU64::MIN,
        ),
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::None,
    )
    .unwrap()
}

// req: STO-001, STO-032, REC-001, REC-002, REC-004, PERF-019
#[test]
fn compact_extent_profile_rotates_recovers_and_transitions_capacity_without_losing_history_stubs() {
    use riffdb_storage_api::{
        NoChangelogPublicationPort, ServiceAuditAppendRepository, ServiceAuditGroupAppend,
    };

    let scope = crate::test_path::ScopedDirectory::new("compact-extent-capacity-transition");
    let path = scope.join("db.redb");
    let database_id =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x75; 10]).unwrap();

    // 1. Initialize in Compact profile; verify the active extent is 8 MiB + header.
    let mut store = RedbStore::open_with_extent_profile(
        &path,
        crate::RedbCommitProfile::Standard,
        crate::JournalExtentProfile::Compact,
        Arc::new(NoChangelogPublicationPort),
    )
    .unwrap();
    assert_eq!(store.extent_profile(), crate::JournalExtentProfile::Compact);
    store.initialize_database(database_id).unwrap();
    let mut ports = open_cleanly(store);

    let ServiceAuditGroupAppend::Submitted(fence) = ports
        .submit_service_audit_group(&[compact_audit_intent(1)])
        .unwrap()
    else {
        panic!("expected journaled service audit fence");
    };
    assert_eq!(fence.wait().unwrap().len(), 1);

    let active_path = crate::journal::journal_path(&path);
    assert_eq!(
        std::fs::metadata(&active_path).unwrap().len(),
        u64::try_from(crate::journal::extent_file_bytes(
            crate::COMPACT_EXTENT_DATA_BYTES
        ))
        .unwrap(),
        "compact profile active extent must be 8 MiB + 8 KiB header"
    );

    // Materialize the first audit into the active extent (advancing anchor_position > 8 KiB)
    // and close cleanly so its ADMINISTRATION_HISTORY row holds a StubLocator into generation 1.
    let close_receipt = ports.complete_graceful_close();
    assert_eq!(
        close_receipt.lifecycle(),
        crate::GracefulLifecycleOutcomeV1::CleanCommitted
    );
    drop(ports);

    let census_before = crate::fixtures::history_stub_census_fixture(&path).unwrap();
    assert_eq!(
        census_before.history.0, 1,
        "materialized compact frame must be represented by a history stub"
    );

    // 2. Reopen in Standard profile: generation 1 has materialized history before its anchor,
    // so recovery must seal generation 1 into .riffjournal.sealed.1 and create generation 2
    // at Standard capacity (EXTENT_DATA_BYTES) without losing history stub resolution.
    let store_std = RedbStore::open_with_extent_profile(
        &path,
        crate::RedbCommitProfile::Standard,
        crate::JournalExtentProfile::Standard,
        Arc::new(NoChangelogPublicationPort),
    )
    .unwrap();
    assert_eq!(
        store_std.extent_profile(),
        crate::JournalExtentProfile::Standard
    );
    assert!(
        crate::journal::sealed_journal_path(&path, 1).is_file(),
        "retiring a compact extent with materialized history must seal generation 1"
    );
    assert_eq!(
        crate::journal::extent_generation_with_media(&RealJournalMedia, &active_path).unwrap(),
        Some(2),
        "normalized active extent must advance to generation 2"
    );
    assert_eq!(
        std::fs::metadata(&active_path).unwrap().len(),
        u64::try_from(crate::journal::extent_file_bytes(
            crate::journal::EXTENT_DATA_BYTES
        ))
        .unwrap(),
        "active extent after transition to Standard must match EXTENT_DATA_BYTES"
    );
    let ports_std = open_cleanly(store_std);
    crate::changelog_v3_roots::validate_retained_history(
        &ports_std.shared.database.begin_read().unwrap(),
    )
    .unwrap()
    .unwrap();
    drop(ports_std);

    // 3. Reopen back in Compact profile while generation 2 is still empty (anchor at 8 KiB):
    // recovery must normalize generation 2 in place to generation 3 at Compact capacity (8 MiB),
    // while generation 1's sealed history stub remains readable.
    let store_compact = RedbStore::open_with_extent_profile(
        &path,
        crate::RedbCommitProfile::Standard,
        crate::JournalExtentProfile::Compact,
        Arc::new(NoChangelogPublicationPort),
    )
    .unwrap();
    assert_eq!(
        crate::journal::extent_generation_with_media(&RealJournalMedia, &active_path).unwrap(),
        Some(3),
        "empty active extent must normalize in place to generation 3"
    );
    assert_eq!(
        std::fs::metadata(&active_path).unwrap().len(),
        u64::try_from(crate::journal::extent_file_bytes(
            crate::COMPACT_EXTENT_DATA_BYTES
        ))
        .unwrap(),
        "active extent after transition back to Compact must be 8 MiB + 8 KiB header"
    );
    let ports_compact = open_cleanly(store_compact);
    crate::changelog_v3_roots::validate_retained_history(
        &ports_compact.shared.database.begin_read().unwrap(),
    )
    .unwrap()
    .unwrap();
}

// req: STO-001, STO-032, REC-001, REC-002, REC-004, PERF-019
#[test]
fn compact_extent_profile_defers_spare_until_utilization_threshold_or_checkpoint_rotation() {
    use riffdb_storage_api::{
        NoChangelogPublicationPort, ServiceAuditAppendRepository, ServiceAuditGroupAppend,
    };

    let scope = crate::test_path::ScopedDirectory::new("compact-lazy-spare");
    let path = scope.join("db.redb");
    let database_id =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x76; 10]).unwrap();
    let mut store = RedbStore::open_with_extent_profile(
        &path,
        crate::RedbCommitProfile::Standard,
        crate::JournalExtentProfile::Compact,
        Arc::new(NoChangelogPublicationPort),
    )
    .unwrap();
    store.initialize_database(database_id).unwrap();
    let media = Arc::new(SpareCreationMedia {
        preparing: crate::journal::preparing_spare_journal_path(&path),
        creates: AtomicU64::new(0),
    });
    Arc::get_mut(&mut store.shared).unwrap().journal_media = media.clone();
    let mut ports = open_cleanly(store);

    // 1. Append a small frame in Compact mode; verify .riffjournal.spare is NOT created eagerly.
    let ServiceAuditGroupAppend::Submitted(fence) = ports
        .submit_service_audit_group(&[compact_audit_intent(1)])
        .unwrap()
    else {
        panic!("expected journaled service audit fence");
    };
    assert_eq!(fence.wait().unwrap().len(), 1);
    ports.shared.join_journal_sealer();

    let spare_path = crate::journal::spare_journal_path(&path);
    assert!(
        !spare_path.exists(),
        "compact profile must not eagerly allocate .riffjournal.spare below the 25% threshold"
    );
    assert_eq!(
        media.creates.load(Ordering::Relaxed),
        0,
        "no background spare preparation should occur for a small initial suffix"
    );

    // 2. Trigger an early checkpoint rotation before .riffjournal.spare exists (via a source
    // checkpoint request + ordinary writer); verify rotation succeeds by creating an 8 MiB
    // active successor on demand and sealing generation 1.
    ports
        .shared
        .mutation_gate
        .note_source_publication_request()
        .unwrap();
    let ordinary = ports.begin_deferred_service_audit_write().unwrap();
    ordinary.abort().unwrap();
    {
        let writer = ports.shared.mutation_gate.acquire().unwrap();
        ports.shared.poll_async_checkpoint_locked(true).unwrap();
        drop(writer);
    }
    ports.shared.join_journal_sealer();

    let active_path = crate::journal::journal_path(&path);
    assert_eq!(
        crate::journal::extent_generation_with_media(&RealJournalMedia, &active_path).unwrap(),
        Some(2),
        "on-demand spare allocation must rotate active extent to generation 2"
    );
    assert!(
        crate::journal::sealed_journal_path(&path, 1).is_file(),
        "generation 1 must be sealed after checkpoint rotation"
    );
    assert!(
        !spare_path.exists(),
        "post-checkpoint sealer in compact profile must also defer spare when new suffix is empty"
    );

    // 3. Cross the compact utilization threshold and verify background spare preparation
    // allocates an 8 MiB .riffjournal.spare off the writer path.
    {
        let mut runtime = ports.shared.journal_runtime.lock().unwrap();
        runtime.as_mut().unwrap().suffix_physical_bytes = crate::COMPACT_LAZY_SPARE_THRESHOLD_BYTES;
    }
    ports.shared.maybe_start_async_checkpoint().unwrap();
    ports.shared.join_journal_sealer();
    assert!(
        spare_path.is_file(),
        "crossing COMPACT_LAZY_SPARE_THRESHOLD_BYTES must prepare .riffjournal.spare"
    );
    assert_eq!(
        std::fs::metadata(&spare_path).unwrap().len(),
        u64::try_from(crate::journal::extent_file_bytes(
            crate::COMPACT_EXTENT_DATA_BYTES
        ))
        .unwrap(),
        "prepared spare in compact profile must be 8 MiB + 8 KiB header"
    );
    assert_eq!(media.creates.load(Ordering::Relaxed), 1);
}
