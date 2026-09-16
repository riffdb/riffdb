//! Real source pins and the external archive owner, with explicit sink barriers.
// req: REP-007, AFC-007
use super::*;
use crate::replication_publication::ReplicationPublication;
use riffdb_storage_api::{
    ChangelogPublicationPort, OfflineBackupPersistencePort, ReadableCapabilityDigestInventory,
    ReadableDigestKey, ReadableIdempotencyDigestInventory, StartupValidationInputs,
};
use riffdb_types::{DigestKeyId, Timestamp};

struct Fixture {
    root: tempfile::TempDir,
    archive: ConfiguredArchive,
    lineage: ChangelogLineageV3,
    before: ChangelogHistoryPointV3,
    head: ChangelogHistoryPointV3,
    pin: Arc<dyn PublishedDurableSnapshot>,
    _ports: riffdb_storage_redb::RedbOperationalPorts,
}
fn fixture() -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("database.redb");
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
    let inputs = StartupValidationInputs::new(
        Timestamp::new(1000, 0).unwrap(),
        ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
        ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
    );
    let ids = crate::identifiers::ProductionIdentifierSources::new();
    let startup =
        crate::startup::open_redb_startup(&database, inputs.clone(), &ids.database_ids()).unwrap();
    drop(startup);
    std::fs::create_dir(root.path().join("backups")).unwrap();
    let backup_path = root.path().join("backups/baseline");
    riffdb_storage_redb::RedbOfflineBackup::bind(&database, &backup_path)
        .create_offline_backup(
            &crate::maintenance_driver::contract_migration_backup_build_metadata().unwrap(),
        )
        .unwrap();
    let backup = RedbVerifiedArchiveBackup::open(&backup_path).unwrap();
    let history = backup.history();
    drop(backup);
    let startup =
        crate::startup::open_redb_startup(&database, inputs, &ids.database_ids()).unwrap();
    let (_, _, _, _, _, ports) = startup.into_parts();
    let pin = ports.published_changelog_snapshot_v3().unwrap();
    let head = pin.authoritative_state_v3().unwrap().history().tail();
    assert!(
        head.sequence() > history.tail().sequence(),
        "reopen must emit real administrative history"
    );
    let config = root.path().join("config.toml");
    std::fs::write(&config, format!("[server]\ndatabase = '{}'\n[maintenance]\nbackup_root = '{}'\n[[maintenance.archives]]\nname = 'daily'\npath = '{}'\nencryption = 'unencrypted'\nbackup = 'baseline'\n", database.display(), root.path().join("backups").display(), root.path().join("archive").display())).unwrap();
    let config =
        crate::config::ServerConfig::parse(["--config".into(), config.into_os_string()]).unwrap();
    Fixture {
        root,
        archive: config.databases()[0].archives()[0].clone(),
        lineage: history.lineage(),
        before: history.tail(),
        head,
        pin,
        _ports: ports,
    }
}
async fn caught_up(worker: &mut Worker, head: ChangelogHistoryPointV3) {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let status = *worker.status.borrow_and_update();
            assert!(status.failure.is_none(), "{status:?}");
            if status.caught_up && status.position == Some(head) {
                break;
            }
            worker.status.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_worker_collects_real_published_history_and_restarts_at_durable_position() {
    let fixture = fixture();
    let (publisher, publications) = ReplicationPublication::channel();
    publisher.observe_published_snapshot_v3(fixture.pin.clone());
    let mut digest = None;
    for _ in 0..2 {
        let mut worker = Worker::start(
            fixture.archive.clone(),
            fixture.root.path().join("backups"),
            publications.clone(),
        );
        caught_up(&mut worker, fixture.head).await;
        worker.join();
        let backup =
            RedbVerifiedArchiveBackup::open(&fixture.root.path().join("backups/baseline")).unwrap();
        let archive = backup
            .open_archive(fixture.archive.path(), fixture.archive.encryption())
            .unwrap();
        assert_eq!(archive.position(), fixture.head);
        let observed = archive.head().unwrap().digest();
        if let Some(digest) = digest {
            assert_eq!(observed, digest);
        }
        digest = Some(observed);
        assert!(archive.frames().count() > 0);
    }
}

struct UncertainSink {
    attempts: Vec<Vec<u8>>,
}
impl ArchiveFrameSinkV1 for UncertainSink {
    fn persist(&mut self, frame: &riffdb_storage_api::ArchiveFrameV1) -> Result<(), Error> {
        self.attempts.push(frame.as_bytes().to_vec());
        if self.attempts.len() == 1 {
            Err(Error::SinkUnavailable)
        } else {
            Ok(())
        }
    }
}
#[test]
fn archive_pump_retries_only_pending_bytes_before_considering_a_new_source_pin() {
    let fixture = fixture();
    let foreign = self::fixture();
    let mut pump = Pump::open(
        UncertainSink { attempts: vec![] },
        fixture.lineage,
        fixture.before,
        fixture.pin.as_ref(),
    )
    .unwrap();
    assert_eq!(pump.step(None), Err(Error::SinkUnavailable));
    assert_eq!(pump.consumer.position(), fixture.before);
    let emitted = pump.cursor.position();
    assert!(emitted.sequence() > fixture.before.sequence());
    assert_eq!(pump.step(Some(foreign.pin.as_ref())), Ok(true));
    assert_eq!(pump.consumer.position(), emitted);
    assert_eq!(
        pump.step(Some(foreign.pin.as_ref())),
        Err(Error::ResyncRequired)
    );
    let sink = pump.consumer.into_sink();
    assert_eq!(sink.attempts.len(), 2);
    assert_eq!(sink.attempts[0], sink.attempts[1]);
}

struct HeldSink {
    entered: std::sync::mpsc::SyncSender<()>,
    release: std::sync::mpsc::Receiver<()>,
}
impl ArchiveFrameSinkV1 for HeldSink {
    fn persist(&mut self, _: &riffdb_storage_api::ArchiveFrameV1) -> Result<(), Error> {
        self.entered.send(()).unwrap();
        self.release.recv().unwrap();
        Ok(())
    }
}
#[test]
fn archive_sink_wait_does_not_hold_the_source_publication_slot() {
    let fixture = fixture();
    let (publisher, mut publications) = ReplicationPublication::channel();
    publisher.observe_published_snapshot_v3(fixture.pin.clone());
    let (entered, blocked) = std::sync::mpsc::sync_channel(1);
    let (release, resume) = std::sync::mpsc::sync_channel(1);
    let mut pump = Pump::open(
        HeldSink {
            entered,
            release: resume,
        },
        fixture.lineage,
        fixture.before,
        publications.latest().unwrap().unwrap().as_ref(),
    )
    .unwrap();
    let collector = std::thread::spawn(move || pump.step(None));
    blocked.recv_timeout(Duration::from_secs(5)).unwrap();
    let (done, published) = std::sync::mpsc::sync_channel(1);
    let pin = fixture.pin.clone();
    let source = std::thread::spawn(move || {
        publisher.observe_published_snapshot_v3(pin);
        done.send(()).unwrap();
    });
    let independent = published.recv_timeout(Duration::from_secs(5));
    release.send(()).unwrap();
    source.join().unwrap();
    assert_eq!(collector.join().unwrap(), Ok(true));
    independent.unwrap();
    assert!(publications.latest().unwrap().is_some());
}

async fn failed(worker: &mut Worker) -> Error {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Some(error) = worker.status.borrow_and_update().failure {
                return error;
            }
            worker.status.changed().await.unwrap();
        }
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_worker_missing_backup_refuses_before_creating_sink() {
    let fixture = fixture();
    let (publisher, publications) = ReplicationPublication::channel();
    publisher.observe_published_snapshot_v3(fixture.pin.clone());
    let mut worker = Worker::start(
        fixture.archive.clone(),
        fixture.root.path().join("missing-backups"),
        publications,
    );
    assert_eq!(failed(&mut worker).await, Error::InvalidManifest);
    worker.join();
    assert!(!fixture.archive.path().exists());
    assert_eq!(
        fixture
            .pin
            .authoritative_state_v3()
            .unwrap()
            .history()
            .tail(),
        fixture.head
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_worker_refuses_foreign_publication_without_advancing_existing_sink() {
    let fixture = fixture();
    let foreign = self::fixture();
    let (publisher, publications) = ReplicationPublication::channel();
    publisher.observe_published_snapshot_v3(fixture.pin.clone());
    let mut worker = Worker::start(
        fixture.archive.clone(),
        fixture.root.path().join("backups"),
        publications.clone(),
    );
    caught_up(&mut worker, fixture.head).await;
    publisher.observe_published_snapshot_v3(foreign.pin.clone());
    assert_eq!(failed(&mut worker).await, Error::ResyncRequired);
    worker.join();
    let backup =
        RedbVerifiedArchiveBackup::open(&fixture.root.path().join("backups/baseline")).unwrap();
    let archive = backup
        .open_archive(fixture.archive.path(), fixture.archive.encryption())
        .unwrap();
    assert_eq!(archive.position(), fixture.head);
    assert_eq!(
        fixture
            .pin
            .authoritative_state_v3()
            .unwrap()
            .history()
            .tail(),
        fixture.head
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_worker_held_sink_refuses_without_waiting_for_its_lock() {
    let fixture = fixture();
    let backup =
        RedbVerifiedArchiveBackup::open(&fixture.root.path().join("backups/baseline")).unwrap();
    let held = backup
        .open_archive(fixture.archive.path(), fixture.archive.encryption())
        .unwrap();
    let (publisher, publications) = ReplicationPublication::channel();
    publisher.observe_published_snapshot_v3(fixture.pin.clone());
    let mut worker = Worker::start(
        fixture.archive.clone(),
        fixture.root.path().join("backups"),
        publications,
    );
    assert_eq!(failed(&mut worker).await, Error::SinkUnavailable);
    worker.join();
    assert_eq!(held.position(), fixture.before);
    assert_eq!(
        fixture
            .pin
            .authoritative_state_v3()
            .unwrap()
            .history()
            .tail(),
        fixture.head
    );
}
