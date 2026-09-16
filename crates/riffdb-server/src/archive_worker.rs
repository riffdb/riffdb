//! Archive I/O runs on independent owners of published immutable snapshots.
//! No coordinator, mutation gate, source acknowledgement or application port.
use crate::{config::ConfiguredArchive, replication_publication::ReplicationPublishedSnapshots};
use riffdb_storage_api::{
    ArchiveConsumerErrorV1 as Error, ArchiveConsumerV1, ArchiveFrameSinkV1,
    AuthoritativeStateCatalogV1, ChangelogFrameCursorV3, ChangelogFrameV3, ChangelogHistoryPointV3,
    ChangelogLineageV3, MAX_CHANGELOG_FRAME_BYTES, MAX_STAGED_COMMANDS, PublishedDurableSnapshot,
    ReplicationHandshakeV3,
};
use riffdb_storage_redb::{RedbArchiveRepository, RedbVerifiedArchiveBackup};
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};
use tokio::sync::{Notify, watch};

const SINK_ATTEMPTS: usize = 3;
const RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Status {
    position: Option<ChangelogHistoryPointV3>,
    caught_up: bool,
    failure: Option<Error>,
}

struct Pump<S> {
    consumer: ArchiveConsumerV1<S>,
    cursor: ChangelogFrameCursorV3,
}
impl<S: ArchiveFrameSinkV1> Pump<S> {
    fn open(
        sink: S,
        lineage: ChangelogLineageV3,
        position: ChangelogHistoryPointV3,
        pin: &dyn PublishedDurableSnapshot,
    ) -> Result<Self, Error> {
        let handshake = ReplicationHandshakeV3::new(
            lineage,
            position,
            ChangelogFrameV3::IDENTITY,
            AuthoritativeStateCatalogV1.digest(),
            MAX_CHANGELOG_FRAME_BYTES as u64,
            MAX_STAGED_COMMANDS as u64,
        )
        .map_err(|_| Error::ResyncRequired)?;
        let cursor =
            ChangelogFrameCursorV3::open(pin, handshake).map_err(|_| Error::ResyncRequired)?;
        let mut consumer = ArchiveConsumerV1::new(sink, lineage, position);
        consumer.begin_stream()?;
        Ok(Self { consumer, cursor })
    }
    /// Never emits another frame while sink durability remains uncertain.
    fn step(&mut self, pin: Option<&dyn PublishedDurableSnapshot>) -> Result<bool, Error> {
        if self.consumer.has_pending_frame() {
            self.consumer.retry_pending()?;
            return Ok(true);
        }
        if let Some(pin) = pin {
            self.cursor
                .advance_snapshot(pin)
                .map_err(|_| Error::ResyncRequired)?;
        }
        let Some(frame) = self
            .cursor
            .next_frame()
            .map_err(|_| Error::ResyncRequired)?
        else {
            return Ok(false);
        };
        self.consumer.append(frame.into_bytes())?;
        Ok(true)
    }
}

struct Worker {
    cancellation: Arc<AtomicBool>,
    wake: Arc<Notify>,
    thread: Option<JoinHandle<()>>,
    #[cfg(test)]
    status: watch::Receiver<Status>,
}
impl Worker {
    fn start(
        archive: ConfiguredArchive,
        backup_root: PathBuf,
        publications: ReplicationPublishedSnapshots,
    ) -> Self {
        let cancellation = Arc::new(AtomicBool::new(false));
        let wake = Arc::new(Notify::new());
        let (status, receiver) = watch::channel(Status {
            position: None,
            caught_up: false,
            failure: None,
        });
        let cancelled = cancellation.clone();
        let notified = wake.clone();
        let runtime = tokio::runtime::Handle::current();
        let thread = std::thread::Builder::new()
            .name("riffdb-archive".into())
            .stack_size(crate::PRODUCTION_THREAD_STACK_BYTES)
            .spawn(move || {
                let result = collect(
                    &archive,
                    &backup_root,
                    publications,
                    &cancelled,
                    &notified,
                    &runtime,
                    &status,
                );
                if !cancelled.load(Ordering::Acquire)
                    && let Err(error) = result
                {
                    let mut observed = *status.borrow();
                    observed.failure = Some(error);
                    status.send_replace(observed);
                    // Fixed safe class and checked bounded operator name; never a path or payload.
                    eprintln!(
                        "riffdb-archive-v1\tarchive={}\tstate={error}",
                        archive.name().as_str()
                    );
                }
            })
            .ok();
        if thread.is_none() {
            eprintln!("riffdb-archive-v1\tstate=worker unavailable");
        }
        #[cfg(not(test))]
        drop(receiver);
        Self {
            cancellation,
            wake,
            thread,
            #[cfg(test)]
            status: receiver,
        }
    }
    fn stop(&self) {
        self.cancellation.store(true, Ordering::Release);
        // One retained permit avoids losing shutdown between a cancellation
        // check and registration of the sole waiting future.
        self.wake.notify_one();
    }
    fn join(mut self) {
        self.stop();
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
        {
            // The thread is joined and its pins released even after panic. The
            // uncertain external archive is checked again on the next startup.
            eprintln!("riffdb-archive-v1\tstate=worker failed");
        }
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Graph-owned bounded set; every collector is joined before database close.
pub(crate) struct RunningArchiveWorkers(Vec<Worker>);
impl RunningArchiveWorkers {
    pub(crate) fn start(
        archives: Vec<ConfiguredArchive>,
        backup_root: PathBuf,
        publications: Option<ReplicationPublishedSnapshots>,
    ) -> Self {
        let mut workers = Vec::new();
        for archive in archives
            .into_iter()
            .filter(|archive| archive.backup().is_some())
        {
            if let Some(publications) = publications.clone() {
                workers.push(Worker::start(archive, backup_root.clone(), publications));
            } else {
                eprintln!(
                    "riffdb-archive-v1\tarchive={}\tstate=source unavailable",
                    archive.name().as_str()
                );
            }
        }
        Self(workers)
    }
    pub(crate) fn shutdown(&mut self) {
        for worker in &self.0 {
            worker.stop();
        }
        for worker in self.0.drain(..) {
            worker.join();
        }
    }
}

fn collect(
    archive: &ConfiguredArchive,
    backup_root: &Path,
    mut publications: ReplicationPublishedSnapshots,
    cancellation: &AtomicBool,
    wake: &Notify,
    runtime: &tokio::runtime::Handle,
    status: &watch::Sender<Status>,
) -> Result<(), Error> {
    if cancellation.load(Ordering::Acquire) {
        return Ok(());
    }
    let backup = RedbVerifiedArchiveBackup::open(
        &backup_root.join(archive.backup().ok_or(Error::InvalidManifest)?.as_str()),
    )
    .map_err(|_| Error::InvalidManifest)?;
    let lineage = backup.history().lineage();
    let sink: RedbArchiveRepository =
        backup.open_archive_cancellable(archive.path(), archive.encryption(), cancellation)?;
    let position = sink.position();
    drop(backup);
    let pin = publications
        .latest()
        .map_err(|_| Error::ResyncRequired)?
        .ok_or(Error::ResyncRequired)?;
    let mut pump = Pump::open(sink, lineage, position, pin.as_ref())?;
    drop(pin);
    let mut failures = 0;
    loop {
        if cancellation.load(Ordering::Acquire) {
            return Ok(());
        }
        // Keep only the latest notification. All intermediate authority comes
        // from retained durable history; a pruned gap becomes typed resync.
        let pin = if pump.consumer.has_pending_frame() {
            None
        } else {
            publications
                .take_newer()
                .map_err(|_| Error::ResyncRequired)?
        };
        let step = pump.step(pin.as_deref());
        drop(pin);
        match step {
            Ok(progressed) => {
                failures = 0;
                status.send_replace(Status {
                    position: Some(pump.consumer.position()),
                    caught_up: !progressed,
                    failure: None,
                });
                #[cfg(feature = "test-fixtures")]
                probe::observe(archive.name(), pump.consumer.position());
                if progressed {
                    continue;
                }
                runtime.block_on(async {
                    tokio::select! {
                        biased;
                        _ = wake.notified() => Ok(()),
                        changed = publications.changed() => {
                            let pin = changed.map_err(|_| Error::ResyncRequired)?;
                            pump.cursor.advance_snapshot(pin.as_ref()).map_err(|_| Error::ResyncRequired)
                        }
                    }
                })?;
            }
            Err(Error::SinkUnavailable) => {
                failures += 1;
                if failures >= SINK_ATTEMPTS {
                    return Err(Error::SinkUnavailable);
                }
                runtime.block_on(async {
                    tokio::select! {
                        biased;
                        _ = wake.notified() => {},
                        _ = tokio::time::sleep(RETRY_DELAY) => {},
                    }
                });
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
#[path = "archive_worker_tests.rs"]
mod tests;

#[cfg(feature = "test-fixtures")]
#[path = "archive_worker_probe.rs"]
mod probe;
#[cfg(feature = "test-fixtures")]
pub use probe::install_archive_progress_probe;
