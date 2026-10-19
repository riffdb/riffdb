//! Archive I/O runs on independent owners of published immutable snapshots.
//! No coordinator, mutation gate, source acknowledgement or application port.
use crate::{config::ConfiguredArchive, replication_publication::ReplicationPublishedSnapshots};
use riffdb_storage_api::{
    ArchiveConsumerErrorV1 as Error, ArchiveConsumerV1, ArchiveFrameSinkV1, ChangelogFrameCursorV3,
    ChangelogFrameV3, ChangelogHistoryPointV3, ChangelogLineageV3, MAX_CHANGELOG_FRAME_BYTES,
    MAX_STAGED_COMMANDS, PublishedDurableSnapshot, ReplicationHandshakeV3,
};
use riffdb_storage_redb::{
    ArchiveFrameSummaryV1, RedbArchiveRepository, RedbVerifiedArchiveBackup,
};
use riffdb_types::ArchiveNameV1;
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

/// Callback invoked after an archive worker durably confirms a `(frame, manifest)`
/// pair in `RedbArchiveRepository` (ADR-0320).
pub(crate) type ArchiveConfirmedFrameObserver =
    Arc<dyn Fn(&ArchiveNameV1, &ArchiveFrameSummaryV1) + Send + Sync>;

const SINK_ATTEMPTS: usize = 3;
const RETRY_DELAY: Duration = Duration::from_secs(1);
const COALESCE_IDLE: Duration = Duration::from_millis(10);
const COALESCE_MAX: Duration = Duration::from_millis(80);
const COALESCE_TARGET_COMMANDS: u64 = (MAX_STAGED_COMMANDS - 32) as u64;
const MAINTENANCE_POLL: Duration = Duration::from_millis(10);
const MAINTENANCE_MAX_WAIT: Duration = Duration::from_millis(1500);
const BACKLOG_FRAME_PACE: Duration = Duration::from_millis(6);

fn point_transitions(point: ChangelogHistoryPointV3) -> u64 {
    point.frontier().application().map_or(0, |s| s.get())
        + point.frontier().administration().map_or(0, |s| s.get())
}

fn snapshot_transitions(pin: &dyn PublishedDurableSnapshot) -> Result<u64, Error> {
    let app = pin
        .application_frontier()
        .map_err(|_| Error::ResyncRequired)?
        .map_or(0, |s| s.get());
    let admin = pin
        .administration_frontier()
        .map_err(|_| Error::ResyncRequired)?
        .map_or(0, |s| s.get());
    Ok(app + admin)
}

fn wait_for_extent_maintenance(
    cancellation: &AtomicBool,
    wake: &Notify,
    runtime: &tokio::runtime::Handle,
) -> bool {
    if !riffdb_storage_redb::journal_extent_maintenance_active_v1() {
        return false;
    }
    runtime.block_on(async {
        let deadline = tokio::time::Instant::now() + MAINTENANCE_MAX_WAIT;
        while riffdb_storage_redb::journal_extent_maintenance_active_v1()
            && !cancellation.load(Ordering::Acquire)
        {
            let now = tokio::time::Instant::now();
            if now >= deadline {
                break;
            }
            let sleep_until = (now + MAINTENANCE_POLL).min(deadline);
            tokio::select! {
                biased;
                _ = wake.notified() => break,
                _ = tokio::time::sleep_until(sleep_until) => {},
            }
        }
    });
    true
}

fn coalesce_publications(
    publications: &mut ReplicationPublishedSnapshots,
    mut latest: Option<Arc<dyn PublishedDurableSnapshot>>,
    archived_transitions: u64,
    cancellation: &AtomicBool,
    wake: &Notify,
    runtime: &tokio::runtime::Handle,
) -> Result<(Option<Arc<dyn PublishedDurableSnapshot>>, bool), Error> {
    runtime.block_on(async {
        let deadline = tokio::time::Instant::now() + COALESCE_MAX;
        loop {
            if cancellation.load(Ordering::Acquire) {
                return Ok((latest, true));
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Ok((latest, false));
            }
            let sleep_until = (now + COALESCE_IDLE).min(deadline);
            tokio::select! {
                biased;
                _ = wake.notified() => return Ok((latest, true)),
                _ = tokio::time::sleep_until(sleep_until) => {},
            }
            if riffdb_storage_redb::journal_extent_maintenance_active_v1() {
                return Ok((latest, false));
            }
            let Some(newer) = publications
                .take_newer()
                .map_err(|_| Error::ResyncRequired)?
            else {
                return Ok((latest, true));
            };
            let current = snapshot_transitions(newer.as_ref())?;
            latest = Some(newer);
            if current.saturating_sub(archived_transitions) >= COALESCE_TARGET_COMMANDS {
                return Ok((latest, false));
            }
        }
    })
}

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
            lineage.catalog_digest(),
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
            .next_coalesced_frame()
            .map_err(|_| Error::ResyncRequired)?
        else {
            return Ok(false);
        };
        self.consumer.append_emitted(frame)?;
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
    #[cfg(test)]
    fn start(
        archive: ConfiguredArchive,
        backup_root: PathBuf,
        publications: ReplicationPublishedSnapshots,
    ) -> Self {
        Self::start_with_observer(archive, backup_root, publications, None)
    }

    fn start_with_observer(
        archive: ConfiguredArchive,
        backup_root: PathBuf,
        publications: ReplicationPublishedSnapshots,
        observer: Option<ArchiveConfirmedFrameObserver>,
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
                    observer,
                );
                if !cancelled.load(Ordering::Acquire)
                    && let Err(error) = result
                {
                    let mut observed = *status.borrow();
                    observed.failure = Some(error);
                    status.send_replace(observed);
                    #[cfg(feature = "test-fixtures")]
                    probe::observe_failure(archive.name(), error);
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
        Self::start_with_observer(archives, backup_root, publications, None)
    }

    pub(crate) fn start_with_observer(
        archives: Vec<ConfiguredArchive>,
        backup_root: PathBuf,
        publications: Option<ReplicationPublishedSnapshots>,
        observer: Option<ArchiveConfirmedFrameObserver>,
    ) -> Self {
        let mut workers = Vec::new();
        for archive in archives
            .into_iter()
            .filter(|archive| archive.backup().is_some())
        {
            if let Some(publications) = publications.clone() {
                workers.push(Worker::start_with_observer(
                    archive,
                    backup_root.clone(),
                    publications,
                    observer.clone(),
                ));
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

#[expect(
    clippy::too_many_arguments,
    reason = "archive collector loop binds configured archive, backup root, snapshot channel, cancellation, wake, runtime handle, status watch, and optional confirmed-frame observer"
)]
fn collect(
    archive: &ConfiguredArchive,
    backup_root: &Path,
    mut publications: ReplicationPublishedSnapshots,
    cancellation: &AtomicBool,
    wake: &Notify,
    runtime: &tokio::runtime::Handle,
    status: &watch::Sender<Status>,
    observer: Option<ArchiveConfirmedFrameObserver>,
) -> Result<(), Error> {
    if cancellation.load(Ordering::Acquire) {
        return Ok(());
    }
    let backup = RedbVerifiedArchiveBackup::open(
        &backup_root.join(archive.backup().ok_or(Error::InvalidManifest)?.as_str()),
    )
    .map_err(|_| Error::InvalidManifest)?;
    let lineage = backup.history().lineage();
    let mut sink: RedbArchiveRepository =
        backup.open_archive_cancellable(archive.path(), archive.encryption(), cancellation)?;
    if let Some(observer) = observer {
        let archive_name = archive.name().clone();
        sink.set_confirmed_frame_observer(Arc::new(move |summary| {
            observer(&archive_name, summary);
        }));
    }
    let position = sink.position();
    drop(backup);
    let pin = publications
        .latest()
        .map_err(|_| Error::ResyncRequired)?
        .ok_or(Error::ResyncRequired)?;
    let mut pump = Pump::open(sink, lineage, position, pin.as_ref())?;
    drop(pin);
    let mut failures = 0;
    let mut next_pin = None;
    let mut startup_drained = false;
    let mut coalesced_idle = false;
    loop {
        if cancellation.load(Ordering::Acquire) {
            return Ok(());
        }
        // Keep only the latest notification. All intermediate authority comes
        // from retained durable history; a pruned gap becomes typed resync.
        let pin = if pump.consumer.has_pending_frame() {
            if startup_drained {
                let _ = wait_for_extent_maintenance(cancellation, wake, runtime);
                if cancellation.load(Ordering::Acquire) {
                    return Ok(());
                }
            }
            None
        } else if !startup_drained {
            if let Some(newer) = publications
                .take_newer()
                .map_err(|_| Error::ResyncRequired)?
            {
                next_pin = None;
                Some(newer)
            } else {
                next_pin.take()
            }
        } else {
            let _ = wait_for_extent_maintenance(cancellation, wake, runtime);
            if cancellation.load(Ordering::Acquire) {
                return Ok(());
            }
            let archived_transitions = point_transitions(pump.consumer.position());
            let cursor_transitions = point_transitions(
                pump.cursor
                    .published_head()
                    .map_err(|_| Error::ResyncRequired)?,
            );
            let cursor_pending = cursor_transitions.saturating_sub(archived_transitions);
            if cursor_pending >= COALESCE_TARGET_COMMANDS {
                runtime.block_on(async {
                    tokio::select! {
                        biased;
                        _ = wake.notified() => {},
                        _ = tokio::time::sleep(BACKLOG_FRAME_PACE) => {},
                    }
                });
                let _ = wait_for_extent_maintenance(cancellation, wake, runtime);
                if cancellation.load(Ordering::Acquire) {
                    return Ok(());
                }
                coalesced_idle = false;
                None
            } else {
                let mut candidate = if let Some(newer) = publications
                    .take_newer()
                    .map_err(|_| Error::ResyncRequired)?
                {
                    next_pin = None;
                    Some(newer)
                } else {
                    next_pin.take()
                };
                let latest_transitions = match &candidate {
                    Some(pin) => snapshot_transitions(pin.as_ref())?,
                    None => cursor_transitions,
                };
                let pending = latest_transitions.saturating_sub(archived_transitions);
                if pending < COALESCE_TARGET_COMMANDS
                    && (pending > 0 || (candidate.is_none() && !coalesced_idle))
                {
                    let (coalesced, idle) = coalesce_publications(
                        &mut publications,
                        candidate,
                        archived_transitions,
                        cancellation,
                        wake,
                        runtime,
                    )?;
                    candidate = coalesced;
                    coalesced_idle = idle;
                    if cancellation.load(Ordering::Acquire) {
                        return Ok(());
                    }
                } else if pending >= COALESCE_TARGET_COMMANDS {
                    coalesced_idle = false;
                }
                if wait_for_extent_maintenance(cancellation, wake, runtime) {
                    if cancellation.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    if let Some(newer) = publications
                        .take_newer()
                        .map_err(|_| Error::ResyncRequired)?
                    {
                        next_pin = None;
                        candidate = Some(newer);
                        coalesced_idle = false;
                    }
                }
                candidate
            }
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
                startup_drained = true;
                coalesced_idle = false;
                next_pin = runtime.block_on(async {
                    tokio::select! {
                        biased;
                        _ = wake.notified() => Ok(None),
                        changed = publications.changed() => {
                            changed.map(Some).map_err(|_| Error::ResyncRequired)
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
