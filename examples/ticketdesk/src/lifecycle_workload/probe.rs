//! Bounded offered-load probe; lifecycle orchestration remains controller-owned.

use std::collections::VecDeque;
use std::env;
use std::io::Read;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::Barrier;
use tokio::task::JoinSet;

use super::application::{GetTicketParams, GetTicketResult};
use super::{Result, TICKETS_PER_ORGANIZATION, current, model, uuid};

const WORKERS: u32 = 28;
const QUEUE: usize = 32_768;
const LATENCY_CEILING: usize = 250_000;
const MAX_WINDOW_SAMPLES: u64 = 200_000;

// A campaign publishes the host CLOCK_MONOTONIC origin once. Both owners then
// use that exact epoch; process replacement cannot move the offered timeline.
struct OfferedClock {
    legacy: Instant,
    shared_origin_ns: Option<u64>,
}

fn shared_clock_requested(value: Option<&str>) -> Result<bool> {
    match value {
        None => Ok(false),
        Some("1") => Ok(true),
        _ => Err("offered_clock_configuration"),
    }
}

#[cfg(target_os = "linux")]
fn monotonic_ns() -> Result<u64> {
    let now = rustix::time::clock_gettime(rustix::time::ClockId::Monotonic);
    monotonic_parts(now.tv_sec, now.tv_nsec)
}

#[cfg(not(target_os = "linux"))]
fn monotonic_ns() -> Result<u64> {
    Err("offered_clock_platform")
}

#[cfg(any(target_os = "linux", test))]
fn monotonic_parts(seconds: i64, nanos: i64) -> Result<u64> {
    let seconds = u64::try_from(seconds).map_err(|_| "offered_clock_value")?;
    let nanos = u64::try_from(nanos).map_err(|_| "offered_clock_value")?;
    if nanos >= 1_000_000_000 {
        return Err("offered_clock_value");
    }
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(nanos))
        .filter(|value| *value > 0)
        .ok_or("offered_clock_value")
}

impl OfferedClock {
    fn start(shared: bool) -> Result<Self> {
        Ok(Self {
            legacy: Instant::now(),
            shared_origin_ns: if shared { Some(monotonic_ns()?) } else { None },
        })
    }

    fn elapsed(&self) -> Result<Duration> {
        match self.shared_origin_ns {
            Some(origin) => monotonic_ns()?
                .checked_sub(origin)
                .map(Duration::from_nanos)
                .ok_or("offered_clock_rewind"),
            None => Ok(self.legacy.elapsed()),
        }
    }

    async fn wait_for(&self, second: u64) -> Result<()> {
        self.wait_until(Duration::from_secs(second)).await
    }

    async fn wait_for_offset(&self, second: u64, offset_us: u32) -> Result<()> {
        if offset_us >= 1_000_000 {
            return Err("offered_clock_value");
        }
        let deadline = Duration::from_secs(second)
            .checked_add(Duration::from_micros(u64::from(offset_us)))
            .ok_or("offered_clock_value")?;
        self.wait_until(deadline).await
    }

    async fn wait_until(&self, deadline: Duration) -> Result<()> {
        loop {
            let remaining = deadline.saturating_sub(self.elapsed()?);
            if remaining.is_zero() {
                return Ok(());
            }
            tokio::time::sleep(remaining).await;
        }
    }
}

struct Active {
    current: Arc<AtomicU32>,
}

impl Active {
    fn reserve(current: &Arc<AtomicU32>, peak: &AtomicU32) -> Result<Self> {
        let previous = current
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                if active < WORKERS {
                    Some(active + 1)
                } else {
                    None
                }
            })
            .map_err(|_| "in_flight_bound")?;
        peak.fetch_max(previous + 1, Ordering::AcqRel);
        Ok(Self {
            current: Arc::clone(current),
        })
    }
}

impl Drop for Active {
    fn drop(&mut self) {
        self.current.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Release {
    pub(super) generation: u64,
    pub(super) version: u64,
    pub(super) incarnation: u64,
    pub(super) credential: PathBuf,
}

struct AdmissionState {
    paused: bool,
    restore_reads: bool,
    restore_generation: Option<u64>,
    restore_after_reads: Option<u64>,
    paused_commands: Option<u64>,
    queue: VecDeque<Job>,
    closed: bool,
    release: Arc<Release>,
    resume_after_commands: Option<u64>,
}

struct Admission {
    state: Mutex<AdmissionState>,
    current: Arc<AtomicU32>,
    changed: tokio::sync::Notify,
}

impl Admission {
    fn new(release: Release, current: Arc<AtomicU32>) -> Self {
        Self {
            state: Mutex::new(AdmissionState {
                paused: false,
                restore_reads: false,
                restore_generation: None,
                restore_after_reads: None,
                paused_commands: None,
                queue: VecDeque::with_capacity(QUEUE),
                closed: false,
                release: Arc::new(release),
                resume_after_commands: None,
            }),
            current,
            changed: tokio::sync::Notify::new(),
        }
    }

    #[cfg(test)]
    fn admit(&self, peak: &AtomicU32) -> Result<Option<(Active, Arc<Release>)>> {
        let state = self.state.lock().map_err(|_| "admission_state")?;
        if state.paused {
            return Ok(None);
        }
        Ok(Some((
            Active::reserve(&self.current, peak)?,
            Arc::clone(&state.release),
        )))
    }

    fn pause(&self) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| "admission_state")?;
        state.paused = true;
        state.restore_reads = false;
        Ok(())
    }

    fn offer(&self, job: Job) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| "admission_state")?;
        if state.closed || state.queue.len() == QUEUE {
            return Err("offered_queue_bound");
        }
        state.queue.push_back(job);
        drop(state);
        self.changed.notify_one();
        Ok(())
    }

    fn close_offers(&self) -> Result<()> {
        self.state.lock().map_err(|_| "admission_state")?.closed = true;
        self.changed.notify_waiters();
        Ok(())
    }

    fn take_job(&self, peak: &AtomicU32) -> Result<Dispatch> {
        let mut state = self.state.lock().map_err(|_| "admission_state")?;
        let index = if !state.paused {
            (!state.queue.is_empty()).then_some(0)
        } else if state.restore_reads {
            state
                .queue
                .iter()
                .position(|job| matches!(job.operation, Operation::Read(_)))
        } else {
            None
        };
        let Some(index) = index else {
            return Ok(if state.closed && state.queue.is_empty() {
                Dispatch::Closed
            } else {
                Dispatch::Pending
            });
        };
        // Selection and reservation share the original admission mutex. A
        // paused write never occupies a worker waiting ahead of readable jobs.
        let active = Active::reserve(&self.current, peak)?;
        let job = state.queue.remove(index).ok_or("offered_queue_state")?;
        Ok(Dispatch::Admitted(job, active, Arc::clone(&state.release)))
    }

    fn quiesced(&self) -> Result<bool> {
        let state = self.state.lock().map_err(|_| "admission_state")?;
        Ok(state.paused && !state.restore_reads && self.current.load(Ordering::Acquire) == 0)
    }

    #[cfg(test)]
    fn replace(&self, release: Release) -> Result<()> {
        self.replace_at(release, None)
    }

    fn replace_at(&self, release: Release, resume_after_commands: Option<u64>) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| "admission_state")?;
        if !state.paused
            || state.restore_reads
            || self.current.load(Ordering::Acquire) != 0
            || state.resume_after_commands.is_some()
            || release.generation <= state.release.generation
            || !(1..=2).contains(&release.version)
            || release.version < state.release.version
            || (release.incarnation != state.release.incarnation
                && Some(release.incarnation) != state.release.incarnation.checked_add(1))
        {
            return Err("release_change_requires_quiescence");
        }
        state.release = Arc::new(release);
        state.resume_after_commands = resume_after_commands;
        Ok(())
    }

    fn release(&self) -> Result<Arc<Release>> {
        Ok(Arc::clone(
            &self.state.lock().map_err(|_| "admission_state")?.release,
        ))
    }

    fn resume(&self) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| "admission_state")?;
        if state
            .restore_generation
            .is_some_and(|generation| state.release.generation <= generation)
        {
            return Err("restore_read_unfenced_release");
        }
        state.paused = false;
        state.restore_reads = false;
        state.restore_generation = None;
        state.restore_after_reads = None;
        state.paused_commands = None;
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    fn allow_restore_reads(&self, commands: u64, reads: u64) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| "admission_state")?;
        if !state.paused
            || state.paused_commands != Some(commands)
            || state.resume_after_commands.is_some()
        {
            return Err("restore_reads_requires_quiescence");
        }
        if state.restore_generation.is_none() {
            if self.current.load(Ordering::Acquire) != 0 {
                return Err("restore_reads_requires_quiescence");
            }
            state.restore_generation = Some(state.release.generation);
            state.restore_after_reads = Some(reads);
        } else if !state.restore_reads || state.restore_generation != Some(state.release.generation)
        {
            return Err("restore_reads_repeated");
        }
        state.restore_reads = true;
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    fn poll(&self, root: &Path, metrics: &Mutex<Metrics>) -> Result<()> {
        let paused = root.join("load.quiesce").exists();
        let restore_reads = restore_reads_requested(root, self.release()?.as_ref())?;
        if restore_reads && !paused {
            return Err("restore_reads_requires_quiescence");
        }
        if restore_reads {
            let totals = metrics.lock().map_err(|_| "observation_state")?;
            self.allow_restore_reads(totals.total_commands, totals.total_reads)?;
        } else if paused {
            self.pause()?;
        }
        let phase = root.join("load.phase");
        if phase.exists() {
            let release = read_release(root, &phase)?;
            if release != *self.release()? {
                if root.join("load.resumed").exists() {
                    return Err("resume_confirmation_stale");
                }
                let commands = metrics
                    .lock()
                    .map_err(|_| "observation_state")?
                    .total_commands;
                self.replace_at(release, Some(commands))?;
            }
        }
        if !paused {
            self.resume()?;
            return self.confirm_resume(root, metrics);
        }
        if self.quiesced()? && !root.join("load.quiesced").exists() {
            let totals = metrics.lock().map_err(|_| "observation_state")?;
            publish_quiescence(
                root,
                "load.quiesced",
                &format!(
                    "{{\"schema\":\"riffdb.ticketdesk-lifecycle-quiesced/v1\",\"generation\":{},\"completed_commands\":{},\"completed_reads\":{}}}\n",
                    self.release()?.generation,
                    totals.total_commands,
                    totals.total_reads
                ),
            )?;
            self.state
                .lock()
                .map_err(|_| "admission_state")?
                .paused_commands = Some(totals.total_commands);
        }
        self.confirm_restore_reads(root, metrics)?;
        Ok(())
    }

    fn confirm_restore_reads(&self, root: &Path, metrics: &Mutex<Metrics>) -> Result<()> {
        let (release, reading, after, commands) = {
            let state = self.state.lock().map_err(|_| "admission_state")?;
            if state.restore_generation.is_none() {
                return Ok(());
            }
            (
                Arc::clone(&state.release),
                state.restore_reads,
                state.restore_after_reads.ok_or("restore_read_state")?,
                state.paused_commands.ok_or("restore_read_state")?,
            )
        };
        let totals = metrics.lock().map_err(|_| "observation_state")?;
        if totals.total_commands != commands {
            return Err("restore_read_acknowledgement_gap");
        }
        let (name, schema) = if reading {
            if totals.total_reads <= after {
                return Ok(());
            }
            (
                "load.restore-reads-active",
                "riffdb.ticketdesk-lifecycle-restore-reads-active/v1",
            )
        } else {
            if !self.quiesced()? {
                return Ok(());
            }
            (
                "load.restore-readers-quiesced",
                "riffdb.ticketdesk-lifecycle-restore-readers-quiesced/v1",
            )
        };
        if !root.join(name).exists() {
            publish_quiescence(
                root,
                name,
                &format!(
                    "{{\"schema\":\"{schema}\",\"generation\":{},\"contract_version\":{},\"history_incarnation\":{},\"completed_commands\":{},\"completed_reads\":{}}}\n",
                    release.generation,
                    release.version,
                    release.incarnation,
                    totals.total_commands,
                    totals.total_reads,
                ),
            )?;
        }
        Ok(())
    }

    fn confirm_resume(&self, root: &Path, metrics: &Mutex<Metrics>) -> Result<()> {
        let (release, after) = {
            let state = self.state.lock().map_err(|_| "admission_state")?;
            let Some(after) = state.resume_after_commands else {
                return Ok(());
            };
            (Arc::clone(&state.release), after)
        };
        let (commands, reads) = {
            let totals = metrics.lock().map_err(|_| "observation_state")?;
            (totals.total_commands, totals.total_reads)
        };
        if commands <= after {
            return Ok(());
        }
        // This is one acknowledgement after handover, not periodic telemetry.
        // Native operation validation and witness recording precede these totals.
        // Publish outside both mutexes; the control poll is the sole publisher.
        publish_quiescence(
            root,
            "load.resumed",
            &format!(
                "{{\"schema\":\"riffdb.ticketdesk-lifecycle-resumed/v1\",\"generation\":{},\"contract_version\":{},\"history_incarnation\":{},\"completed_commands\":{commands},\"completed_reads\":{reads}}}\n",
                release.generation, release.version, release.incarnation
            ),
        )?;
        let mut state = self.state.lock().map_err(|_| "admission_state")?;
        if state.release != release || state.resume_after_commands != Some(after) {
            return Err("resume_confirmation_state");
        }
        state.resume_after_commands = None;
        Ok(())
    }
}

pub(super) fn publish_quiescence(root: &Path, name: &str, encoded: &str) -> Result<()> {
    if !matches!(
        name,
        "load.quiesced"
            | "observer.quiesced"
            | "load.resumed"
            | "load.terminal"
            | "load.restore-reads-active"
            | "load.restore-readers-quiesced"
    ) || encoded.len() > 4096
    {
        return Err("quiescence_output");
    }
    let pending = root.join(format!(".{name}.pending"));
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&pending)
        .map_err(|_| "quiescence_output")?;
    let result = (|| {
        output
            .write_all(encoded.as_bytes())
            .map_err(|_| "quiescence_output")?;
        output.sync_all().map_err(|_| "quiescence_output")?;
        std::fs::hard_link(&pending, root.join(name)).map_err(|_| "quiescence_output")
    })();
    drop(output);
    std::fs::remove_file(pending).map_err(|_| "quiescence_output")?;
    result
}

pub(super) fn read_release(root: &Path, path: &Path) -> Result<Release> {
    if path.is_symlink() || !path.is_file() {
        return Err("release_input");
    }
    let file = std::fs::File::open(path).map_err(|_| "release_input")?;
    let mut encoded = Vec::with_capacity(1025);
    file.take(1025)
        .read_to_end(&mut encoded)
        .map_err(|_| "release_input")?;
    if encoded.len() > 1024 {
        return Err("release_input_bound");
    }
    let text = std::str::from_utf8(&encoded).map_err(|_| "release_input")?;
    let fields = text
        .strip_suffix('\n')
        .ok_or("release_input")?
        .split(' ')
        .collect::<Vec<_>>();
    if fields.len() != 4
        || fields[3].is_empty()
        || fields[3].len() > 128
        || !fields[3]
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        || matches!(fields[3], "." | "..")
    {
        return Err("release_input");
    }
    let generation = fields[0].parse::<u64>().map_err(|_| "release_input")?;
    let version = fields[1].parse::<u64>().map_err(|_| "release_input")?;
    let incarnation = fields[2].parse::<u64>().map_err(|_| "release_input")?;
    if generation == 0
        || !(1..=2).contains(&version)
        || generation.to_string() != fields[0]
        || version.to_string() != fields[1]
        || incarnation == 0
        || incarnation.to_string() != fields[2]
    {
        return Err("release_input");
    }
    Ok(Release {
        generation,
        version,
        incarnation,
        credential: root.join(fields[3]),
    })
}

fn restore_reads_requested(root: &Path, release: &Release) -> Result<bool> {
    use std::os::unix::fs::MetadataExt;
    let path = root.join("load.restore-reads");
    let before = match std::fs::symlink_metadata(&path) {
        Ok(info) => info,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(_) => return Err("restore_read_control"),
    };
    if !before.is_file() || before.len() == 0 || before.len() > 128 || before.nlink() != 1 {
        return Err("restore_read_control");
    }
    let mut file = match std::fs::File::open(&path) {
        Ok(file) => file,
        Err(error)
            if error.kind() == io::ErrorKind::NotFound
                && matches!(std::fs::symlink_metadata(&path), Err(ref e) if e.kind() == io::ErrorKind::NotFound) =>
        {
            return Ok(false);
        }
        Err(_) => return Err("restore_read_control"),
    };
    let identity = |info: &std::fs::Metadata| {
        (
            info.dev(),
            info.ino(),
            info.len(),
            info.mtime(),
            info.mtime_nsec(),
            info.ctime(),
            info.ctime_nsec(),
        )
    };
    let opened = file.metadata().map_err(|_| "restore_read_control")?;
    if identity(&opened) != identity(&before) || opened.nlink() != 1 {
        return Err("restore_read_control");
    }
    let mut bytes = Vec::with_capacity(129);
    (&mut file)
        .take(129)
        .read_to_end(&mut bytes)
        .map_err(|_| "restore_read_control")?;
    let after = file.metadata().map_err(|_| "restore_read_control")?;
    let named = match std::fs::symlink_metadata(&path) {
        Ok(info) => info,
        // Removing this operation-local marker intentionally closes read
        // admission. Never keep reads enabled using a retired descriptor.
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(_) => return Err("restore_read_control"),
    };
    let expected = format!(
        "{} {} {}\n",
        release.generation, release.version, release.incarnation
    );
    if bytes != expected.as_bytes()
        || identity(&opened) != identity(&after)
        || identity(&opened) != identity(&named)
        || !named.is_file()
        || named.nlink() != 1
    {
        return Err("restore_read_control");
    }
    Ok(true)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Operation {
    Update(u64),
    Read(u64),
}

struct Job {
    operation: Operation,
    offered: Instant,
    window: u64,
}

enum Dispatch {
    Pending,
    Closed,
    Admitted(Job, Active, Arc<Release>),
}

/// Bound on how long a closed window waits for its offered operations to
/// settle before it is published with whatever completed.
const WINDOW_SETTLE: Duration = Duration::from_secs(60);

/// Per-second offered operations: exactly 100 updates and 400 reads, each at
/// its own offset within the second. Offsets come from a fixed-seed generator
/// keyed by the second, so every run offers the identical schedule. Uniform
/// independent offsets for a fixed count are a Poisson arrival process
/// conditioned on that count: clustered like real traffic, never a burst of
/// the whole second at one instant.
fn arrival_schedule(second: u64) -> Vec<(u32, Operation)> {
    let mut state = second
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(0xD1B5_4A32_D192_ED03);
    let mut next_offset = || {
        // splitmix64
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut mixed = state;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        mixed ^= mixed >> 31;
        u32::try_from(mixed % 1_000_000).unwrap_or(0)
    };
    let mut schedule = Vec::with_capacity(500);
    for command in 0..100 {
        schedule.push((next_offset(), Operation::Update(second * 100 + command)));
        for read in 0..4 {
            schedule.push((
                next_offset(),
                Operation::Read(second * 400 + command * 4 + read),
            ));
        }
    }
    // Stable: equal offsets keep their generation order.
    schedule.sort_by_key(|(offset, _)| *offset);
    schedule
}

/// One 300-second observation window. Operations are attributed to the
/// window they were offered in, wherever their completion falls.
struct Window {
    index: u64,
    start: u64,
    end: u64,
    commands: u64,
    reads: u64,
    retries: u64,
    latencies: Vec<u32>,
    overflow: u64,
    peak: u32,
}

impl Window {
    fn new(index: u64, start: u64, end: u64) -> Self {
        Self {
            index,
            start,
            end,
            commands: 0,
            reads: 0,
            retries: 0,
            latencies: vec![0; LATENCY_CEILING + 1],
            overflow: 0,
            peak: 0,
        }
    }

    fn settled(&self) -> bool {
        let seconds = self.end - self.start;
        self.commands == seconds * 100 && self.reads == seconds * 400
    }

    fn record(&mut self, operation: Operation, latency: Duration, retries: u32) -> Result<()> {
        if self.commands + self.reads >= MAX_WINDOW_SAMPLES {
            return Err("sample_bound");
        }
        match operation {
            Operation::Update(_) => self.commands += 1,
            Operation::Read(_) => self.reads += 1,
        }
        self.retries = self
            .retries
            .checked_add(u64::from(retries))
            .ok_or("sample_bound")?;
        let micros = latency.as_micros();
        if micros > LATENCY_CEILING as u128 {
            self.overflow += 1;
        } else {
            let index = usize::try_from(micros).map_err(|_| "sample_bound")?;
            self.latencies[index] = self.latencies[index].checked_add(1).ok_or("sample_bound")?;
        }
        Ok(())
    }

    fn quantile(&self, percent: u64) -> u64 {
        let count = self.commands + self.reads;
        if count == 0 {
            return 0;
        }
        let wanted = (count * percent).div_ceil(100);
        let mut seen = 0_u64;
        for (micros, frequency) in self.latencies.iter().enumerate() {
            seen += u64::from(*frequency);
            if seen >= wanted {
                return micros as u64;
            }
        }
        u64::MAX
    }

    fn line(&self) -> Result<String> {
        let duration = self.end.checked_sub(self.start).ok_or("window_bound")?;
        if duration == 0 {
            return Err("window_bound");
        }
        Ok(format!(
            concat!(
                "{{\"schema\":\"riffdb.ticketdesk-lifecycle-load-probe/v1\",",
                "\"start_s\":{},\"end_s\":{},\"offered_commands\":{},\"offered_reads\":{},",
                "\"completed_commands\":{},\"completed_reads\":{},",
                "\"settled_submission_attempts\":{},\"transport_attempts\":{},\"retries\":{},",
                "\"p95_us\":{},\"p99_us\":{},\"load_peak_in_flight\":{},",
                "\"reserved_auxiliary_slots\":4,\"latency_overflow\":{}}}"
            ),
            self.start,
            self.end,
            duration * 100,
            duration * 400,
            self.commands,
            self.reads,
            self.commands + self.reads + self.retries,
            self.commands + self.reads + self.retries,
            self.retries,
            self.quantile(95),
            self.quantile(99),
            self.peak,
            self.overflow
        ))
    }
}

/// The open window, the window closing behind it, and completions that
/// arrived after their window was published.
struct Metrics {
    total_commands: u64,
    total_reads: u64,
    open: Window,
    closing: Option<(Window, Instant)>,
    late_commands: u64,
    late_reads: u64,
}

impl Metrics {
    fn new(first_end: u64) -> Self {
        Self {
            total_commands: 0,
            total_reads: 0,
            open: Window::new(0, 0, first_end),
            closing: None,
            late_commands: 0,
            late_reads: 0,
        }
    }

    fn record(
        &mut self,
        window: u64,
        operation: Operation,
        latency: Duration,
        retries: u32,
    ) -> Result<()> {
        match operation {
            Operation::Update(_) => self.total_commands += 1,
            Operation::Read(_) => self.total_reads += 1,
        }
        if self.open.index == window {
            return self.open.record(operation, latency, retries);
        }
        if let Some((closing, _)) = self.closing.as_mut()
            && closing.index == window
        {
            return closing.record(operation, latency, retries);
        }
        match operation {
            Operation::Update(_) => self.late_commands += 1,
            Operation::Read(_) => self.late_reads += 1,
        }
        Ok(())
    }

    /// Starts the next window; the current one closes once its offered
    /// operations settle or at `deadline`, whichever comes first.
    fn rotate(&mut self, next: Option<Window>, peak: u32, deadline: Instant) -> Result<()> {
        if self.closing.is_some() {
            return Err("window_bound");
        }
        let mut finished = match next {
            Some(next) => std::mem::replace(&mut self.open, next),
            None => std::mem::replace(&mut self.open, Window::new(u64::MAX, 0, 0)),
        };
        finished.peak = peak;
        self.closing = Some((finished, deadline));
        Ok(())
    }

    /// The closing window's observation line, once it is due.
    fn due(&mut self, now: Instant) -> Result<Option<String>> {
        let ready = self
            .closing
            .as_ref()
            .is_some_and(|(window, deadline)| window.settled() || now >= *deadline);
        if !ready {
            return Ok(None);
        }
        let (window, _) = self.closing.take().ok_or("window_bound")?;
        window.line().map(Some)
    }
}

fn publish_line(line: &str) -> Result<()> {
    println!("{line}");
    io::stdout().flush().map_err(|_| "observation_output")
}

pub(super) async fn run() -> Result<()> {
    let duration = env::var("RIFFDB_LIFECYCLE_DURATION_SECONDS")
        .map_err(|_| "configuration")?
        .parse::<u64>()
        .map_err(|_| "configuration")?;
    if !(1..=86_400).contains(&duration) {
        return Err("duration_bound");
    }
    let clock_configuration = match env::var("RIFFDB_LIFECYCLE_SHARED_CLOCK") {
        Ok(value) => Some(value),
        Err(env::VarError::NotPresent) => None,
        Err(_) => return Err("offered_clock_configuration"),
    };
    let shared_clock = shared_clock_requested(clock_configuration.as_deref())?;
    let ready = Arc::new(Barrier::new(WORKERS as usize + 1));
    let metrics = Arc::new(Mutex::new(Metrics::new(duration.min(300))));
    let current = Arc::new(AtomicU32::new(0));
    let peak = Arc::new(AtomicU32::new(0));
    let version = match env::var("RIFFDB_LIFECYCLE_CONTRACT_VERSION") {
        Ok(value) if value == "1" => 1,
        Ok(value) if value == "2" => 2,
        Err(env::VarError::NotPresent) => 1,
        _ => return Err("contract_version"),
    };
    let initial = Release {
        generation: 1,
        version,
        incarnation: 1,
        credential: PathBuf::from(
            env::var_os("RIFFDB_LIFECYCLE_APPLICATION_CREDENTIAL").ok_or("configuration")?,
        ),
    };
    let admission = Arc::new(Admission::new(initial, Arc::clone(&current)));
    let witness = super::witness::Witness::configured("updates.witness", duration * 100)?;
    let control = env::var_os("RIFFDB_LIFECYCLE_LOAD_CONTROL").map(PathBuf::from);
    if let Some(root) = &control {
        if !root.is_absolute() || root.is_symlink() || !root.is_dir() {
            return Err("load_control_root");
        }
        admission.poll(root, &metrics)?;
    }
    let terminal_configuration = match env::var("RIFFDB_LIFECYCLE_TERMINAL_BARRIER") {
        Ok(value) => Some(value),
        Err(env::VarError::NotPresent) => None,
        Err(_) => return Err("terminal_configuration"),
    };
    let terminal =
        super::terminal::configured(control.as_deref(), terminal_configuration.as_deref())?;
    let offered_commands = Arc::new(AtomicU64::new(0));
    let mut workers = JoinSet::new();
    for worker in 0..u64::from(WORKERS) {
        let ready = Arc::clone(&ready);
        let metrics = Arc::clone(&metrics);
        let admission = Arc::clone(&admission);
        let peak = Arc::clone(&peak);
        let offered_commands = Arc::clone(&offered_commands);
        let control = control.clone();
        let witness = witness.clone();
        workers.spawn(async move {
            let mut release = admission.release()?;
            let mut client = current::connect_release(release.version, &release.credential).await?;
            // Warm named-query carriage before the offered-load clock begins.
            let warm = client
                .get_ticket(GetTicketParams {
                    organization_id: uuid(0x8001, worker / TICKETS_PER_ORGANIZATION),
                    ticket_id: uuid(0x8004, worker),
                })
                .await
                .map_err(|_| "warm_read")?;
            if !matches!(warm, GetTicketResult::Found(_)) {
                return Err("seed_not_ready");
            }
            ready.wait().await;
            loop {
                let (job, active, next_release) = loop {
                    let notified = admission.changed.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    match admission.take_job(&peak)? {
                        Dispatch::Admitted(job, active, release) => break (job, active, release),
                        Dispatch::Closed => return Ok::<_, &'static str>(()),
                        Dispatch::Pending => notified.await,
                    }
                };
                if next_release.generation != release.generation {
                    client =
                        current::connect_release(next_release.version, &next_release.credential)
                            .await?;
                    release = next_release;
                }
                let retries = match job.operation {
                    Operation::Update(ordinal) => {
                        let (changed, retries, recovering) = super::recovery::run(
                            &mut super::recovery::Update {
                                client: &mut client,
                                input: model::update(ordinal)?,
                            },
                            control.as_deref(),
                        )
                        .await?;
                        model::validate_update(ordinal, &changed.outcome)?;
                        if changed.commit_sequence.is_none() {
                            return Err("missing_commit");
                        }
                        if changed.replayed
                            && !recovering
                            && !super::recovery::planned_fault(control.as_deref())?
                        {
                            return Err("probe_requires_fresh_updates");
                        }
                        if changed.contract_version != release.version {
                            return Err("witness_release");
                        }
                        if let Some(witness) = &witness {
                            witness.record(
                                ordinal,
                                changed.commit_sequence.ok_or("missing_commit")?,
                                changed.contract_version,
                                release.incarnation,
                            )?;
                        }
                        retries
                    }
                    Operation::Read(ordinal) => {
                        let ticket = ordinal % model::POPULATION;
                        let (read, retries, _) = super::recovery::run(
                            &mut super::recovery::ReadTicket {
                                client: &mut client,
                                input: GetTicketParams {
                                    organization_id: uuid(
                                        0x8001,
                                        ticket / TICKETS_PER_ORGANIZATION,
                                    ),
                                    ticket_id: uuid(0x8004, ticket),
                                },
                            },
                            control.as_deref(),
                        )
                        .await?;
                        let GetTicketResult::Found(found) = read else {
                            return Err("state_reconciliation");
                        };
                        model::validate_live(
                            offered_commands.load(Ordering::Acquire),
                            ticket,
                            &found.ticket,
                        )?;
                        retries
                    }
                };
                let mut observation = metrics.lock().map_err(|_| "observation_state")?;
                observation.record(job.window, job.operation, job.offered.elapsed(), retries)?;
                drop(observation);
                drop(active);
            }
        });
    }
    tokio::select! {
        _ = ready.wait() => {},
        result = workers.join_next() => {
            return result.ok_or("worker")?.map_err(|_| "worker")?.and(Err("worker_startup"));
        }
    }
    let clock = OfferedClock::start(shared_clock)?;
    if let Some(origin) = clock.shared_origin_ns {
        println!(
            "{{\"schema\":\"riffdb.ticketdesk-lifecycle-load-ready/v2\",\"workers\":28,\"queue_slots\":32768,\"origin_monotonic_ns\":{origin}}}"
        );
    } else {
        println!(
            "{{\"schema\":\"riffdb.ticketdesk-lifecycle-load-ready/v1\",\"workers\":28,\"queue_slots\":32768}}"
        );
    }
    io::stdout().flush().map_err(|_| "observation_output")?;
    let mut control_tick = tokio::time::interval(Duration::from_millis(100));
    control_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut window = 0_u64;
    for second in 0..duration {
        let tick = clock.wait_for(second);
        tokio::pin!(tick);
        loop {
            tokio::select! {
                result = &mut tick => { result?; break; },
                _ = control_tick.tick(), if control.is_some() => {
                    admission.poll(control.as_ref().ok_or("load_control_root")?, &metrics)?;
                }
                result = workers.join_next() => {
                    return result.ok_or("worker")?.map_err(|_| "worker")?.and(Err("worker_ended"));
                }
            }
        }
        if clock.elapsed()?.as_millis() > u128::from(second) * 1000 + 500 {
            return Err("offered_clock_gap");
        }
        let due = metrics
            .lock()
            .map_err(|_| "observation_state")?
            .due(Instant::now())?;
        if let Some(line) = due {
            publish_line(&line)?;
        }
        if second > 0 && second.is_multiple_of(300) {
            window += 1;
            metrics.lock().map_err(|_| "observation_state")?.rotate(
                Some(Window::new(window, second, (second + 300).min(duration))),
                peak.swap(0, Ordering::AcqRel),
                Instant::now() + WINDOW_SETTLE,
            )?;
            peak.fetch_max(current.load(Ordering::Acquire), Ordering::AcqRel);
        }
        offered_commands.store((second + 1) * 100, Ordering::Release);
        for (offset, operation) in arrival_schedule(second) {
            let arrival = clock.wait_for_offset(second, offset);
            tokio::pin!(arrival);
            loop {
                tokio::select! {
                    result = &mut arrival => { result?; break; },
                    _ = control_tick.tick(), if control.is_some() => {
                        admission.poll(control.as_ref().ok_or("load_control_root")?, &metrics)?;
                    }
                    result = workers.join_next() => {
                        return result.ok_or("worker")?.map_err(|_| "worker")?.and(Err("worker_ended"));
                    }
                }
            }
            admission.offer(Job {
                operation,
                offered: Instant::now(),
                window,
            })?;
        }
    }
    // Close the last observed window at its deadline. Its operations still
    // count toward it when they settle; later completions are disclosed
    // separately and never added to a window.
    clock.wait_for(duration).await?;
    let due = metrics
        .lock()
        .map_err(|_| "observation_state")?
        .due(Instant::now())?;
    if let Some(line) = due {
        publish_line(&line)?;
    }
    metrics.lock().map_err(|_| "observation_state")?.rotate(
        None,
        peak.load(Ordering::Acquire),
        Instant::now() + WINDOW_SETTLE,
    )?;
    admission.close_offers()?;
    tokio::time::timeout(Duration::from_secs(60), async {
        while let Some(result) = workers.join_next().await {
            result.map_err(|_| "worker")??;
        }
        Ok::<_, &'static str>(())
    })
    .await
    .map_err(|_| "drain_bound")??;
    if let Some(witness) = witness {
        witness.finish()?;
    }
    // Every worker has finished, so the last window has settled.
    let last = metrics
        .lock()
        .map_err(|_| "observation_state")?
        .due(Instant::now() + WINDOW_SETTLE)?;
    publish_line(&last.ok_or("window_bound")?)?;
    let (total_commands, total_reads, post_commands, post_reads) = {
        let final_state = metrics.lock().map_err(|_| "observation_state")?;
        (
            final_state.total_commands,
            final_state.total_reads,
            final_state.late_commands,
            final_state.late_reads,
        )
    };
    println!(
        concat!(
            "{{\"schema\":\"riffdb.ticketdesk-lifecycle-load-stopped/v1\",",
            "\"completed_updates\":{},\"completed_reads\":{},",
            "\"post_duration_updates\":{},\"post_duration_reads\":{}}}"
        ),
        total_commands, total_reads, post_commands, post_reads
    );
    io::stdout().flush().map_err(|_| "observation_output")?;
    if terminal {
        super::terminal::hold(
            control.as_deref().ok_or("terminal_configuration")?,
            (*admission.release()?).clone(),
            total_commands,
            total_reads,
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ReadPreparationRoot(PathBuf);
    impl ReadPreparationRoot {
        fn new(tag: &str) -> Self {
            let root = Self(
                std::env::temp_dir()
                    .join(format!("riffdb-preparation-{tag}-{}", std::process::id())),
            );
            std::fs::create_dir(&root.0).unwrap();
            root
        }
    }
    impl Drop for ReadPreparationRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn read_preparation_admission() -> Admission {
        Admission::new(
            Release {
                generation: 1,
                version: 1,
                incarnation: 1,
                credential: "application.credential".into(),
            },
            Arc::new(AtomicU32::new(0)),
        )
    }

    #[test]
    // req: END-007, END-008, END-010
    fn ordinary_offers_keep_fifo_window_and_original_offer_time() {
        let admission = read_preparation_admission();
        let peak = AtomicU32::new(0);
        let offered = Instant::now();
        let operations = [
            Operation::Update(0),
            Operation::Read(0),
            Operation::Read(1),
            Operation::Update(1),
        ];
        for operation in operations {
            admission
                .offer(Job {
                    operation,
                    offered,
                    window: 7,
                })
                .unwrap();
        }
        admission.close_offers().unwrap();
        assert!(
            admission
                .offer(Job {
                    operation: Operation::Read(9),
                    offered,
                    window: 7
                })
                .is_err()
        );
        for expected in operations {
            let Dispatch::Admitted(job, active, release) = admission.take_job(&peak).unwrap()
            else {
                panic!("offered job must remain available");
            };
            assert_eq!(job.operation, expected);
            assert_eq!(job.offered, offered);
            assert_eq!(job.window, 7);
            assert_eq!(release.generation, 1);
            drop(active);
        }
        assert!(matches!(
            admission.take_job(&peak).unwrap(),
            Dispatch::Closed
        ));
    }

    #[test]
    // req: END-007, END-008, END-010
    fn preparation_reads_bypass_fenced_writes_under_one_unchanged_queue_bound() {
        let root = ReadPreparationRoot::new("queue-bound");
        let admission = read_preparation_admission();
        let metrics = Mutex::new(Metrics::new(300));
        let peak = AtomicU32::new(0);
        std::fs::write(root.0.join("load.quiesce"), b"pause\n").unwrap();
        admission.poll(&root.0, &metrics).unwrap();
        let offered = Instant::now();
        for ordinal in 0..QUEUE - 1 {
            admission
                .offer(Job {
                    operation: Operation::Update(ordinal as u64),
                    offered,
                    window: 0,
                })
                .unwrap();
        }
        admission
            .offer(Job {
                operation: Operation::Read(17),
                offered,
                window: 0,
            })
            .unwrap();
        assert!(
            admission
                .offer(Job {
                    operation: Operation::Read(18),
                    offered,
                    window: 0
                })
                .is_err()
        );
        std::fs::write(root.0.join("load.restore-reads"), b"1 1 1\n").unwrap();
        admission.poll(&root.0, &metrics).unwrap();
        let Dispatch::Admitted(job, active, _) = admission.take_job(&peak).unwrap() else {
            panic!("a fenced write must not occupy every worker ahead of reads");
        };
        assert_eq!(job.operation, Operation::Read(17));
        assert_eq!(job.offered, offered);
        assert_eq!(admission.state.lock().unwrap().queue.len(), QUEUE - 1);
        drop(active);
        assert!(matches!(
            admission.take_job(&peak).unwrap(),
            Dispatch::Pending
        ));
        admission
            .offer(Job {
                operation: Operation::Read(18),
                offered,
                window: 0,
            })
            .unwrap();
        assert!(
            admission
                .offer(Job {
                    operation: Operation::Read(19),
                    offered,
                    window: 0
                })
                .is_err()
        );
        assert!(
            !root.0.join("load.restore-reads-active").exists(),
            "a control marker alone cannot claim a completed public read"
        );
    }

    #[test]
    // req: END-007, END-008, END-010
    fn preparation_read_progress_and_full_pause_require_actual_completed_admissions() {
        let root = ReadPreparationRoot::new("phase-handover");
        let admission = read_preparation_admission();
        let metrics = Mutex::new(Metrics::new(300));
        let peak = AtomicU32::new(0);
        std::fs::write(root.0.join("load.quiesce"), b"pause\n").unwrap();
        admission.poll(&root.0, &metrics).unwrap();
        std::fs::write(root.0.join("load.restore-reads"), b"1 1 1\n").unwrap();
        admission.poll(&root.0, &metrics).unwrap();
        let offered = Instant::now();
        admission
            .offer(Job {
                operation: Operation::Update(0),
                offered,
                window: 0,
            })
            .unwrap();
        admission
            .offer(Job {
                operation: Operation::Read(0),
                offered,
                window: 0,
            })
            .unwrap();
        let Dispatch::Admitted(job, active, _) = admission.take_job(&peak).unwrap() else {
            panic!("read");
        };
        assert_eq!(job.operation, Operation::Read(0));
        metrics
            .lock()
            .unwrap()
            .record(0, job.operation, Duration::ZERO, 0)
            .unwrap();
        admission.poll(&root.0, &metrics).unwrap();
        assert!(root.0.join("load.restore-reads-active").is_file());
        std::fs::remove_file(root.0.join("load.restore-reads")).unwrap();
        admission.poll(&root.0, &metrics).unwrap();
        assert!(!root.0.join("load.restore-readers-quiesced").exists());
        assert!(matches!(
            admission.take_job(&peak).unwrap(),
            Dispatch::Pending
        ));
        drop(active);
        admission.poll(&root.0, &metrics).unwrap();
        assert!(root.0.join("load.restore-readers-quiesced").is_file());
        std::fs::remove_file(root.0.join("load.quiesce")).unwrap();
        assert_eq!(
            admission.poll(&root.0, &metrics),
            Err("restore_read_unfenced_release")
        );
        std::fs::write(root.0.join("load.quiesce"), b"pause\n").unwrap();
        std::fs::write(root.0.join("next.credential"), b"private fixture\n").unwrap();
        std::fs::write(root.0.join("load.phase"), b"2 1 2 next.credential\n").unwrap();
        admission.poll(&root.0, &metrics).unwrap();
        std::fs::remove_file(root.0.join("load.quiesce")).unwrap();
        admission.poll(&root.0, &metrics).unwrap();
        let Dispatch::Admitted(job, active, release) = admission.take_job(&peak).unwrap() else {
            panic!("write");
        };
        assert_eq!(job.operation, Operation::Update(0));
        assert_eq!(release.generation, 2);
        assert_eq!(release.incarnation, 2);
        drop(active);
    }

    #[tokio::test]
    // req: END-007, END-008, END-010
    async fn queued_read_and_mode_change_between_registration_and_wait_keep_the_wakeup() {
        let root = ReadPreparationRoot::new("read-notification");
        let admission = Arc::new(read_preparation_admission());
        let metrics = Mutex::new(Metrics::new(300));
        std::fs::write(root.0.join("load.quiesce"), b"pause\n").unwrap();
        admission.poll(&root.0, &metrics).unwrap();
        let (registered, registration) = tokio::sync::oneshot::channel();
        let (continue_wait, continuation) = tokio::sync::oneshot::channel();
        let owned = Arc::clone(&admission);
        let worker = tokio::spawn(async move {
            let peak = AtomicU32::new(0);
            let notified = owned.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            assert!(matches!(owned.take_job(&peak).unwrap(), Dispatch::Pending));
            registered.send(()).unwrap();
            continuation.await.unwrap();
            notified.await;
            let Dispatch::Admitted(job, active, _) = owned.take_job(&peak).unwrap() else {
                panic!("registered read must remain dispatched");
            };
            assert_eq!(job.operation, Operation::Read(9));
            drop(active);
        });
        registration.await.unwrap();
        admission
            .offer(Job {
                operation: Operation::Update(0),
                offered: Instant::now(),
                window: 0,
            })
            .unwrap();
        admission
            .offer(Job {
                operation: Operation::Read(9),
                offered: Instant::now(),
                window: 0,
            })
            .unwrap();
        std::fs::write(root.0.join("load.restore-reads"), b"1 1 1\n").unwrap();
        admission.poll(&root.0, &metrics).unwrap();
        continue_wait.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(admission.current.load(Ordering::Acquire), 0);
        assert_eq!(admission.state.lock().unwrap().queue.len(), 1);
    }

    // req: END-007, END-008, END-010
    #[test]
    fn shared_clock_configuration_and_monotonic_values_are_closed() {
        assert_eq!(shared_clock_requested(None), Ok(false));
        assert_eq!(shared_clock_requested(Some("1")), Ok(true));
        for value in ["", "0", "true", " 1", "2"] {
            assert_eq!(
                shared_clock_requested(Some(value)),
                Err("offered_clock_configuration")
            );
        }
        assert_eq!(monotonic_parts(2, 3), Ok(2_000_000_003));
        for (seconds, nanos) in [(-1, 0), (0, -1), (1, 1_000_000_000), (0, 0), (i64::MAX, 0)] {
            assert_eq!(monotonic_parts(seconds, nanos), Err("offered_clock_value"));
        }
    }

    // req: END-007, END-008, END-010
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn shared_clock_publishes_the_same_native_epoch_and_rejects_rewind() {
        let before = monotonic_ns().unwrap();
        let mut clock = OfferedClock::start(true).unwrap();
        let origin = clock.shared_origin_ns.unwrap();
        let after = monotonic_ns().unwrap();
        assert!(before <= origin && origin <= after);
        assert!(
            clock.elapsed().unwrap().as_nanos() <= u128::from(monotonic_ns().unwrap() - origin)
        );
        clock.wait_for(0).await.unwrap();
        clock.wait_for_offset(0, 0).await.unwrap();
        assert_eq!(
            clock.wait_for_offset(0, 1_000_000).await,
            Err("offered_clock_value")
        );
        clock.shared_origin_ns = Some(u64::MAX);
        assert_eq!(clock.elapsed(), Err("offered_clock_rewind"));
        assert_eq!(
            clock.wait_for_offset(0, 0).await,
            Err("offered_clock_rewind")
        );
    }

    // req: END-003, END-007, END-008, END-010
    #[test]
    fn release_confirmation_requires_a_new_acknowledged_command_and_is_published_once() {
        struct Directory(PathBuf);
        impl Drop for Directory {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let root = Directory(
            std::env::temp_dir().join(format!("riffdb-load-resume-test-{}", std::process::id())),
        );
        std::fs::create_dir(&root.0).unwrap();
        std::fs::write(root.0.join("next.credential"), b"private fixture\n").unwrap();
        let metrics = Mutex::new(Metrics::new(300));
        metrics
            .lock()
            .unwrap()
            .record(0, Operation::Update(0), Duration::ZERO, 0)
            .unwrap();
        let admission = Admission::new(
            Release {
                generation: 1,
                version: 1,
                incarnation: 1,
                credential: root.0.join("initial.credential"),
            },
            Arc::new(AtomicU32::new(0)),
        );
        std::fs::write(root.0.join("load.quiesce"), b"pause\n").unwrap();
        admission.poll(&root.0, &metrics).unwrap();
        let original_ack = std::fs::read(root.0.join("load.quiesced")).unwrap();
        std::fs::write(root.0.join("load.phase"), b"2 2 2 next.credential\n").unwrap();
        admission.poll(&root.0, &metrics).unwrap();
        assert!(!root.0.join("load.resumed").exists());
        std::fs::remove_file(root.0.join("load.quiesce")).unwrap();
        admission.poll(&root.0, &metrics).unwrap();
        assert!(!root.0.join("load.resumed").exists());
        metrics
            .lock()
            .unwrap()
            .record(0, Operation::Read(0), Duration::ZERO, 0)
            .unwrap();
        admission.poll(&root.0, &metrics).unwrap();
        assert!(!root.0.join("load.resumed").exists());
        let (active, release) = admission.admit(&AtomicU32::new(0)).unwrap().unwrap();
        assert_eq!(
            (release.generation, release.version, release.incarnation),
            (2, 2, 2)
        );
        metrics
            .lock()
            .unwrap()
            .record(0, Operation::Update(1), Duration::ZERO, 0)
            .unwrap();
        drop(active);
        admission.poll(&root.0, &metrics).unwrap();
        let confirmed = std::fs::read(root.0.join("load.resumed"))
            .expect("new acknowledged command must publish native resume confirmation");
        assert_eq!(
            confirmed,
            concat!(
                "{\"schema\":\"riffdb.ticketdesk-lifecycle-resumed/v1\",",
                "\"generation\":2,\"contract_version\":2,\"history_incarnation\":2,",
                "\"completed_commands\":2,\"completed_reads\":1}\n"
            )
            .as_bytes()
        );
        admission.poll(&root.0, &metrics).unwrap();
        assert_eq!(
            std::fs::read(root.0.join("load.resumed")).unwrap(),
            confirmed
        );
        assert_eq!(
            std::fs::read(root.0.join("load.quiesced")).unwrap(),
            original_ack
        );
        // A subsequent handover cannot overwrite evidence that the controller
        // has not consumed; generation advancement alone is insufficient.
        std::fs::write(root.0.join("load.quiesce"), b"pause\n").unwrap();
        std::fs::write(root.0.join("load.phase"), b"3 2 2 next.credential\n").unwrap();
        assert!(admission.poll(&root.0, &metrics).is_err());
        assert_eq!(admission.release().unwrap().generation, 2);
    }

    // req: END-002, END-003, END-007, END-008
    #[test]
    fn quiescence_requires_every_original_admission_to_settle_before_release_change() {
        let current = Arc::new(AtomicU32::new(0));
        let peak = AtomicU32::new(0);
        let admission = Admission::new(
            Release {
                generation: 1,
                version: 1,
                incarnation: 1,
                credential: "application.credential".into(),
            },
            Arc::clone(&current),
        );
        let (pin, release) = admission.admit(&peak).unwrap().unwrap();
        assert_eq!(release.version, 1);
        admission.pause().unwrap();
        assert!(admission.admit(&peak).unwrap().is_none());
        assert!(!admission.quiesced().unwrap());
        let successor = Release {
            generation: 2,
            version: 2,
            incarnation: 1,
            credential: "application-v2.credential".into(),
        };
        assert!(admission.replace(successor.clone()).is_err());
        drop(pin);
        assert!(admission.quiesced().unwrap());
        admission.replace(successor).unwrap();
        admission.resume().unwrap();
        let (pin, release) = admission.admit(&peak).unwrap().unwrap();
        assert_eq!(release.generation, 2);
        assert_eq!(release.version, 2);
        drop(pin);
        assert_eq!(current.load(Ordering::Acquire), 0);
    }

    // req: END-002, END-007, END-008
    #[tokio::test]
    async fn resumption_between_registration_and_wait_does_not_lose_an_admission_wakeup() {
        let admission = Arc::new(Admission::new(
            Release {
                generation: 1,
                version: 1,
                incarnation: 1,
                credential: "application.credential".into(),
            },
            Arc::new(AtomicU32::new(0)),
        ));
        admission.pause().unwrap();
        let (registered, ready) = tokio::sync::oneshot::channel();
        let (continue_wait, wait) = tokio::sync::oneshot::channel();
        let owned = Arc::clone(&admission);
        let worker = tokio::spawn(async move {
            let notified = owned.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            assert!(owned.admit(&AtomicU32::new(0)).unwrap().is_none());
            registered.send(()).unwrap();
            wait.await.unwrap();
            notified.await;
            let (pin, _) = owned.admit(&AtomicU32::new(0)).unwrap().unwrap();
            drop(pin);
        });
        ready.await.unwrap();
        admission.resume().unwrap();
        continue_wait.send(()).unwrap();
        worker.await.unwrap();
        assert_eq!(admission.current.load(Ordering::Acquire), 0);
    }

    // req: END-002, END-003, END-008
    #[test]
    fn an_unpaused_or_repeated_or_older_release_cannot_replace_current_carriage() {
        let admission = Admission::new(
            Release {
                generation: 1,
                version: 2,
                incarnation: 1,
                credential: "application-v2.credential".into(),
            },
            Arc::new(AtomicU32::new(0)),
        );
        let next = Release {
            generation: 2,
            version: 2,
            incarnation: 1,
            credential: "rotated.credential".into(),
        };
        assert!(admission.replace(next.clone()).is_err());
        admission.pause().unwrap();
        assert!(
            admission
                .replace(Release {
                    version: 1,
                    ..next.clone()
                })
                .is_err()
        );
        assert!(
            admission
                .replace(Release {
                    generation: 1,
                    ..next.clone()
                })
                .is_err()
        );
        admission.replace(next.clone()).unwrap();
        assert!(admission.replace(next).is_err());
        assert_eq!(admission.release().unwrap().version, 2);
        assert_eq!(admission.release().unwrap().generation, 2);
    }

    // req: END-004, END-007, END-010
    #[tokio::test]
    async fn cancellation_releases_the_admitted_attempt_before_reusing_its_slot() {
        let current = Arc::new(AtomicU32::new(0));
        let peak = Arc::new(AtomicU32::new(0));
        let (admitted, observed) = tokio::sync::oneshot::channel();
        let owned = Arc::clone(&current);
        let peak_owned = Arc::clone(&peak);
        let task = tokio::spawn(async move {
            let _pin = Active::reserve(&owned, &peak_owned).unwrap();
            admitted.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        observed.await.unwrap();
        assert_eq!(current.load(Ordering::Acquire), 1);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(current.load(Ordering::Acquire), 0);
        let reused = Active::reserve(&current, &peak).unwrap();
        assert_eq!(current.load(Ordering::Acquire), 1);
        drop(reused);
        assert_eq!(current.load(Ordering::Acquire), 0);
    }

    // req: END-004, END-007, END-010
    #[test]
    fn protected_load_saturation_keeps_four_auxiliary_slots_unavailable_to_load() {
        let current = Arc::new(AtomicU32::new(0));
        let peak = AtomicU32::new(0);
        let pins = (0..WORKERS)
            .map(|_| Active::reserve(&current, &peak).unwrap())
            .collect::<Vec<_>>();
        assert!(matches!(
            Active::reserve(&current, &peak),
            Err("in_flight_bound")
        ));
        assert_eq!(current.load(Ordering::Acquire), 28);
        assert_eq!(peak.load(Ordering::Acquire), 28);
        drop(pins);
        assert_eq!(current.load(Ordering::Acquire), 0);
    }

    // req: END-004, END-007
    #[test]
    fn overflow_latency_cannot_report_a_fast_tail() {
        let mut window = Window::new(0, 0, 300);
        window
            .record(Operation::Update(0), Duration::from_secs(1), 0)
            .unwrap();
        assert_eq!(window.quantile(95), u64::MAX);
        assert_eq!(window.quantile(99), u64::MAX);
    }

    // req: END-007
    #[test]
    fn each_second_offers_exactly_its_operations_spread_across_the_second() {
        for second in [0_u64, 1, 299, 86_399] {
            let schedule = arrival_schedule(second);
            assert_eq!(schedule.len(), 500);
            assert!(schedule.windows(2).all(|pair| pair[0].0 <= pair[1].0));
            assert!(schedule.iter().all(|(offset, _)| *offset < 1_000_000));
            let mut updates = schedule
                .iter()
                .filter_map(|(_, operation)| match operation {
                    Operation::Update(ordinal) => Some(*ordinal),
                    Operation::Read(_) => None,
                })
                .collect::<Vec<_>>();
            let mut reads = schedule
                .iter()
                .filter_map(|(_, operation)| match operation {
                    Operation::Read(ordinal) => Some(*ordinal),
                    Operation::Update(_) => None,
                })
                .collect::<Vec<_>>();
            updates.sort_unstable();
            reads.sort_unstable();
            assert_eq!(
                updates,
                (second * 100..second * 100 + 100).collect::<Vec<_>>()
            );
            assert_eq!(
                reads,
                (second * 400..second * 400 + 400).collect::<Vec<_>>()
            );
            // Arrivals cover the whole second rather than one instant: every
            // tenth of it holds some offers and none holds most of them.
            let mut tenths = [0_u32; 10];
            for (offset, _) in &schedule {
                tenths[(*offset / 100_000) as usize] += 1;
            }
            assert!(
                tenths.iter().all(|count| (10..=100).contains(count)),
                "{tenths:?}"
            );
            // The same second always offers the same schedule.
            let again = arrival_schedule(second);
            assert!(
                schedule
                    .iter()
                    .zip(&again)
                    .all(|(a, b)| a.0 == b.0 && same_operation(a.1, b.1))
            );
        }
        assert!(
            arrival_schedule(1)
                .iter()
                .zip(arrival_schedule(2).iter())
                .any(|(a, b)| a.0 != b.0)
        );
    }

    fn same_operation(a: Operation, b: Operation) -> bool {
        match (a, b) {
            (Operation::Update(a), Operation::Update(b))
            | (Operation::Read(a), Operation::Read(b)) => a == b,
            _ => false,
        }
    }

    // req: END-007
    #[test]
    fn operations_count_toward_the_window_they_were_offered_in() {
        let start = Instant::now();
        let mut metrics = Metrics::new(1);
        metrics
            .rotate(
                Some(Window::new(1, 1, 2)),
                3,
                start + Duration::from_secs(60),
            )
            .unwrap();
        // A first-window update completing after the rotation still counts
        // toward the first window, which publishes once all its work settles.
        for ordinal in 0..99 {
            metrics
                .record(0, Operation::Update(ordinal), Duration::from_millis(5), 0)
                .unwrap();
        }
        for ordinal in 0..400 {
            metrics
                .record(0, Operation::Read(ordinal), Duration::from_millis(1), 0)
                .unwrap();
        }
        assert!(
            metrics.due(start).unwrap().is_none(),
            "one update is still out"
        );
        metrics
            .record(0, Operation::Update(99), Duration::from_millis(5), 0)
            .unwrap();
        let line = metrics.due(start).unwrap().unwrap();
        assert!(line.contains("\"start_s\":0,\"end_s\":1,"));
        assert!(line.contains("\"completed_commands\":100,\"completed_reads\":400,"));
        assert!(line.contains("\"load_peak_in_flight\":3,"));
        // Second-window work goes to the open window; anything for a window
        // already published is disclosed as late rather than counted.
        metrics
            .record(1, Operation::Read(400), Duration::from_millis(1), 0)
            .unwrap();
        metrics
            .record(0, Operation::Read(0), Duration::from_millis(1), 0)
            .unwrap();
        assert_eq!(metrics.open.reads, 1);
        assert_eq!(metrics.late_reads, 1);
        // An unsettled window publishes at its deadline with what completed.
        metrics
            .rotate(None, 1, start + Duration::from_secs(60))
            .unwrap();
        assert!(metrics.due(start).unwrap().is_none());
        let line = metrics
            .due(start + Duration::from_secs(60))
            .unwrap()
            .unwrap();
        assert!(line.contains("\"completed_commands\":0,\"completed_reads\":1,"));
    }
}
