//! Bounded offered-load probe; lifecycle orchestration remains controller-owned.

use std::env;
use std::io::Read;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{Barrier, mpsc};
use tokio::task::JoinSet;

use super::application::{GetTicketParams, GetTicketResult};
use super::{Result, TICKETS_PER_ORGANIZATION, current, model, uuid};

const WORKERS: u32 = 28;
const QUEUE: usize = 32_768;
const LATENCY_CEILING: usize = 250_000;
const MAX_WINDOW_SAMPLES: u64 = 200_000;

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
                release: Arc::new(release),
                resume_after_commands: None,
            }),
            current,
            changed: tokio::sync::Notify::new(),
        }
    }

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
        self.state.lock().map_err(|_| "admission_state")?.paused = true;
        Ok(())
    }

    fn quiesced(&self) -> Result<bool> {
        let state = self.state.lock().map_err(|_| "admission_state")?;
        Ok(state.paused && self.current.load(Ordering::Acquire) == 0)
    }

    #[cfg(test)]
    fn replace(&self, release: Release) -> Result<()> {
        self.replace_at(release, None)
    }

    fn replace_at(&self, release: Release, resume_after_commands: Option<u64>) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| "admission_state")?;
        if !state.paused
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
        self.state.lock().map_err(|_| "admission_state")?.paused = false;
        self.changed.notify_waiters();
        Ok(())
    }

    fn poll(&self, root: &Path, metrics: &Mutex<Metrics>) -> Result<()> {
        let paused = root.join("load.quiesce").exists();
        if paused {
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
    if !matches!(name, "load.quiesced" | "observer.quiesced" | "load.resumed")
        || encoded.len() > 4096
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

#[derive(Clone, Copy)]
enum Operation {
    Update(u64),
    Read(u64),
}

struct Job {
    operation: Operation,
    offered: Instant,
}

struct Metrics {
    commands: u64,
    reads: u64,
    retries: u64,
    total_commands: u64,
    total_reads: u64,
    latencies: Vec<u32>,
    overflow: u64,
}

impl Metrics {
    fn new() -> Self {
        Self {
            commands: 0,
            reads: 0,
            retries: 0,
            total_commands: 0,
            total_reads: 0,
            latencies: vec![0; LATENCY_CEILING + 1],
            overflow: 0,
        }
    }

    fn record(&mut self, operation: Operation, latency: Duration) -> Result<()> {
        if self.commands + self.reads >= MAX_WINDOW_SAMPLES {
            return Err("sample_bound");
        }
        match operation {
            Operation::Update(_) => {
                self.commands += 1;
                self.total_commands += 1;
            }
            Operation::Read(_) => {
                self.reads += 1;
                self.total_reads += 1;
            }
        }
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

    fn publish(&mut self, start: u64, end: u64, peak: u32) -> Result<()> {
        let duration = end.checked_sub(start).ok_or("window_bound")?;
        if duration == 0 {
            return Err("window_bound");
        }
        println!(
            concat!(
                "{{\"schema\":\"riffdb.ticketdesk-lifecycle-load-probe/v1\",",
                "\"start_s\":{},\"end_s\":{},\"offered_commands\":{},\"offered_reads\":{},",
                "\"completed_commands\":{},\"completed_reads\":{},",
                "\"settled_submission_attempts\":{},\"transport_attempts\":{},\"retries\":{},",
                "\"p95_us\":{},\"p99_us\":{},\"load_peak_in_flight\":{},",
                "\"reserved_auxiliary_slots\":4,\"latency_overflow\":{}}}"
            ),
            start,
            end,
            duration * 100,
            duration * 400,
            self.commands,
            self.reads,
            self.commands + self.reads + self.retries,
            self.commands + self.reads + self.retries,
            self.retries,
            self.quantile(95),
            self.quantile(99),
            peak,
            self.overflow
        );
        io::stdout().flush().map_err(|_| "observation_output")?;
        self.commands = 0;
        self.reads = 0;
        self.retries = 0;
        self.latencies.fill(0);
        self.overflow = 0;
        Ok(())
    }
}

pub(super) async fn run() -> Result<()> {
    let duration = env::var("RIFFDB_LIFECYCLE_DURATION_SECONDS")
        .map_err(|_| "configuration")?
        .parse::<u64>()
        .map_err(|_| "configuration")?;
    if !(1..=86_400).contains(&duration) {
        return Err("duration_bound");
    }
    let (sender, receiver) = mpsc::channel::<Job>(QUEUE);
    let receiver = Arc::new(tokio::sync::Mutex::new(receiver));
    let ready = Arc::new(Barrier::new(WORKERS as usize + 1));
    let metrics = Arc::new(Mutex::new(Metrics::new()));
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
    let offered_commands = Arc::new(AtomicU64::new(0));
    let mut workers = JoinSet::new();
    for worker in 0..u64::from(WORKERS) {
        let receiver = Arc::clone(&receiver);
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
                let job = { receiver.lock().await.recv().await };
                let Some(job) = job else {
                    return Ok::<_, &'static str>(());
                };
                let (active, next_release) = loop {
                    let notified = admission.changed.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    if let Some(admitted) = admission.admit(&peak)? {
                        break admitted;
                    }
                    notified.await;
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
                observation.record(job.operation, job.offered.elapsed())?;
                observation.retries = observation
                    .retries
                    .checked_add(u64::from(retries))
                    .ok_or("sample_bound")?;
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
    println!(
        "{{\"schema\":\"riffdb.ticketdesk-lifecycle-load-ready/v1\",\"workers\":28,\"queue_slots\":32768}}"
    );
    io::stdout().flush().map_err(|_| "observation_output")?;
    let started = Instant::now();
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    let mut control_tick = tokio::time::interval(Duration::from_millis(100));
    control_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut window_start = 0;
    for second in 0..duration {
        loop {
            tokio::select! {
                _ = ticker.tick() => break,
                _ = control_tick.tick(), if control.is_some() => {
                    admission.poll(control.as_ref().ok_or("load_control_root")?, &metrics)?;
                }
                result = workers.join_next() => {
                    return result.ok_or("worker")?.map_err(|_| "worker")?.and(Err("worker_ended"));
                }
            }
        }
        if started.elapsed().as_millis() > u128::from(second) * 1000 + 500 {
            return Err("offered_clock_gap");
        }
        if second > 0 && second.is_multiple_of(300) {
            metrics.lock().map_err(|_| "observation_state")?.publish(
                window_start,
                second,
                peak.swap(0, Ordering::AcqRel),
            )?;
            peak.fetch_max(current.load(Ordering::Acquire), Ordering::AcqRel);
            window_start = second;
        }
        offered_commands.store((second + 1) * 100, Ordering::Release);
        let offered = Instant::now();
        for command in 0..100 {
            sender
                .try_send(Job {
                    operation: Operation::Update(second * 100 + command),
                    offered,
                })
                .map_err(|_| "offered_queue_bound")?;
            for read in 0..4 {
                sender
                    .try_send(Job {
                        operation: Operation::Read(second * 400 + command * 4 + read),
                        offered,
                    })
                    .map_err(|_| "offered_queue_bound")?;
            }
        }
    }
    // Close the last observed window at its deadline. Later drain completions
    // are disclosed separately and never added to a normal-window rate.
    ticker.tick().await;
    metrics.lock().map_err(|_| "observation_state")?.publish(
        window_start,
        duration,
        peak.load(Ordering::Acquire),
    )?;
    drop(sender);
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
    let final_state = metrics.lock().map_err(|_| "observation_state")?;
    println!(
        concat!(
            "{{\"schema\":\"riffdb.ticketdesk-lifecycle-load-stopped/v1\",",
            "\"completed_updates\":{},\"completed_reads\":{},",
            "\"post_duration_updates\":{},\"post_duration_reads\":{}}}"
        ),
        final_state.total_commands,
        final_state.total_reads,
        final_state.commands,
        final_state.reads
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let metrics = Mutex::new(Metrics::new());
        metrics
            .lock()
            .unwrap()
            .record(Operation::Update(0), Duration::ZERO)
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
            .record(Operation::Read(0), Duration::ZERO)
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
            .record(Operation::Update(1), Duration::ZERO)
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
        let mut metrics = Metrics::new();
        metrics
            .record(Operation::Update(0), Duration::from_secs(1))
            .unwrap();
        assert_eq!(metrics.quantile(95), u64::MAX);
        assert_eq!(metrics.quantile(99), u64::MAX);
    }
}
