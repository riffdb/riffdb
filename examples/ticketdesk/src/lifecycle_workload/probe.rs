//! Bounded offered-load probe; lifecycle orchestration remains controller-owned.

use std::env;
use std::io::{self, Write};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{Barrier, mpsc};
use tokio::task::JoinSet;

use super::application::{GetTicketParams, GetTicketResult};
use super::{Result, TICKETS_PER_ORGANIZATION, connect, model, uuid};

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
                "\"settled_submission_attempts\":{},\"retries\":0,",
                "\"p95_us\":{},\"p99_us\":{},\"load_peak_in_flight\":{},",
                "\"reserved_auxiliary_slots\":4,\"latency_overflow\":{}}}"
            ),
            start,
            end,
            duration * 100,
            duration * 400,
            self.commands,
            self.reads,
            self.commands + self.reads,
            self.quantile(95),
            self.quantile(99),
            peak,
            self.overflow
        );
        io::stdout().flush().map_err(|_| "observation_output")?;
        self.commands = 0;
        self.reads = 0;
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
    let offered_commands = Arc::new(AtomicU64::new(0));
    let mut workers = JoinSet::new();
    for worker in 0..u64::from(WORKERS) {
        let receiver = Arc::clone(&receiver);
        let ready = Arc::clone(&ready);
        let metrics = Arc::clone(&metrics);
        let current = Arc::clone(&current);
        let peak = Arc::clone(&peak);
        let offered_commands = Arc::clone(&offered_commands);
        workers.spawn(async move {
            let mut client = connect("RIFFDB_LIFECYCLE_APPLICATION_CREDENTIAL").await?;
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
                let active = Active::reserve(&current, &peak)?;
                match job.operation {
                    Operation::Update(ordinal) => {
                        let changed = client
                            .update_ticket(model::update(ordinal)?)
                            .await
                            .map_err(|_| "load_command")?;
                        model::validate_update(ordinal, &changed.outcome)?;
                        if changed.commit_sequence.is_none() {
                            return Err("missing_commit");
                        }
                        if changed.replayed {
                            return Err("probe_requires_fresh_updates");
                        }
                    }
                    Operation::Read(ordinal) => {
                        let ticket = ordinal % model::POPULATION;
                        let read = client
                            .get_ticket(GetTicketParams {
                                organization_id: uuid(0x8001, ticket / TICKETS_PER_ORGANIZATION),
                                ticket_id: uuid(0x8004, ticket),
                            })
                            .await
                            .map_err(|_| "load_read")?;
                        let GetTicketResult::Found(found) = read else {
                            return Err("state_reconciliation");
                        };
                        model::validate_live(
                            offered_commands.load(Ordering::Acquire),
                            ticket,
                            &found.ticket,
                        )?;
                    }
                }
                drop(active);
                metrics
                    .lock()
                    .map_err(|_| "observation_state")?
                    .record(job.operation, job.offered.elapsed())?;
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
    let mut window_start = 0;
    for second in 0..duration {
        tokio::select! {
            _ = ticker.tick() => {},
            result = workers.join_next() => {
                return result.ok_or("worker")?.map_err(|_| "worker")?.and(Err("worker_ended"));
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
