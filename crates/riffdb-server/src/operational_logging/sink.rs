//! Bounded output adapter. No worker failure or shutdown path writes to stdout.

use riffdb_observability::{LogDestination, LogObservation, process_logging_metrics};
use std::collections::VecDeque;
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use tracing::Level;

const LEVELS: [Level; 5] = [
    Level::ERROR,
    Level::WARN,
    Level::INFO,
    Level::DEBUG,
    Level::TRACE,
];
fn level_index(level: Level) -> usize {
    LEVELS.iter().position(|v| *v == level).unwrap_or(0)
}
struct QueuedRecord {
    bytes: Vec<u8>,
    level: Level,
}

#[derive(Default)]
pub(super) struct SinkCounters {
    pub(super) enqueued: AtomicU64,
    pub(super) written: AtomicU64,
    pub(super) dropped: AtomicU64,
    pub(super) failed: AtomicU64,
    pub(super) unflushed: AtomicU64,
}

struct State {
    queue: Mutex<VecDeque<QueuedRecord>>,
    destination: LogDestination,
    pending: [AtomicU64; 5],
    closing: AtomicBool,
    capacity: usize,
    reserve: usize,
    counters: SinkCounters,
}

#[derive(Clone)]
pub(super) struct NonBlocking {
    state: Arc<State>,
    wake: mpsc::SyncSender<()>,
}

impl NonBlocking {
    pub(super) fn enqueue(&self, bytes: &[u8], level: Level) -> io::Result<()> {
        let high_priority = matches!(level, Level::ERROR | Level::WARN);
        let limit = if high_priority {
            self.state.capacity
        } else {
            self.state.capacity - self.state.reserve
        };
        let mut queue = match self.state.queue.lock() {
            Ok(queue) => queue,
            Err(_) => return self.refuse(level, LogObservation::QueueUnavailable),
        };
        if self.state.closing.load(Ordering::Acquire) {
            return self.refuse(level, LogObservation::Closing);
        }
        if bytes.len() > super::events::MAX_RECORD_BYTES {
            return self.refuse(level, LogObservation::RecordLimit);
        }
        if queue.len() >= limit {
            return self.refuse(level, LogObservation::QueueFull);
        }
        queue.push_back(QueuedRecord {
            bytes: bytes.to_vec(),
            level,
        });
        self.state.pending[level_index(level)].fetch_add(1, Ordering::Relaxed);
        super::increment(&self.state.counters.enqueued);
        process_logging_metrics().record(
            self.state.destination,
            level,
            LogObservation::Enqueued,
            1,
        );
        process_logging_metrics().set_depth(self.state.destination, queue.len());
        drop(queue);
        let _ = self.wake.try_send(());
        Ok(())
    }
    fn refuse(&self, level: Level, reason: LogObservation) -> io::Result<()> {
        super::increment(&self.state.counters.dropped);
        process_logging_metrics().record(self.state.destination, level, reason, 1);
        Err(io::ErrorKind::WouldBlock.into())
    }
}

pub(super) struct NonBlockingBuilder {
    capacity: usize,
    name: &'static str,
    destination: LogDestination,
}

impl Default for NonBlockingBuilder {
    fn default() -> Self {
        Self {
            capacity: 4096,
            destination: LogDestination::Stderr,
            name: "riffdb-log-writer",
        }
    }
}

impl NonBlockingBuilder {
    pub(super) fn destination(mut self, destination: LogDestination) -> Self {
        self.destination = destination;
        self
    }

    pub(super) fn buffered_lines_limit(mut self, capacity: usize) -> Self {
        self.capacity = capacity;
        self
    }
    pub(super) fn thread_name(mut self, name: &'static str) -> Self {
        self.name = name;
        self
    }
    pub(super) fn finish<W: Write + Send + 'static>(
        self,
        mut output: W,
    ) -> io::Result<(NonBlocking, WorkerGuard)> {
        if !(1..=4096).contains(&self.capacity) {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let state = Arc::new(State {
            queue: Mutex::new(VecDeque::with_capacity(self.capacity)),
            closing: AtomicBool::new(false),
            destination: self.destination,
            pending: [const { AtomicU64::new(0) }; 5],
            capacity: self.capacity,
            reserve: 64.min(self.capacity / 4),
            counters: SinkCounters::default(),
        });
        let (wake, receiver) = mpsc::sync_channel(1);
        let (done, completed) = mpsc::sync_channel(1);
        let worker_state = state.clone();
        let task = thread::Builder::new()
            .name(self.name.to_owned())
            .stack_size(crate::PRODUCTION_THREAD_STACK_BYTES)
            .spawn(move || {
                let mut failed = false;
                let mut reported: Option<Instant> = None;
                loop {
                    let (next, closing) = match worker_state.queue.lock() {
                        Ok(mut queue) => {
                            let next = queue.pop_front();
                            process_logging_metrics()
                                .set_depth(worker_state.destination, queue.len());
                            (next, worker_state.closing.load(Ordering::Acquire))
                        }
                        Err(_) => {
                            super::increment(&worker_state.counters.failed);
                            process_logging_metrics().record(
                                worker_state.destination,
                                Level::ERROR,
                                LogObservation::WorkerFailed,
                                1,
                            );
                            break;
                        }
                    };
                    if let Some(record) = next {
                        let error = output.write_all(&record.bytes).err();
                        if let Some(error) = &error {
                            super::increment(&worker_state.counters.failed);
                            let observation = match error.kind() {
                                io::ErrorKind::StorageFull => LogObservation::StorageFull,
                                io::ErrorKind::PermissionDenied => LogObservation::PermissionDenied,
                                io::ErrorKind::BrokenPipe => LogObservation::BrokenPipe,
                                _ => LogObservation::WriteFailure,
                            };
                            process_logging_metrics().record(
                                worker_state.destination,
                                record.level,
                                observation,
                                1,
                            );
                        } else {
                            super::increment(&worker_state.counters.written);
                            process_logging_metrics().record(
                                worker_state.destination,
                                record.level,
                                LogObservation::Written,
                                1,
                            );
                        }
                        worker_state.pending[level_index(record.level)]
                            .fetch_sub(1, Ordering::Relaxed);
                        let now_failed = error.is_some();
                        if failed != now_failed
                            && reported.is_none_or(|at| at.elapsed() >= Duration::from_secs(60))
                        {
                            super::emit(super::OperationalEvent::SinkState {
                                destination: worker_state.destination,
                                failed: now_failed,
                            });
                            reported = Some(Instant::now());
                            failed = now_failed;
                        }
                    } else if closing || receiver.recv().is_err() {
                        break;
                    }
                }
                if output.flush().is_err() {
                    super::increment(&worker_state.counters.failed);
                    process_logging_metrics().record(
                        worker_state.destination,
                        Level::ERROR,
                        LogObservation::WorkerFailed,
                        1,
                    );
                }
                process_logging_metrics().set_enabled(worker_state.destination, false);
                drop(output);
                let _ = done.try_send(());
            })?;
        process_logging_metrics().set_enabled(self.destination, true);
        let writer = NonBlocking {
            state: state.clone(),
            wake: wake.clone(),
        };
        Ok((
            writer,
            WorkerGuard {
                state,
                wake,
                completed,
                task: Some(task),
            },
        ))
    }
}

pub(super) struct WorkerGuard {
    state: Arc<State>,
    wake: mpsc::SyncSender<()>,
    completed: mpsc::Receiver<()>,
    task: Option<JoinHandle<()>>,
}

impl WorkerGuard {
    pub(super) fn stop(&mut self, timeout: Duration) -> bool {
        self.state.closing.store(true, Ordering::Release);
        let _ = self.wake.try_send(());
        if self.completed.recv_timeout(timeout).is_err() {
            let mut unflushed = 0;
            for (index, level) in LEVELS.iter().enumerate() {
                let pending = self.state.pending[index].load(Ordering::Relaxed);
                unflushed += pending;
                process_logging_metrics().record(
                    self.state.destination,
                    *level,
                    LogObservation::Unflushed,
                    pending,
                );
            }
            self.state
                .counters
                .unflushed
                .store(unflushed, Ordering::Relaxed);
            process_logging_metrics().set_enabled(self.state.destination, false);
            // The process deadline wins over a blocked OS writer. Dropping a
            // JoinHandle detaches it; its bounded resources die with the process.
            self.task.take();
            return false;
        }
        self.task.take().is_none_or(|task| task.join().is_ok())
    }
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        if self.task.is_some() {
            let _ = self.stop(Duration::from_secs(1));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    struct Stalled {
        entered: Arc<Barrier>,
        release: Arc<Barrier>,
    }
    impl Write for Stalled {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.entered.wait();
            self.release.wait();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    // req: ID-005, END-007, END-008
    #[test]
    fn stalled_log_sink_does_not_block_command_completion() {
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let (writer, mut guard) = NonBlockingBuilder::default()
            .buffered_lines_limit(4)
            .finish(Stalled {
                entered: entered.clone(),
                release: release.clone(),
            })
            .unwrap();
        writer.enqueue(b"first\n", Level::INFO).unwrap();
        entered.wait();
        for _ in 0..3 {
            writer.enqueue(b"routine\n", Level::INFO).unwrap();
        }
        assert!(writer.enqueue(b"overflow\n", Level::INFO).is_err());
        writer.enqueue(b"critical\n", Level::ERROR).unwrap();
        assert_eq!(writer.state.counters.dropped.load(Ordering::Relaxed), 1);
        assert!(!guard.stop(Duration::ZERO));
        assert_eq!(guard.state.counters.unflushed.load(Ordering::Relaxed), 5);
        // Release exactly each accepted write without sleeps or timing guesses.
        for index in 0..5 {
            release.wait();
            if index < 4 {
                entered.wait();
            }
        }
    }
    struct Controlled {
        outcomes: mpsc::Receiver<Option<io::ErrorKind>>,
        completed: mpsc::SyncSender<()>,
    }
    impl Write for Controlled {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let outcome = self.outcomes.recv().unwrap();
            self.completed.send(()).unwrap();
            outcome.map_or(Ok(bytes.len()), |kind| Err(kind.into()))
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    // req: ID-005, END-007, END-008
    #[test]
    fn failed_sink_recovers_without_blocking_independent_delivery() {
        let (send, outcomes) = mpsc::sync_channel(1);
        let (completed, received) = mpsc::sync_channel(1);
        let (failed, mut failure_guard) = NonBlockingBuilder::default()
            .destination(LogDestination::File)
            .finish(Controlled {
                outcomes,
                completed,
            })
            .unwrap();
        let (healthy, mut healthy_guard) = NonBlockingBuilder::default()
            .finish(Vec::<u8>::new())
            .unwrap();
        failed.enqueue(b"first\n", Level::ERROR).unwrap();
        send.send(Some(io::ErrorKind::StorageFull)).unwrap();
        received.recv().unwrap();
        healthy.enqueue(b"first\n", Level::ERROR).unwrap();
        failed.enqueue(b"second\n", Level::WARN).unwrap();
        send.send(None).unwrap();
        received.recv().unwrap();
        healthy.enqueue(b"second\n", Level::WARN).unwrap();
        assert!(failure_guard.stop(Duration::from_secs(1)));
        assert!(healthy_guard.stop(Duration::from_secs(1)));
        assert_eq!(failed.state.counters.failed.load(Ordering::Relaxed), 1);
        assert_eq!(failed.state.counters.written.load(Ordering::Relaxed), 1);
        assert_eq!(healthy.state.counters.written.load(Ordering::Relaxed), 2);
    }
    // req: ID-005, END-007, END-008
    #[test]
    fn multiple_stalled_sinks_share_one_final_deadline() {
        let mut barriers = Vec::new();
        let mut writers = Vec::new();
        for _ in 0..2 {
            let entered = Arc::new(Barrier::new(2));
            let release = Arc::new(Barrier::new(2));
            let (writer, guard) = NonBlockingBuilder::default()
                .finish(Stalled {
                    entered: entered.clone(),
                    release: release.clone(),
                })
                .unwrap();
            writer.enqueue(b"pending\n", Level::ERROR).unwrap();
            entered.wait();
            writers.push(guard);
            barriers.push(release);
        }
        let logging = super::super::RunningLogging {
            writers,
            deadline: Some(Instant::now()),
            sampler: None,
            signals: None,
            metrics: None,
            counters: Arc::new(super::super::LogCounters::default()),
        };
        drop(logging); // Both writes are still blocked; drain cannot join either.
        for release in barriers {
            release.wait();
        }
    }
}
