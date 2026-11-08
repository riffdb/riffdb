//! Bounded output adapter. No worker failure or shutdown path writes to stdout.

use std::collections::VecDeque;
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

#[derive(Default)]
pub(super) struct SinkCounters {
    pub(super) enqueued: AtomicU64,
    pub(super) written: AtomicU64,
    pub(super) dropped: AtomicU64,
    pub(super) failed: AtomicU64,
    pub(super) unflushed: AtomicU64,
}

struct State {
    queue: Mutex<VecDeque<Vec<u8>>>,
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
    pub(super) fn enqueue(&self, bytes: &[u8], high_priority: bool) -> io::Result<()> {
        let limit = if high_priority {
            self.state.capacity
        } else {
            self.state.capacity - self.state.reserve
        };
        let mut queue = match self.state.queue.lock() {
            Ok(queue) => queue,
            Err(_) => {
                super::increment(&self.state.counters.dropped);
                return Err(io::ErrorKind::WouldBlock.into());
            }
        };
        if self.state.closing.load(Ordering::Acquire)
            || queue.len() >= limit
            || bytes.len() > super::events::MAX_RECORD_BYTES
        {
            super::increment(&self.state.counters.dropped);
            return Err(io::ErrorKind::WouldBlock.into());
        }
        queue.push_back(bytes.to_vec());
        super::increment(&self.state.counters.enqueued);
        drop(queue);
        let _ = self.wake.try_send(());
        Ok(())
    }
}

pub(super) struct NonBlockingBuilder {
    capacity: usize,
    name: &'static str,
}

impl Default for NonBlockingBuilder {
    fn default() -> Self {
        Self {
            capacity: 4096,
            name: "riffdb-log-writer",
        }
    }
}

impl NonBlockingBuilder {
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
                loop {
                    let (next, closing) = match worker_state.queue.lock() {
                        Ok(mut queue) => (
                            queue.pop_front(),
                            worker_state.closing.load(Ordering::Acquire),
                        ),
                        Err(_) => {
                            super::increment(&worker_state.counters.failed);
                            break;
                        }
                    };
                    if let Some(record) = next {
                        if output.write_all(&record).is_ok() {
                            super::increment(&worker_state.counters.written);
                        } else {
                            super::increment(&worker_state.counters.failed);
                        }
                    } else if closing || receiver.recv().is_err() {
                        break;
                    }
                }
                if output.flush().is_err() {
                    super::increment(&worker_state.counters.failed);
                }
                drop(output);
                let _ = done.try_send(());
            })?;
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
    fn stop(&mut self, timeout: Duration) -> bool {
        self.state.closing.store(true, Ordering::Release);
        let _ = self.wake.try_send(());
        if self.completed.recv_timeout(timeout).is_err() {
            let queued = self
                .state
                .queue
                .try_lock()
                .map_or(self.state.capacity, |q| q.len());
            self.state
                .counters
                .unflushed
                .store(queued as u64, Ordering::Relaxed);
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
        writer.enqueue(b"first\n", false).unwrap();
        entered.wait();
        for _ in 0..3 {
            writer.enqueue(b"routine\n", false).unwrap();
        }
        assert!(writer.enqueue(b"overflow\n", false).is_err());
        writer.enqueue(b"critical\n", true).unwrap();
        assert_eq!(writer.state.counters.dropped.load(Ordering::Relaxed), 1);
        assert!(!guard.stop(Duration::ZERO));
        // Release exactly each accepted write without sleeps or timing guesses.
        for index in 0..5 {
            release.wait();
            if index < 4 {
                entered.wait();
            }
        }
    }
}
