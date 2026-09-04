pub use riffdb_observability::{
    ConflictEvent, ConflictEventKind, ConflictObserver, NoopConflictObserver,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError, sync_channel};
use std::thread;
pub(crate) const DEFAULT_TELEMETRY_QUEUE_CAPACITY: usize = 4_096;

/// A bounded, nonblocking handoff between lock progress and observer code.
///
/// The worker is intentionally detached. Dropping a manager closes the sender
/// without waiting for arbitrary observer code, so shutdown remains bounded
/// even if a subscriber is stalled. Events are dropped rather than blocking
/// when the queue is full; the cumulative drop counter remains observable.
pub(crate) struct ConflictTelemetry {
    sender: Option<SyncSender<ConflictEvent>>,
    dropped: AtomicUsize,
}

impl ConflictTelemetry {
    pub(crate) fn new(
        observer: Arc<dyn ConflictObserver>,
        capacity: usize,
    ) -> Result<Self, ConflictTelemetryBuildError> {
        debug_assert!(capacity > 0);
        let (sender, receiver) = sync_channel(capacity);
        let _worker = thread::Builder::new()
            .name("riffdb-conflict-telemetry".to_owned())
            .spawn(move || {
                while let Ok(event) = receiver.recv() {
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        observer.observe(event);
                    }));
                }
            })
            .map_err(|_| ConflictTelemetryBuildError)?;
        Ok(Self {
            sender: Some(sender),
            dropped: AtomicUsize::new(0),
        })
    }

    #[cfg(any(test, feature = "loom", feature = "shuttle"))]
    pub(crate) fn disabled() -> Self {
        Self {
            sender: None,
            dropped: AtomicUsize::new(0),
        }
    }

    pub(crate) fn emit(&self, event: ConflictEvent) {
        let Some(sender) = &self.sender else {
            return;
        };
        if let Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) = sender.try_send(event) {
            let _ = self
                .dropped
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    Some(current.saturating_add(1))
                });
        }
    }

    pub(crate) fn dropped(&self) -> usize {
        self.dropped.load(Ordering::Acquire)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ConflictTelemetryBuildError;
