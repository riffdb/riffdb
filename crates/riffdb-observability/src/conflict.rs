//! Closed conflict telemetry vocabulary.

use std::fmt;
use std::time::Duration;

use riffdb_types::ConflictKeyHash;

/// The terminal or queue event represented by one redaction-safe observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConflictEventKind {
    /// A waiter entered the FIFO queue for this key.
    Queued,
    /// The complete multi-key set was granted.
    Acquired,
    /// Cancellation removed the waiter or released a concurrent grant.
    Cancelled,
    /// The absolute acquisition deadline elapsed.
    DeadlineExceeded,
    /// An acquired mutation capability was released.
    Released,
    /// A configured table or queue capacity rejected acquisition.
    CapacityRejected,
}

/// One bounded observation that cannot reveal a raw business key.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ConflictEvent {
    kind: ConflictEventKind,
    conflict_key_hash: ConflictKeyHash,
    wait_duration: Duration,
    queue_depth: usize,
    total_queue_depth: usize,
    key_count: usize,
}

impl ConflictEvent {
    #[doc(hidden)]
    pub const fn new(
        kind: ConflictEventKind,
        conflict_key_hash: ConflictKeyHash,
        wait_duration: Duration,
        queue_depth: usize,
        total_queue_depth: usize,
        key_count: usize,
    ) -> Self {
        Self {
            kind,
            conflict_key_hash,
            wait_duration,
            queue_depth,
            total_queue_depth,
            key_count,
        }
    }

    /// Returns the event classification.
    #[must_use]
    pub const fn kind(&self) -> ConflictEventKind {
        self.kind
    }

    /// Returns the domain-separated hash of the affected conflict key.
    #[must_use]
    pub const fn conflict_key_hash(&self) -> ConflictKeyHash {
        self.conflict_key_hash
    }

    /// Returns the time spent waiting before this observation.
    #[must_use]
    pub const fn wait_duration(&self) -> Duration {
        self.wait_duration
    }

    /// Returns the bounded per-key queue depth at observation time.
    #[must_use]
    pub const fn queue_depth(&self) -> usize {
        self.queue_depth
    }

    /// Returns the bounded total number of queued acquisitions.
    #[must_use]
    pub const fn total_queue_depth(&self) -> usize {
        self.total_queue_depth
    }

    /// Returns the number of canonical keys in the acquisition.
    #[must_use]
    pub const fn key_count(&self) -> usize {
        self.key_count
    }
}

impl fmt::Debug for ConflictEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConflictEvent")
            .field("kind", &self.kind)
            .field("conflict_key_hash", &self.conflict_key_hash)
            .field("wait_duration", &self.wait_duration)
            .field("queue_depth", &self.queue_depth)
            .field("total_queue_depth", &self.total_queue_depth)
            .field("key_count", &self.key_count)
            .finish()
    }
}

/// A sink for bounded conflict wait metrics and diagnostic events.
///
/// Implementations receive hashes only. The conflict manager contains observer
/// panics so telemetry cannot leak a granted capability during unwinding.
pub trait ConflictObserver: Send + Sync + 'static {
    /// Records one redaction-safe event.
    fn observe(&self, event: ConflictEvent);
}

/// An observer that intentionally discards all events.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopConflictObserver;

impl ConflictObserver for NoopConflictObserver {
    fn observe(&self, _event: ConflictEvent) {}
}
