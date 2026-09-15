//! API-neutral, non-durable stream observations for sequence-lag reporting.
use riffdb_types::DualFrontier;

/// Source head observed in the immutable publication used to emit a frame.
/// The stream handshake supplies its lineage. This is advisory telemetry:
/// it cannot acknowledge a receiver, authorize replay or satisfy freshness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicationSourceHead {
    transaction_sequence: u64,
    frontier: DualFrontier,
}
impl ReplicationSourceHead {
    /// Checks the nonzero physical sequence; the receiving storage adapter
    /// additionally checks this observation against the frame it accompanies.
    #[must_use]
    pub fn new(transaction_sequence: u64, frontier: DualFrontier) -> Option<Self> {
        (transaction_sequence != 0).then_some(Self {
            transaction_sequence,
            frontier,
        })
    }
    /// Observed source transaction head, not the receiver's durable position.
    #[must_use]
    pub const fn transaction_sequence(self) -> u64 {
        self.transaction_sequence
    }
    /// Application and administration frontiers at the observed source head.
    #[must_use]
    pub const fn frontier(self) -> DualFrontier {
        self.frontier
    }
}

/// Unchanged durable frame bytes with optional non-authoritative source progress.
/// Absence means the peer did not report its head; it never means zero lag.
#[derive(Clone, Eq, PartialEq)]
pub struct ReplicationFrame {
    bytes: Vec<u8>,
    source_head: Option<ReplicationSourceHead>,
}
impl ReplicationFrame {
    /// Wraps transport bytes and their optional observation. Normal service
    /// release bounds and receiver frame validation still apply to these bytes.
    #[must_use]
    pub fn new(bytes: Vec<u8>, source_head: Option<ReplicationSourceHead>) -> Self {
        Self { bytes, source_head }
    }
    /// Optional source progress, never a local durability acknowledgement.
    #[must_use]
    pub const fn source_head(&self) -> Option<ReplicationSourceHead> {
        self.source_head
    }
    /// Transfers the durable payload and its separate observation without copying.
    #[must_use]
    pub fn into_parts(self) -> (Vec<u8>, Option<ReplicationSourceHead>) {
        (self.bytes, self.source_head)
    }
}
impl From<Vec<u8>> for ReplicationFrame {
    fn from(bytes: Vec<u8>) -> Self {
        Self::new(bytes, None)
    }
}
impl std::ops::Deref for ReplicationFrame {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}
impl std::fmt::Debug for ReplicationFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicationFrame([redacted])")
    }
}
