//! Bounded archive consumption of existing V3 frames (ADR-0178 §7).
//!
//! This owner has no authoritative writer or acknowledgement callback. A sink
//! confirmation advances only archive-local progress; composition separately
//! resolves any source retention acknowledgement through its existing owner.
use std::fmt;

use sha2::{Digest, Sha256};

use crate::{
    ChangelogFrameV3, ChangelogHistoryPointV3, ChangelogLineageV3, MAX_CHANGELOG_FRAME_BYTES,
};

/// Value-free archive failures. No keys, payloads, paths or checksums are exposed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArchiveConsumerErrorV1 {
    /// The frame is malformed, corrupt, noncanonical or over its hard bound.
    InvalidFrame,
    /// Database, incarnation or leadership differs from the archive binding.
    ForeignLineage,
    /// Frame/receipt predecessor or exact frontier does not match confirmed progress.
    InvalidPosition,
    /// Sink durability is uncertain; retain and retry the exact pending frame.
    SinkUnavailable,
    /// The bounded consumer cannot continue; reopen from verified durable sink state.
    ResyncRequired,
}
impl fmt::Display for ArchiveConsumerErrorV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidFrame => "invalid archive frame",
            Self::ForeignLineage => "archive lineage mismatch",
            Self::InvalidPosition => "archive position mismatch",
            Self::SinkUnavailable => "archive sink durability unavailable",
            Self::ResyncRequired => "archive resynchronization required",
        })
    }
}
impl std::error::Error for ArchiveConsumerErrorV1 {}

/// One fully validated complete frame and its exact archive-local descriptor.
/// Construction is private: a sink cannot receive unchecked frame metadata.
pub struct ArchiveFrameV1 {
    bytes: Vec<u8>,
    lineage: ChangelogLineageV3,
    before: ChangelogHistoryPointV3,
    covered: ChangelogHistoryPointV3,
    digest: [u8; 32],
    frame_checksum: [u8; 32],
}
impl ArchiveFrameV1 {
    /// Full unredacted frame bytes, bounded by the unchanged V3 frame ceiling.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
    /// Exact database, history and leadership binding.
    #[must_use]
    pub const fn lineage(&self) -> ChangelogLineageV3 {
        self.lineage
    }
    /// Exact durable receipt position immediately before this frame.
    #[must_use]
    pub const fn before(&self) -> ChangelogHistoryPointV3 {
        self.before
    }
    /// Exact receipt position covered by the complete unsplit frame.
    #[must_use]
    pub const fn covered(&self) -> ChangelogHistoryPointV3 {
        self.covered
    }
    /// SHA-256 over all stored file bytes, including the existing V3 footer.
    #[must_use]
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}
impl fmt::Debug for ArchiveFrameV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ArchiveFrameV1([redacted])")
    }
}

/// Storage-independent durable sink. Implementations must compare exact bytes
/// on retry, sync the complete frame and its discoverable archive descriptor,
/// and return success only after durability is known. Failure may mean that
/// either neither or both reached disk; it never permits a different retry.
/// This internal port grants no application or database mutation authority.
pub trait ArchiveFrameSinkV1 {
    /// Persists a whole frame idempotently without modifying any existing bytes.
    fn persist(&mut self, frame: &ArchiveFrameV1) -> Result<(), ArchiveConsumerErrorV1>;
}

/// One sink owner with at most one pending V3 frame. No unbounded queue or task
/// is created, and progress never advances on decode, emission or uncertain I/O.
pub struct ArchiveConsumerV1<S> {
    sink: S,
    lineage: ChangelogLineageV3,
    position: ChangelogHistoryPointV3,
    prior_frame_checksum: [u8; 32],
    pending: Option<ArchiveFrameV1>,
    failure: Option<ArchiveConsumerErrorV1>,
}
impl<S: ArchiveFrameSinkV1> ArchiveConsumerV1<S> {
    /// Starts at the caller's independently verified full-backup/archive fence.
    /// Constructing a consumer does not validate that external durable evidence.
    #[must_use]
    pub const fn new(sink: S, lineage: ChangelogLineageV3, after: ChangelogHistoryPointV3) -> Self {
        Self {
            sink,
            lineage,
            position: after,
            prior_frame_checksum: after.history_hash(),
            pending: None,
            failure: None,
        }
    }

    /// Begins a new replication connection at confirmed progress, preserving
    /// the durable cross-connection receipt chain and resetting only frame chaining.
    pub fn begin_stream(&mut self) -> Result<(), ArchiveConsumerErrorV1> {
        self.check_live()?;
        if self.pending.is_some() {
            return self.fail(ArchiveConsumerErrorV1::ResyncRequired);
        }
        self.prior_frame_checksum = self.position.history_hash();
        Ok(())
    }

    /// Validates before calling the sink. A second submission while one is
    /// uncertain fuses with typed resync; it cannot skip or replace pending bytes.
    pub fn append(
        &mut self,
        bytes: Vec<u8>,
    ) -> Result<ChangelogHistoryPointV3, ArchiveConsumerErrorV1> {
        self.check_live()?;
        if self.pending.is_some() {
            return self.fail(ArchiveConsumerErrorV1::ResyncRequired);
        }
        let frame = match self.validate(bytes) {
            Ok(frame) => frame,
            Err(error) => return self.fail(error),
        };
        self.pending = Some(frame);
        self.retry_pending()
    }

    /// Retries only retained, already validated bytes. Sink success is the sole
    /// transition that clears pending and advances this consumer's local fence.
    pub fn retry_pending(&mut self) -> Result<ChangelogHistoryPointV3, ArchiveConsumerErrorV1> {
        self.check_live()?;
        let frame = self
            .pending
            .as_ref()
            .ok_or(ArchiveConsumerErrorV1::InvalidPosition)?;
        match self.sink.persist(frame) {
            Ok(()) => {
                self.position = frame.covered;
                self.prior_frame_checksum = frame.frame_checksum;
                self.pending = None;
                Ok(self.position)
            }
            Err(ArchiveConsumerErrorV1::SinkUnavailable) => {
                Err(ArchiveConsumerErrorV1::SinkUnavailable)
            }
            Err(error) => self.fail(error),
        }
    }

    /// Last sink-confirmed fence, never a source acknowledgement or a freshness token.
    #[must_use]
    pub const fn position(&self) -> ChangelogHistoryPointV3 {
        self.position
    }
    /// Whether the sole retained frame still requires exact durability resolution.
    #[must_use]
    pub const fn has_pending_frame(&self) -> bool {
        self.pending.is_some()
    }
    /// Releases sink ownership; any uncertain pending frame remains a sink recovery concern.
    #[must_use]
    pub fn into_sink(self) -> S {
        self.sink
    }

    fn check_live(&self) -> Result<(), ArchiveConsumerErrorV1> {
        self.failure.map_or(Ok(()), Err)
    }
    fn fail<T>(&mut self, error: ArchiveConsumerErrorV1) -> Result<T, ArchiveConsumerErrorV1> {
        self.failure = Some(error);
        Err(error)
    }
    fn validate(&self, bytes: Vec<u8>) -> Result<ArchiveFrameV1, ArchiveConsumerErrorV1> {
        if bytes.len() > MAX_CHANGELOG_FRAME_BYTES {
            return Err(ArchiveConsumerErrorV1::InvalidFrame);
        }
        let frame =
            ChangelogFrameV3::decode(&bytes).map_err(|_| ArchiveConsumerErrorV1::InvalidFrame)?;
        let binding = frame.binding();
        if binding.database_id() != self.lineage.database_id()
            || binding.history_incarnation() != self.lineage.history_incarnation()
            || binding.leadership_epoch() != self.lineage.leadership_epoch().get()
        {
            return Err(ArchiveConsumerErrorV1::ForeignLineage);
        }
        let first = frame
            .receipts()
            .first()
            .ok_or(ArchiveConsumerErrorV1::InvalidFrame)?
            .binding();
        if binding.prior_frame_hash() != self.prior_frame_checksum
            || first.predecessor != Some(self.position.sequence())
            || Some(first.sequence) != self.position.sequence().checked_next()
            || first.predecessor_frontier != self.position.frontier()
            || first.prior_history_hash != self.position.history_hash()
        {
            return Err(ArchiveConsumerErrorV1::InvalidPosition);
        }
        let last = frame
            .receipts()
            .last()
            .ok_or(ArchiveConsumerErrorV1::InvalidFrame)?;
        let covered = ChangelogHistoryPointV3::from_receipt(last)
            .map_err(|_| ArchiveConsumerErrorV1::InvalidFrame)?;
        let frame_checksum = *bytes
            .last_chunk::<32>()
            .ok_or(ArchiveConsumerErrorV1::InvalidFrame)?;
        Ok(ArchiveFrameV1 {
            lineage: self.lineage,
            before: self.position,
            covered,
            frame_checksum,
            digest: Sha256::digest(&bytes).into(),
            bytes,
        })
    }
}
impl<S> fmt::Debug for ArchiveConsumerV1<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ArchiveConsumerV1([redacted])")
    }
}
