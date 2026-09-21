//! Bounded V3 framing over published receipt cursors. No storage writer,
//! application authorization, network, or journal bytes are reachable here.

use super::{
    AuthoritativeTransactionV3, ChangelogCursorErrorV3, ChangelogFrameBindingV3, ChangelogFrameV3,
    ChangelogHistoryPointV3, ChangelogLineageV3, ChangelogReceiptCursorV3, ChangelogV3Error,
};
use crate::{
    MAX_CHANGELOG_FRAME_BYTES, MAX_STAGED_COMMANDS, PublishedDurableSnapshot, StorageErrorKind,
};

pub use riffdb_errors::ReplicationStreamErrorV3;

impl From<ChangelogCursorErrorV3> for ReplicationStreamErrorV3 {
    fn from(error: ChangelogCursorErrorV3) -> Self {
        match error {
            ChangelogCursorErrorV3::ForeignLineage => Self::ForeignLineage,
            ChangelogCursorErrorV3::StaleEpoch => Self::StaleEpoch,
            ChangelogCursorErrorV3::InvalidPosition => Self::InvalidPosition,
            ChangelogCursorErrorV3::Storage(error) => match error.kind() {
                StorageErrorKind::HistoryPruned => Self::HistoryPruned,
                StorageErrorKind::IncompatibleFormat => Self::UnsupportedFormat,
                StorageErrorKind::Unavailable | StorageErrorKind::CommitStatusUnknown => {
                    Self::Unavailable
                }
                StorageErrorKind::CorruptData
                | StorageErrorKind::InvariantViolation
                | StorageErrorKind::LimitExceeded
                | StorageErrorKind::SequenceExhausted => Self::CorruptHistory,
            },
        }
    }
}

impl From<ChangelogV3Error> for ReplicationStreamErrorV3 {
    fn from(_: ChangelogV3Error) -> Self {
        Self::CorruptHistory
    }
}

/// Exact negotiation values, not an authorization or bootstrap permit.
/// The service must authenticate the administrative capability and establish
/// transport confidentiality before opening or exposing any source bytes.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ReplicationHandshakeV3 {
    lineage: ChangelogLineageV3,
    after: ChangelogHistoryPointV3,
}

impl ReplicationHandshakeV3 {
    /// Accepts only the complete production format, catalog, and exact bounds.
    pub fn new(
        lineage: ChangelogLineageV3,
        after: ChangelogHistoryPointV3,
        readable_format: &str,
        catalog_digest: [u8; 32],
        maximum_frame_bytes: u64,
        maximum_transitions: u64,
    ) -> Result<Self, ReplicationStreamErrorV3> {
        if readable_format != ChangelogFrameV3::IDENTITY {
            return Err(ReplicationStreamErrorV3::UnsupportedFormat);
        }
        if catalog_digest != lineage.catalog_digest() {
            return Err(ReplicationStreamErrorV3::UnsupportedCatalog);
        }
        if maximum_frame_bytes != MAX_CHANGELOG_FRAME_BYTES as u64
            || maximum_transitions != MAX_STAGED_COMMANDS as u64
        {
            return Err(ReplicationStreamErrorV3::UnsupportedBounds);
        }
        Ok(Self { lineage, after })
    }

    /// Exact requested database, incarnation, and leadership epoch.
    #[must_use]
    pub const fn lineage(self) -> ChangelogLineageV3 {
        self.lineage
    }

    /// Exact acknowledged or bootstrap fence whose successor is requested.
    #[must_use]
    pub const fn after(self) -> ChangelogHistoryPointV3 {
        self.after
    }
}

/// Complete checksummed frame bytes plus the emitted (not acknowledged) fence.
pub struct EmittedChangelogFrameV3 {
    bytes: Vec<u8>,
    covered: ChangelogHistoryPointV3,
}

impl EmittedChangelogFrameV3 {
    /// Borrows unredacted replication bytes. Never diagnostic data.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Transfers frame custody without copying its bounded payload.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Last complete receipt included in these bytes. Emission is not durability
    /// at the receiver and never advances a source retention acknowledgement.
    #[must_use]
    pub const fn covered(&self) -> ChangelogHistoryPointV3 {
        self.covered
    }
}

/// Synchronous framing core used outside the exclusive write gate. Retains one
/// bounded receipt cursor. Ordinary emission preserves one receipt per frame;
/// archive emission may group complete receipts within the same wire ceilings.
/// At most one bounded lookahead receipt is retained. New pins can coalesce
/// notifications without skipping retained receipts.
///
/// Each connection's frame chain starts at the negotiated resume history hash;
/// subsequent frames bind the previous frame checksum. Receipt history remains
/// the durable cross-connection chain. Reconnect starts a new frame chain at
/// the same exact acknowledged receipt, never at a bare numeric sequence.
pub struct ChangelogFrameCursorV3 {
    lineage: ChangelogLineageV3,
    position: ChangelogHistoryPointV3,
    prior_frame_hash: [u8; 32],
    cursor: Box<dyn ChangelogReceiptCursorV3>,
    lookahead: Option<AuthoritativeTransactionV3>,
    failure: Option<ReplicationStreamErrorV3>,
}

impl ChangelogFrameCursorV3 {
    /// Opens only the snapshot's WP-772 receipt port. Opening and framing cannot
    /// read current entity values, acquire a writer, or acknowledge a follower.
    pub fn open(
        snapshot: &dyn PublishedDurableSnapshot,
        handshake: ReplicationHandshakeV3,
    ) -> Result<Self, ReplicationStreamErrorV3> {
        let cursor = snapshot.changelog_receipts_v3(handshake.lineage, handshake.after)?;
        validate_pin(cursor.as_ref(), handshake.lineage, handshake.after)?;
        Ok(Self {
            lineage: handshake.lineage,
            position: handshake.after,
            prior_frame_hash: handshake.after.history_hash(),
            cursor,
            lookahead: None,
            failure: None,
        })
    }

    /// Replaces the immutable pin at the exact emitted successor without
    /// resetting the connection's frame chain. Any failure permanently fences
    /// this stream, including a lineage/epoch change while it was connected.
    pub fn advance_snapshot(
        &mut self,
        snapshot: &dyn PublishedDurableSnapshot,
    ) -> Result<(), ReplicationStreamErrorV3> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        let result = (|| {
            let cursor = snapshot.changelog_receipts_v3(self.lineage, self.position)?;
            validate_pin(cursor.as_ref(), self.lineage, self.position)?;
            self.cursor = cursor;
            // The new cursor starts after the emitted position, so any old
            // lookahead must be read again through this pin's exact fence.
            self.lookahead = None;
            Ok(())
        })();
        if let Err(error) = result {
            self.failure = Some(error);
        }
        result
    }

    /// Produces a one-receipt frame or the exact pinned end. Once failed, never
    /// returns another frame. All validation finishes before progress advances.
    pub fn next_frame(
        &mut self,
    ) -> Result<Option<EmittedChangelogFrameV3>, ReplicationStreamErrorV3> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        let result = self.frame_next_receipts(1);
        if let Err(error) = result {
            self.failure = Some(error);
        }
        result
    }

    /// Coalesces already-published complete receipts up to the existing V3
    /// byte and transition ceilings, without waiting for another publication.
    pub fn next_coalesced_frame(
        &mut self,
    ) -> Result<Option<EmittedChangelogFrameV3>, ReplicationStreamErrorV3> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        let result = self.frame_next_receipts(MAX_STAGED_COMMANDS);
        if let Err(error) = result {
            self.failure = Some(error);
            self.lookahead = None;
        }
        result
    }

    /// Most recently emitted position, never a receiver durability claim.
    #[must_use]
    pub const fn position(&self) -> ChangelogHistoryPointV3 {
        self.position
    }

    /// Source head in the current immutable publication pin, which can be ahead
    /// of emission. This is an observation for sequence-lag reporting, never a
    /// receiver acknowledgement or permission to satisfy a freshness wait.
    /// A failed stream cannot keep advertising its last successful observation.
    pub fn published_head(&self) -> Result<ChangelogHistoryPointV3, ReplicationStreamErrorV3> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        Ok(self.cursor.history().tail())
    }

    fn frame_next_receipts(
        &mut self,
        maximum_receipts: usize,
    ) -> Result<Option<EmittedChangelogFrameV3>, ReplicationStreamErrorV3> {
        let tail = self.cursor.history().tail();
        let mut covered = self.position;
        let mut receipts = Vec::new();
        let mut transitions = 0_u64;
        let frame_binding = ChangelogFrameBindingV3::new(
            self.lineage.database_id(),
            self.lineage.history_incarnation(),
            self.lineage.leadership_epoch().get(),
            self.lineage.catalog_digest(),
            self.prior_frame_hash,
        )?;
        let mut bytes = frame_binding.fixed_frame_bytes();
        while receipts.len() < maximum_receipts && transitions < MAX_STAGED_COMMANDS as u64 {
            let next = match self.lookahead.take() {
                Some(receipt) => Some(receipt),
                None => self.cursor.next_receipt()?,
            };
            let Some(receipt) = next else {
                if covered != tail {
                    return Err(ReplicationStreamErrorV3::CorruptHistory);
                }
                break;
            };
            let binding = receipt.binding();
            let successor = ChangelogHistoryPointV3::from_receipt(&receipt)?;
            if binding.database_id != self.lineage.database_id()
                || binding.history_incarnation != self.lineage.history_incarnation()
                || binding.predecessor != Some(covered.sequence())
                || Some(binding.sequence) != covered.sequence().checked_next()
                || binding.prior_history_hash != covered.history_hash()
                || binding.predecessor_frontier != covered.frontier()
                || binding.sequence > tail.sequence()
                || (binding.sequence == tail.sequence() && successor != tail)
            {
                return Err(ReplicationStreamErrorV3::CorruptHistory);
            }
            let next_transitions = transitions
                .checked_add(receipt.transition_count())
                .ok_or(ChangelogV3Error::LimitExceeded)?;
            let next_bytes = bytes
                .checked_add(4)
                .and_then(|n| n.checked_add(receipt.encoded_len().ok()?))
                .ok_or(ChangelogV3Error::LimitExceeded)?;
            if next_transitions > MAX_STAGED_COMMANDS as u64
                || next_bytes > MAX_CHANGELOG_FRAME_BYTES
            {
                if receipts.is_empty() {
                    return Err(ChangelogV3Error::LimitExceeded.into());
                }
                // Never split a physical transaction. Keep only this one
                // checked receipt, bounded by its existing admission ceiling.
                self.lookahead = Some(receipt);
                break;
            }
            receipts.push(receipt);
            transitions = next_transitions;
            bytes = next_bytes;
            covered = successor;
        }
        if receipts.is_empty() {
            return Ok(None);
        }
        let frame = ChangelogFrameV3::new(frame_binding, receipts)?;
        let bytes = frame.encode()?;
        let checksum = bytes
            .last_chunk::<32>()
            .ok_or(ReplicationStreamErrorV3::CorruptHistory)?;
        self.prior_frame_hash = *checksum;
        self.position = covered;
        Ok(Some(EmittedChangelogFrameV3 { bytes, covered }))
    }
}

fn validate_pin(
    cursor: &dyn ChangelogReceiptCursorV3,
    lineage: ChangelogLineageV3,
    after: ChangelogHistoryPointV3,
) -> Result<(), ReplicationStreamErrorV3> {
    let history = cursor.history();
    if history.lineage().database_id() != lineage.database_id()
        || history.lineage().history_incarnation() != lineage.history_incarnation()
    {
        return Err(ReplicationStreamErrorV3::ForeignLineage);
    }
    if history.lineage().leadership_epoch() != lineage.leadership_epoch() {
        return Err(ReplicationStreamErrorV3::StaleEpoch);
    }
    if history.lineage().catalog_digest() != lineage.catalog_digest() {
        return Err(ReplicationStreamErrorV3::UnsupportedCatalog);
    }
    if after.sequence() < history.minimum_resume().sequence() {
        return Err(ReplicationStreamErrorV3::HistoryPruned);
    }
    if after.sequence() > history.tail().sequence()
        || (after.sequence() == history.tail().sequence() && after != history.tail())
        || (after.sequence() == history.minimum_resume().sequence()
            && after != history.minimum_resume())
    {
        return Err(ReplicationStreamErrorV3::InvalidPosition);
    }
    Ok(())
}

macro_rules! redacted_debug {
    ($($ty:ident),+) => { $(
        impl std::fmt::Debug for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(concat!(stringify!($ty), "([redacted])"))
            }
        }
    )+ };
}
redacted_debug!(
    ReplicationHandshakeV3,
    ChangelogFrameCursorV3,
    EmittedChangelogFrameV3
);
