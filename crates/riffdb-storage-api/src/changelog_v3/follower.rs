//! Internal follower consumption boundary. This is not an application SDK
//! write surface; only completed follower activation can supply an owner.

use super::ChangelogHistoryPointV3;
use crate::StorageError;

/// Sole synchronous follower writer, handed to one receiver worker after the
/// structural/catalog startup proof join. No clone, raw transaction, generic
/// mutation callback, source sequence allocator or application writer is exposed.
///
/// Implementations check a complete bounded V3 frame and apply it in one local
/// durable transaction. Failure releases no applied frontier and fences the
/// handle. The caller must authenticate the upstream and retain bounded custody
/// of the input bytes until this synchronous call completes.
pub trait ChangelogFollowerApplyPortV3: Send {
    /// Checksums, lineage, epoch, strict sequence, receipt chaining and exact
    /// pre-images precede successful publication. Returns only durable progress.
    fn apply_frame(&mut self, bytes: &[u8]) -> Result<ChangelogHistoryPointV3, StorageError>;

    /// Starts a new connection's frame chain at the exact durable resume fence.
    /// It neither changes the lineage nor skips unapplied source receipts.
    fn resume_stream(&mut self) -> Result<ChangelogHistoryPointV3, StorageError>;

    /// Persists follower-local acknowledgement before releasing its position.
    /// No source receipt is allocated and no network delivery is inferred.
    fn acknowledge_durable_position(&mut self) -> Result<ChangelogHistoryPointV3, StorageError>;

    /// Consumes the sole worker-owned writer after its durability boundary has
    /// drained, without performing the source-mode CLEAN lifecycle transaction.
    fn close(self: Box<Self>) -> Result<(), StorageError>;
}
