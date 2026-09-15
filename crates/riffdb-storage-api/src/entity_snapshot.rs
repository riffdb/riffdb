//! Rewindable bounded entity pages from one immutable authoritative root.
use riffdb_types::EntityTypeId;

use crate::{
    ApplicationExportSnapshotReader, ApplicationExportSourcePageV1, StorageError, StorageScanLimit,
};

/// The entity-page input needed by derived-state reconstruction. This port
/// carries no export operation, writer, audit allocator, or current-root factory.
/// Every page and rewind must refer to the same pinned publication root.
pub trait AuthoritativeEntitySnapshotReader: Send + Sync {
    /// Reads one ascending page for a compiler-selected entity type. The existing
    /// entity-only source-page shape retains its 500-row, 4-MiB and continuation
    /// bounds. Foreign or oversized continuations refuse before scanning.
    fn read_entity_type_page(
        &self,
        entity: EntityTypeId,
        after: Option<&[u8]>,
        limit: StorageScanLimit,
    ) -> Result<ApplicationExportSourcePageV1, StorageError>;
}

// Preserve the existing primary exporter as an adapter to the narrow build
// input. This grants no export authority to a reader that implements only it.
impl<T: ApplicationExportSnapshotReader + ?Sized> AuthoritativeEntitySnapshotReader for T {
    fn read_entity_type_page(
        &self,
        entity: EntityTypeId,
        after: Option<&[u8]>,
        limit: StorageScanLimit,
    ) -> Result<ApplicationExportSourcePageV1, StorageError> {
        self.read_application_export_entity_page(entity, after, limit)
    }
}
