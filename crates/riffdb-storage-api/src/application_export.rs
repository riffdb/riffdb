//! Engine-neutral ownership and immutable-source ports for symbolic export.

use std::fmt;
use std::sync::Arc;

use riffdb_types::{
    ApplicationExportClassV1, ApplicationExportOperationId, ApplicationExportSnapshotBindingV1,
    ContractLineage,
};

use crate::{
    StorageError, StorageScanLimit, StorageValueError, StoredAdministrationAuditRecordV1,
    StoredDurableEventV1, StoredDurableEventV2, StoredEntityRecordV1, StoredProvenanceRecordV1,
};

/// Maximum canonical durable progress bytes retained for one export operation.
pub const MAX_APPLICATION_EXPORT_STATE_BYTES: usize = 256 * 1024;
/// Maximum active operation records returned to startup recovery in one pass.
pub const MAX_ACTIVE_APPLICATION_EXPORTS: usize = 256;
/// Maximum internal continuation bytes retained by a snapshot reader.
pub const MAX_APPLICATION_EXPORT_CONTINUATION_BYTES: usize = 4 * 1024;
/// Maximum exact encoded bytes returned by one source page.
pub const MAX_APPLICATION_EXPORT_SOURCE_PAGE_BYTES: usize = 4 * 1024 * 1024;

/// Version-aware event value yielded by one immutable source snapshot.
#[derive(Clone, Eq, PartialEq)]
pub enum ApplicationExportEventRecordV1 {
    /// Frozen unanchored event format.
    V1(StoredDurableEventV1),
    /// Current policy-anchored event format.
    V2(StoredDurableEventV2),
}

/// One policy-neutral internal source record. Physical continuation is separate.
#[derive(Clone, Eq, PartialEq)]
pub enum ApplicationExportSourceRecordV1 {
    /// Current entity post-image.
    Entity(Box<StoredEntityRecordV1>),
    /// Retained typed event.
    Event(Box<ApplicationExportEventRecordV1>),
    /// Immutable command provenance.
    Provenance(Box<StoredProvenanceRecordV1>),
    /// Shared administration record pending public-safe projection.
    PublicAudit(Box<StoredAdministrationAuditRecordV1>),
}

impl ApplicationExportSourceRecordV1 {
    /// Closed portable class of this internal record.
    #[must_use]
    pub const fn class(&self) -> ApplicationExportClassV1 {
        match self {
            Self::Entity(_) => ApplicationExportClassV1::Entity,
            Self::Event(_) => ApplicationExportClassV1::Event,
            Self::Provenance(_) => ApplicationExportClassV1::Provenance,
            Self::PublicAudit(_) => ApplicationExportClassV1::PublicAudit,
        }
    }
}

/// One exact bounded ascending page from a pinned published snapshot.
#[derive(Clone, Eq, PartialEq)]
pub struct ApplicationExportSourcePageV1 {
    class: ApplicationExportClassV1,
    records: Vec<ApplicationExportSourceRecordV1>,
    continuation: Option<Box<[u8]>>,
    exact_end: bool,
    encoded_bytes: usize,
}

impl ApplicationExportSourcePageV1 {
    /// Checks class homogeneity and exact row/byte/continuation bounds.
    pub fn new(
        class: ApplicationExportClassV1,
        records: Vec<ApplicationExportSourceRecordV1>,
        continuation: Option<Box<[u8]>>,
        exact_end: bool,
        encoded_bytes: usize,
    ) -> Result<Self, StorageValueError> {
        if records.len() > crate::MAX_SCAN_PAGE_ENTRIES
            || encoded_bytes > MAX_APPLICATION_EXPORT_SOURCE_PAGE_BYTES
            || continuation.as_ref().is_some_and(|value| {
                value.is_empty() || value.len() > MAX_APPLICATION_EXPORT_CONTINUATION_BYTES
            })
        {
            return Err(StorageValueError::LimitExceeded);
        }
        if records.iter().any(|record| record.class() != class)
            || exact_end == continuation.is_some()
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self {
            class,
            records,
            continuation,
            exact_end,
            encoded_bytes,
        })
    }

    /// Closed class returned by this page.
    #[must_use]
    pub const fn class(&self) -> ApplicationExportClassV1 {
        self.class
    }

    /// Policy-neutral source records.
    #[must_use]
    pub fn records(&self) -> &[ApplicationExportSourceRecordV1] {
        &self.records
    }

    /// Internal exact continuation; never crosses the public boundary.
    #[must_use]
    pub fn continuation(&self) -> Option<&[u8]> {
        self.continuation.as_deref()
    }

    /// Whether this class reached exact end at the captured snapshot.
    #[must_use]
    pub const fn exact_end(&self) -> bool {
        self.exact_end
    }

    /// Exact encoded source bytes charged by storage.
    #[must_use]
    pub const fn encoded_bytes(&self) -> usize {
        self.encoded_bytes
    }
}

/// Immutable published snapshot used only by the closed export record classes.
pub trait ApplicationExportSnapshotReader: Send + Sync {
    /// Exact database/history/frontier/application identity of this snapshot.
    fn binding(&self) -> &ApplicationExportSnapshotBindingV1;

    /// Reads one bounded ascending class page after an internal continuation.
    fn read_application_export_source_page(
        &self,
        class: ApplicationExportClassV1,
        after: Option<&[u8]>,
        limit: StorageScanLimit,
    ) -> Result<ApplicationExportSourcePageV1, StorageError>;
}

/// Captures one current immutable published snapshot after full validation.
pub trait ApplicationExportSnapshotPort: Send + Sync {
    /// Captures a snapshot bound to the exact active lineage identity.
    fn capture_application_export_snapshot(
        &self,
        lineage: &ContractLineage,
    ) -> Result<Arc<dyn ApplicationExportSnapshotReader>, StorageError>;
}

/// Opaque canonical durable progress bound to one operation and lineage.
#[derive(Clone, Eq, PartialEq)]
pub struct StoredApplicationExportOperationV1 {
    operation_id: ApplicationExportOperationId,
    lineage: ContractLineage,
    canonical_state: Vec<u8>,
}

impl StoredApplicationExportOperationV1 {
    /// Constructs one bounded identity-bound durable operation record.
    pub fn new(
        operation_id: ApplicationExportOperationId,
        lineage: ContractLineage,
        canonical_state: Vec<u8>,
    ) -> Result<Self, StorageValueError> {
        if canonical_state.is_empty() {
            return Err(StorageValueError::Empty);
        }
        if canonical_state.len() > MAX_APPLICATION_EXPORT_STATE_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self {
            operation_id,
            lineage,
            canonical_state,
        })
    }

    /// Caller-stable operation identity.
    #[must_use]
    pub const fn operation_id(&self) -> ApplicationExportOperationId {
        self.operation_id
    }

    /// Protected application lineage used for observation authorization.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Canonical service-owned checkpoint/receipt bytes.
    #[must_use]
    pub fn canonical_state(&self) -> &[u8] {
        &self.canonical_state
    }
}

impl fmt::Debug for StoredApplicationExportOperationV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredApplicationExportOperationV1")
            .field("operation_id", &self.operation_id)
            .field("lineage", &self.lineage)
            .field("canonical_state", &"[REDACTED]")
            .finish()
    }
}

/// Exact compare-and-swap result for one durable export checkpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationExportOperationWriteResultV1 {
    /// Replacement became durable.
    Applied,
    /// Exact replacement was already durable.
    Unchanged,
    /// Retained state did not match the exact predecessor.
    CompareMismatch,
}

/// Narrow durable repository for bounded export checkpoints and receipts.
pub trait ApplicationExportOperationRepository {
    /// Reads one exact operation by caller-stable identity.
    fn read_application_export_operation(
        &self,
        operation_id: ApplicationExportOperationId,
    ) -> Result<Option<StoredApplicationExportOperationV1>, StorageError>;

    /// Creates or exactly replaces one operation without a transaction callback.
    fn compare_and_swap_application_export_operation(
        &mut self,
        expected: Option<&StoredApplicationExportOperationV1>,
        replacement: &StoredApplicationExportOperationV1,
    ) -> Result<ApplicationExportOperationWriteResultV1, StorageError>;

    /// Lists a bounded startup-recovery inventory of retained operations.
    fn list_application_export_operations(
        &self,
        maximum: usize,
    ) -> Result<Vec<StoredApplicationExportOperationV1>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::ContractLineage;

    #[test]
    fn opaque_checkpoint_is_bounded_and_redacted() {
        let operation_id =
            ApplicationExportOperationId::from_unix_milliseconds_and_random(1, [0x61; 10])
                .expect("operation");
        let lineage = ContractLineage::new("TicketDesk").expect("lineage");
        assert!(
            StoredApplicationExportOperationV1::new(operation_id, lineage.clone(), Vec::new(),)
                .is_err()
        );
        let record = StoredApplicationExportOperationV1::new(
            operation_id,
            lineage,
            b"canonical-secret-bearing-state".to_vec(),
        )
        .expect("record");
        let debug = format!("{record:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("canonical-secret-bearing-state"));
    }
}
