//! Published durable snapshot adapter and bounded V3 receipt cursor.
//!
//! ADR-0207 keeps legacy framing in compatibility tests only. Application
//! export remains a separate supported surface, unchanged by this adapter.

use riffdb_storage_api::{CompositeTableV1, PublishedDurableSnapshot, StorageError};
use riffdb_types::{AdministrationSequence, CommitSequence};

/// A pinned published durable snapshot handed to the changelog emitter.
///
/// Constructed only at a publication site, from the exact read view that
/// publication installed.
pub(crate) struct RedbPublishedSnapshot {
    access: crate::store::RedbReadAccess,
}

impl RedbPublishedSnapshot {
    pub(crate) const fn new(access: crate::store::RedbReadAccess) -> Self {
        Self { access }
    }
}

impl PublishedDurableSnapshot for RedbPublishedSnapshot {
    fn changelog_receipts_v3(
        &self,
        lineage: riffdb_storage_api::ChangelogLineageV3,
        after: riffdb_storage_api::ChangelogHistoryPointV3,
    ) -> Result<
        Box<dyn riffdb_storage_api::ChangelogReceiptCursorV3>,
        riffdb_storage_api::ChangelogCursorErrorV3,
    > {
        crate::changelog_v3_cursor::open(&self.access, lineage, after)
    }

    fn read_value(
        &self,
        table: CompositeTableV1,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageError> {
        self.access.read_value(journal_table(table), key)
    }

    fn read_range(
        &self,
        table: CompositeTableV1,
        start_inclusive: &[u8],
        end_exclusive: &[u8],
        max_rows: usize,
    ) -> Result<Vec<riffdb_storage_api::CompositeRow>, StorageError> {
        self.access.read_range(
            journal_table(table),
            start_inclusive,
            end_exclusive,
            max_rows,
        )
    }

    fn application_frontier(&self) -> Result<Option<CommitSequence>, StorageError> {
        self.access.application_frontier()
    }

    fn administration_frontier(&self) -> Result<Option<AdministrationSequence>, StorageError> {
        self.access.administration_frontier()
    }
}

const fn journal_table(table: CompositeTableV1) -> crate::journal::JournalTable {
    match table {
        CompositeTableV1::Meta => crate::journal::JournalTable::Meta,
        CompositeTableV1::Entities => crate::journal::JournalTable::Entities,
        CompositeTableV1::SecondaryIndexes => crate::journal::JournalTable::SecondaryIndexes,
        CompositeTableV1::IndexEpochs => crate::journal::JournalTable::IndexEpochs,
        CompositeTableV1::Idempotency => crate::journal::JournalTable::Idempotency,
        CompositeTableV1::IdempotencyLocators => crate::journal::JournalTable::IdempotencyLocators,
        CompositeTableV1::ProvenanceLocators => crate::journal::JournalTable::ProvenanceLocators,
        CompositeTableV1::AuditByRequestLocators => {
            crate::journal::JournalTable::AuditByRequestLocators
        }
        CompositeTableV1::IdempotencyPending => crate::journal::JournalTable::IdempotencyPending,
        CompositeTableV1::Events => crate::journal::JournalTable::Events,
        CompositeTableV1::EventRoutes => crate::journal::JournalTable::EventRoutes,
        CompositeTableV1::Outbox => crate::journal::JournalTable::Outbox,
        CompositeTableV1::Provenance => crate::journal::JournalTable::Provenance,
        CompositeTableV1::Commits => crate::journal::JournalTable::Commits,
        CompositeTableV1::Audit => crate::journal::JournalTable::Audit,
        CompositeTableV1::AuditByRequest => crate::journal::JournalTable::AuditByRequest,
        CompositeTableV1::EntityChainHeads => crate::journal::JournalTable::EntityChainHeads,
        CompositeTableV1::VectorEvidence => crate::journal::JournalTable::VectorEvidence,
        CompositeTableV1::VectorObservations => crate::journal::JournalTable::VectorObservations,
        CompositeTableV1::VectorEvidenceIndex => crate::journal::JournalTable::VectorEvidenceIndex,
    }
}
