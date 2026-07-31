//! Exclusive read-only startup evidence over one immutable redb snapshot.

use std::collections::BTreeSet;
use std::fmt;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use redb::{
    Durability, MultimapTableHandle, Range, ReadOnlyTable, ReadTransaction, ReadableDatabase,
    ReadableTable, ReadableTableMetadata, TableDefinition, TableHandle,
};
use riffdb_catalog::{
    CatalogIndexMigrationApplied, CatalogIndexMigrationBackend, CatalogIndexMigrationBundleRequest,
    CatalogIndexMigrationBundleResponse, CatalogIndexMigrationCompletion,
    CatalogIndexMigrationInstruction, CatalogIndexMigrationPendingBatch, CatalogIndexMigrationScan,
    CatalogIndexMigrationScanRequest,
};
use riffdb_storage_api::{
    ApplicationSequenceAllocator, CapabilityLifecycleV1, DormantPortBundle, EvidencePageLimit,
    HistoricalActiveCatalogEvidence, HistoricalBundleBytes, HistoricalBundleEvidence,
    HistoricalCapabilityPartitionEvidenceV1, HistoricalEvidenceCursor, HistoricalEvidenceEnd,
    HistoricalEvidencePage, HistoricalPersistedKeyEvidenceV1, HistoricalSemanticEvidence,
    IndexMigrationCursor, MAX_RETAINED_QUERY_MODULES, OpenSessionId, ReadableDigestKey,
    RetainedMetadataV1, StartupIndexMigrationPort, StartupValidationInputs, StorageError,
    StorageErrorKind, StorageValueError, StructuralEvidenceCursor, StructuralEvidenceEnd,
    StructuralEvidenceOpen, StructuralEvidencePage, StructuralEvidenceSession, StructuralFinding,
    StructuralFindingCode, StructuralFindingScope, StructuralOpenOutcome, StructurallyOpened,
    UniqueIndexTarget, UniqueOccupancyKind,
};
use riffdb_types::{
    CommitSequence, ContractBundleHash, ContractLineage, ContractVersion, DatabaseId,
    FrontierPosition, hash_contract_bundle, hash_query_module,
};

use crate::codec::{self, IdempotencyRecordV1};
use crate::error::{precommit_storage_error, storage_error, table_error, transaction_error};
use crate::gate::ExclusiveLease;
use crate::hooks::RedbTestOperation;
use crate::keys;
use crate::layout::{
    AUDIT, AUDIT_BY_REQUEST, CAPABILITIES, CAPABILITY_TOKENS, CATALOG_ACTIVE, CATALOG_ACTIVE_KEY,
    COMMITS, CONTRACT_BUNDLES, ENTITIES, EVENTS, IDEMPOTENCY, IDEMPOTENCY_PENDING, INDEX_EPOCHS,
    META, META_ADMINISTRATION_SEQUENCE, META_APPLICATION_SEQUENCE, META_CAPABILITY_BOOTSTRAP,
    META_DATABASE_ID, META_FORMAT_VERSION, META_HISTORY_INCARNATION,
    META_INDEX_EPOCH_ROWS_REPAIRED, META_KEYS, META_RECORD_REGISTRY, OUTBOX, OUTBOX_STATUS,
    PROJECTION_APPLIED, PROJECTION_FRONTIER, PROJECTION_STATE, PROVENANCE, QUERY_MODULE_ACTIVE,
    QUERY_MODULES, SECONDARY_INDEXES, TABLE_NAMES,
};
use crate::store::{
    PRE_AUDIT_REQUEST_INDEX_REGISTRY_DIGEST, PRE_HISTORY_INCARNATION_REGISTRY_DIGEST,
    PRE_INDEX_GENERATION_REGISTRY_DIGEST, RedbDormantPorts, RedbStore, SharedRedb,
};

static NEXT_OPEN_SESSION: AtomicU64 = AtomicU64::new(1);
const STRUCTURAL_TABLE_COUNT: usize = 22;

fn startup_registry_is_supported(digest: riffdb_types::SchemaHash) -> bool {
    digest == riffdb_storage_api::proto_codec::current_record_registry_digest()
        || digest == riffdb_types::SchemaHash::from_bytes(PRE_AUDIT_REQUEST_INDEX_REGISTRY_DIGEST)
        || digest == riffdb_types::SchemaHash::from_bytes(PRE_HISTORY_INCARNATION_REGISTRY_DIGEST)
        || digest == riffdb_types::SchemaHash::from_bytes(PRE_INDEX_GENERATION_REGISTRY_DIGEST)
}

fn exclusive_prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    let position = end.iter().rposition(|byte| *byte != u8::MAX)?;
    end[position] = end[position].checked_add(1)?;
    end.truncate(position + 1);
    Some(end)
}

/// Redb authority whose constructor is private to a completed startup session.
pub struct RedbCompletionAuthority {
    _private: (),
}

/// Unforgeable exact-end token for the redb structural stream.
#[derive(Debug)]
pub struct RedbStructuralEvidenceEnd {
    cursor: StructuralEvidenceCursor,
}

/// Unforgeable exact-end token for the redb historical stream.
#[derive(Debug)]
pub struct RedbHistoricalEvidenceEnd {
    cursor: HistoricalEvidenceCursor,
}

/// Exclusive redb migration capability released only by a V1-observing startup pass.
pub struct RedbStartupIndexMigrationPort {
    shared: Arc<SharedRedb>,
    lease: ExclusiveLease,
    database_id: DatabaseId,
    open_session_id: OpenSessionId,
    next_cursor: IndexMigrationCursor,
    after: Option<riffdb_types::IndexEntryKey>,
    #[cfg(test)]
    substitute_before_apply: Option<riffdb_storage_api::StoredIndexEntryV2>,
}

/// Persistent forward-only structural table cursors (linear scan).
struct StructuralCursors {
    /// Table phase: 0 = META, 1.. = BYTE_TABLES[phase-1] / structural phase+1 for inspect.
    phase: usize,
    consumed_in_phase: u64,
    meta: Option<Range<'static, &'static str, &'static [u8]>>,
    bytes: Option<Range<'static, &'static [u8], &'static [u8]>>,
}

/// Compact locator for one historical evidence item; re-materialized on page serve.
#[derive(Debug)]
enum EvidenceLocator {
    /// Taken once when served; plan build stores full evidence for order fidelity.
    Materialized(Option<HistoricalSemanticEvidence>),
}

struct HistoricalEvidencePlan {
    /// Sorted unique (order_key, locator) pairs.
    entries: Vec<(Vec<u8>, EvidenceLocator)>,
    /// Next absolute index into `entries` to serve.
    next_index: usize,
}

/// One exclusive startup session bound to a single immutable redb snapshot.
pub struct RedbStructuralEvidenceSession {
    shared: Arc<SharedRedb>,
    lease: Option<ExclusiveLease>,
    durable_commit_epoch: u64,
    database_id: DatabaseId,
    open_session_id: OpenSessionId,
    retained_metadata: RetainedMetadataV1,
    inputs: StartupValidationInputs,
    structural_counts: [u64; STRUCTURAL_TABLE_COUNT],
    structural_total: u64,
    next_structural: StructuralEvidenceCursor,
    next_historical: HistoricalEvidenceCursor,
    last_historical_key: Option<Vec<u8>>,
    structural_finished: bool,
    historical_finished: bool,
    authoritative_finding_seen: bool,
    saw_v1_index: bool,
    /// Held for the structural evidence pass only; must not outlive the session.
    structural_read: Option<ReadTransaction>,
    structural_cursors: Option<StructuralCursors>,
    historical_plan: Option<HistoricalEvidencePlan>,
}

impl fmt::Debug for RedbStructuralEvidenceSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedbStructuralEvidenceSession")
            .field("database_id", &self.database_id)
            .field("open_session_id", &self.open_session_id)
            .field("state", &"[EXCLUSIVE READ SNAPSHOT]")
            .finish()
    }
}

impl DormantPortBundle for RedbDormantPorts {
    type CompletionAuthority = RedbCompletionAuthority;
}

impl StructuralEvidenceEnd for RedbStructuralEvidenceEnd {
    fn cursor(&self) -> StructuralEvidenceCursor {
        self.cursor
    }
}

impl HistoricalEvidenceEnd for RedbHistoricalEvidenceEnd {
    fn cursor(&self) -> HistoricalEvidenceCursor {
        self.cursor
    }
}

impl StartupIndexMigrationPort for RedbStartupIndexMigrationPort {
    fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    fn open_session_id(&self) -> OpenSessionId {
        self.open_session_id
    }
}

impl CatalogIndexMigrationBackend for RedbStartupIndexMigrationPort {
    type Output = RedbStore;

    fn read_index_migration_page(
        self,
        request: CatalogIndexMigrationScanRequest<Self>,
    ) -> Result<CatalogIndexMigrationScan<Self>, StorageError> {
        let cursor = request.cursor();
        if cursor != self.next_cursor {
            return Err(invariant());
        }

        let transaction = self
            .shared
            .database
            .begin_read()
            .map_err(transaction_error)?;
        let table = transaction
            .open_table(SECONDARY_INDEXES)
            .map_err(table_error)?;
        let lower = self
            .after
            .as_ref()
            .map_or(Unbounded, |key| Excluded(key.as_bytes()));
        let mut scan = table
            .range::<&[u8]>((lower, Unbounded))
            .map_err(precommit_storage_error)?;
        let mut rows = Vec::new();
        let mut evidence_bytes = 0usize;
        let mut instruction_bytes = 0usize;
        let mut exhausted = true;

        for entry in &mut scan {
            let (physical_key, envelope) = entry.map_err(precommit_storage_error)?;
            let physical =
                keys::decode_index_entry_key(physical_key.value()).map_err(|_| corrupt())?;
            let evidence = codec::decode_index_migration_row(&physical, envelope.value())?;
            let next_evidence = evidence_bytes
                .checked_add(evidence.evidence_page_charge())
                .ok_or_else(limit_exceeded)?;
            let next_instruction = instruction_bytes
                .checked_add(evidence.instruction_page_charge())
                .ok_or_else(limit_exceeded)?;
            if rows.len() == riffdb_storage_api::MAX_INDEX_MIGRATION_PAGE_ENTRIES
                || next_evidence > riffdb_storage_api::MAX_INDEX_MIGRATION_PAGE_BYTES
                || next_instruction > riffdb_storage_api::MAX_INDEX_MIGRATION_PAGE_BYTES
            {
                if rows.is_empty() {
                    return Err(limit_exceeded());
                }
                exhausted = false;
                break;
            }
            evidence_bytes = next_evidence;
            instruction_bytes = next_instruction;
            rows.push(evidence);
        }
        drop(scan);
        drop(table);
        drop(transaction);

        if rows.is_empty() {
            if !exhausted {
                return Err(invariant());
            }
            return request.exact_end(self).map_err(value_error_as_storage);
        }

        let count = u64::try_from(rows.len()).map_err(|_| limit_exceeded())?;
        let next = cursor.advanced(count).map_err(value_error_as_storage)?;
        let after = rows.last().ok_or_else(invariant)?.physical_key().clone();
        let mut port = self;
        port.next_cursor = next;
        port.after = Some(after);
        port.shared.observe_index_migration_page();
        request
            .page(port, rows, next)
            .map_err(value_error_as_storage)
    }

    fn read_historical_bundle(
        self,
        request: CatalogIndexMigrationBundleRequest<Self>,
    ) -> Result<CatalogIndexMigrationBundleResponse<Self>, StorageError> {
        let binding = request.evidence().row().schema_binding();
        let lineage = binding.lineage().clone();
        let version = binding.contract_version();
        let bundle_hash = binding.bundle_hash();
        let transaction = self
            .shared
            .database
            .begin_read()
            .map_err(transaction_error)?;
        let bundle = read_historical_bundle(&transaction, &lineage, version, bundle_hash)?
            .ok_or_else(corrupt)?;
        drop(transaction);
        request
            .respond(self, bundle)
            .map_err(value_error_as_storage)
    }

    fn apply_index_migration_batch(
        self,
        pending: CatalogIndexMigrationPendingBatch<Self>,
    ) -> Result<CatalogIndexMigrationApplied<Self>, StorageError> {
        let batch = pending.batch();
        if self.next_cursor != batch.next()
            || batch.next().database_id() != self.database_id
            || batch.next().open_session_id() != self.open_session_id
        {
            return Err(invariant());
        }
        #[cfg(test)]
        if let Some(replacement) = &self.substitute_before_apply {
            apply_index_migration_substitution_fixture(&self.shared, replacement)?;
        }
        let (v1_rewrites, v2_confirms) =
            batch
                .instructions()
                .iter()
                .fold(
                    (0usize, 0usize),
                    |(v1, v2), instruction| match instruction {
                        CatalogIndexMigrationInstruction::V1Rewrite(_) => (v1 + 1, v2),
                        CatalogIndexMigrationInstruction::V2Confirm(_) => (v1, v2 + 1),
                    },
                );
        let mut transaction = self
            .shared
            .database
            .begin_write()
            .map_err(transaction_error)?;
        transaction.set_two_phase_commit(true);
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| invariant())?;
        {
            let mut table = transaction
                .open_table(SECONDARY_INDEXES)
                .map_err(table_error)?;
            for instruction in batch.instructions() {
                apply_index_migration_instruction(&mut table, instruction)?;
            }
        }
        self.shared
            .before_test_commit(RedbTestOperation::IndexMigrationBatch)?;
        self.shared.commit_durable(transaction)?;
        self.shared
            .after_test_commit(RedbTestOperation::IndexMigrationBatch)?;
        self.shared
            .observe_index_migration_batch(v1_rewrites, v2_confirms);
        pending.applied(self).map_err(value_error_as_storage)
    }

    fn finish_index_migration(
        self,
        completion: CatalogIndexMigrationCompletion<Self>,
    ) -> Result<RedbStore, StorageError> {
        if completion.final_cursor() != self.next_cursor {
            return Err(invariant());
        }
        let RedbStartupIndexMigrationPort {
            shared,
            lease,
            database_id: _,
            open_session_id: _,
            next_cursor: _,
            after: _,
            #[cfg(test)]
                substitute_before_apply: _,
        } = self;
        drop(lease);
        let store = RedbStore { shared };
        store.complete_partition_index_generation_migration()?;
        Ok(store)
    }
}

#[cfg(test)]
fn apply_index_migration_substitution_fixture(
    shared: &SharedRedb,
    replacement: &riffdb_storage_api::StoredIndexEntryV2,
) -> Result<(), StorageError> {
    let encoded = codec::encode_index_entry_v2(replacement)?;
    let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
    transaction.set_two_phase_commit(true);
    transaction
        .set_durability(Durability::Immediate)
        .map_err(|_| invariant())?;
    {
        let mut table = transaction
            .open_table(SECONDARY_INDEXES)
            .map_err(table_error)?;
        table
            .insert(replacement.key().as_bytes(), encoded.as_bytes())
            .map_err(precommit_storage_error)?;
    }
    shared.commit_durable(transaction)
}

fn apply_index_migration_instruction(
    table: &mut redb::Table<'_, &'static [u8], &'static [u8]>,
    instruction: &CatalogIndexMigrationInstruction,
) -> Result<(), StorageError> {
    let expected = instruction.expected();
    let key = expected.physical_key().as_bytes();
    let current = table
        .get(key)
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    let current_bytes = current.value();
    match instruction {
        CatalogIndexMigrationInstruction::V1Rewrite(rewrite) => {
            if !rewrite.expected().row().is_v1() {
                return Err(invariant());
            }
            let replacement = codec::encode_index_entry_v2(rewrite.replacement())?;
            if replacement.encoded_content_charge().get()
                > expected.conservative_v2_envelope_charge().get()
            {
                return Err(invariant());
            }
            if current_bytes == rewrite.expected().canonical_envelope() {
                drop(current);
                table
                    .insert(key, replacement.as_bytes())
                    .map_err(precommit_storage_error)?;
            } else if current_bytes != replacement.as_bytes() {
                return Err(corrupt());
            }
        }
        CatalogIndexMigrationInstruction::V2Confirm(confirm) => {
            if confirm.expected().row().is_v1()
                || current_bytes != confirm.expected().canonical_envelope()
            {
                return Err(corrupt());
            }
        }
    }
    Ok(())
}

impl StructuralEvidenceOpen for RedbStore {
    type Session = RedbStructuralEvidenceSession;

    fn begin_structural_evidence(
        self,
        inputs: StartupValidationInputs,
    ) -> Result<Self::Session, StorageError> {
        let lease = self.acquire_mutation_lease()?;
        let durable_commit_epoch = self.shared.durable_commit_epoch();
        let transaction = self
            .shared
            .database
            .begin_read()
            .map_err(transaction_error)?;
        let snapshot = collect_startup_snapshot(&transaction)?;
        if self.shared.durable_commit_epoch() != durable_commit_epoch {
            return Err(corrupt());
        }
        let database_id = snapshot.retained_metadata.database_id();
        let open_session_id = allocate_open_session()?;
        Ok(Self::Session {
            shared: Arc::clone(&self.shared),
            lease: Some(lease),
            durable_commit_epoch,
            database_id,
            open_session_id,
            retained_metadata: snapshot.retained_metadata,
            inputs,
            structural_counts: snapshot.structural_counts,
            structural_total: snapshot.structural_total,
            next_structural: StructuralEvidenceCursor::start(database_id, open_session_id),
            next_historical: HistoricalEvidenceCursor::start(database_id, open_session_id),
            last_historical_key: None,
            structural_finished: false,
            historical_finished: false,
            authoritative_finding_seen: false,
            saw_v1_index: false,
            // Hold one read transaction for the structural pass (savepoint pin).
            structural_read: Some(transaction),
            structural_cursors: None,
            historical_plan: None,
        })
    }
}

impl StructuralEvidenceSession for RedbStructuralEvidenceSession {
    type DormantPorts = RedbDormantPorts;
    type StructuralEnd = RedbStructuralEvidenceEnd;
    type HistoricalEnd = RedbHistoricalEvidenceEnd;
    type MigrationPort = RedbStartupIndexMigrationPort;

    fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    fn open_session_id(&self) -> OpenSessionId {
        self.open_session_id
    }

    fn read_structural_evidence(
        &mut self,
        cursor: StructuralEvidenceCursor,
        limit: EvidencePageLimit,
    ) -> Result<StructuralEvidencePage<Self::StructuralEnd>, StorageError> {
        if self.structural_finished || cursor != self.next_structural {
            return Err(invariant());
        }
        if cursor.position() > self.structural_total {
            return Err(invariant());
        }
        self.ensure_structural_continuity()?;
        if cursor.position() == self.structural_total {
            self.structural_finished = true;
            self.structural_cursors = None;
            self.structural_read = None;
            return Ok(StructuralEvidencePage::ExactEnd(
                RedbStructuralEvidenceEnd { cursor },
            ));
        }

        let finding_bound = u64::try_from(riffdb_storage_api::MAX_INTEGRITY_FINDINGS)
            .map_err(|_| limit_exceeded())?;
        let inspected = (self.structural_total - cursor.position())
            .min(u64::from(limit.get()))
            .min(finding_bound);
        let mut findings = Vec::new();
        for offset in 0..inspected {
            let position = cursor
                .position()
                .checked_add(offset)
                .ok_or_else(limit_exceeded)?;
            if let Some(finding) = self.inspect_structural_forward(position)? {
                if finding.scope() == StructuralFindingScope::Authoritative {
                    self.authoritative_finding_seen = true;
                }
                findings.push(finding);
            }
        }
        let next = cursor.advanced(inspected).map_err(value_error_as_storage)?;
        let page =
            StructuralEvidencePage::page(cursor, findings, next).map_err(value_error_as_storage)?;
        self.next_structural = next;
        if next.position() == self.structural_total {
            // Allow releasing the structural savepoint pin after the final page is
            // prepared; exact-end is still returned on the next call.
        }
        Ok(page)
    }

    fn read_historical_evidence(
        &mut self,
        cursor: HistoricalEvidenceCursor,
        limit: EvidencePageLimit,
    ) -> Result<HistoricalEvidencePage<Self::HistoricalEnd>, StorageError> {
        if self.historical_finished || cursor != self.next_historical {
            return Err(invariant());
        }
        // Structural pin is released once structural finishes; historical uses fresh reads.
        if self.historical_plan.is_none() {
            let transaction = self.open_snapshot_read()?;
            self.historical_plan =
                Some(build_historical_evidence_plan(&transaction, &self.inputs)?);
        }
        let plan = self.historical_plan.as_mut().ok_or_else(invariant)?;
        let requested = usize::try_from(limit.get()).map_err(|_| limit_exceeded())?;
        if plan.next_index >= plan.entries.len() {
            self.historical_finished = true;
            return Ok(HistoricalEvidencePage::ExactEnd(
                RedbHistoricalEvidenceEnd { cursor },
            ));
        }
        let mut evidence = Vec::new();
        let mut bytes = 0usize;
        let mut migration_rows = 0usize;
        let mut migration_evidence_bytes = 0usize;
        let mut migration_instruction_bytes = 0usize;
        let mut last_key = self.last_historical_key.clone();
        while evidence.len() < requested && plan.next_index < plan.entries.len() {
            let locator = &plan.entries[plan.next_index].1;
            let item_ref = peek_historical_evidence(locator)?;
            let next_bytes = bytes
                .checked_add(historical_semantic_bytes(item_ref)?)
                .ok_or_else(limit_exceeded)?;
            if next_bytes > riffdb_storage_api::MAX_HISTORICAL_EVIDENCE_PAGE_BYTES {
                if evidence.is_empty() {
                    return Err(limit_exceeded());
                }
                break;
            }
            if let HistoricalSemanticEvidence::IndexMigrationRow(row) = item_ref {
                let next_rows = migration_rows.checked_add(1).ok_or_else(limit_exceeded)?;
                let next_evidence_bytes = migration_evidence_bytes
                    .checked_add(row.evidence_page_charge())
                    .ok_or_else(limit_exceeded)?;
                let next_instruction_bytes = migration_instruction_bytes
                    .checked_add(row.instruction_page_charge())
                    .ok_or_else(limit_exceeded)?;
                if next_rows > riffdb_storage_api::MAX_INDEX_MIGRATION_PAGE_ENTRIES
                    || next_evidence_bytes > riffdb_storage_api::MAX_INDEX_MIGRATION_PAGE_BYTES
                    || next_instruction_bytes > riffdb_storage_api::MAX_INDEX_MIGRATION_PAGE_BYTES
                {
                    if evidence.is_empty() {
                        return Err(limit_exceeded());
                    }
                    break;
                }
                migration_rows = next_rows;
                migration_evidence_bytes = next_evidence_bytes;
                migration_instruction_bytes = next_instruction_bytes;
            }
            let (order_key, locator) = &mut plan.entries[plan.next_index];
            let order_key = order_key.clone();
            let item = take_historical_evidence(locator)?;
            bytes = next_bytes;
            last_key = Some(order_key);
            if matches!(
                &item,
                HistoricalSemanticEvidence::IndexMigrationRow(row) if row.row().is_v1()
            ) {
                self.saw_v1_index = true;
            }
            evidence.push(item);
            plan.next_index = plan.next_index.checked_add(1).ok_or_else(limit_exceeded)?;
        }
        if evidence.is_empty() {
            self.historical_finished = true;
            return Ok(HistoricalEvidencePage::ExactEnd(
                RedbHistoricalEvidenceEnd { cursor },
            ));
        }
        let count = u64::try_from(evidence.len()).map_err(|_| limit_exceeded())?;
        let next = cursor.advanced(count).map_err(value_error_as_storage)?;
        let page =
            HistoricalEvidencePage::page(cursor, evidence, next).map_err(value_error_as_storage)?;
        self.last_historical_key = last_key;
        self.next_historical = next;
        Ok(page)
    }

    fn read_historical_bundle(
        &mut self,
        lineage: &ContractLineage,
        version: ContractVersion,
        hash: ContractBundleHash,
    ) -> Result<Option<HistoricalBundleEvidence>, StorageError> {
        let transaction = self.open_snapshot_read()?;
        read_historical_bundle(&transaction, lineage, version, hash)
    }

    fn read_integrity_entity(
        &mut self,
        target: &riffdb_storage_api::EntityTarget,
    ) -> Result<Option<riffdb_storage_api::StoredEntityRecordV1>, StorageError> {
        let transaction = self.open_snapshot_read()?;
        let table = transaction.open_table(ENTITIES).map_err(table_error)?;
        crate::reads::read_entity_record(&table, target)
    }

    fn read_integrity_unique_occupancy(
        &mut self,
        target: &UniqueIndexTarget,
    ) -> Result<UniqueOccupancyKind, StorageError> {
        let transaction = self.open_snapshot_read()?;
        let table = transaction
            .open_table(SECONDARY_INDEXES)
            .map_err(table_error)?;
        let prefix = target.prefix().prefix().as_bytes();
        let upper = exclusive_prefix_end(prefix).ok_or_else(corrupt)?;
        let mut range = table
            .range::<&[u8]>((Included(prefix), Excluded(upper.as_slice())))
            .map_err(precommit_storage_error)?;
        let first = range
            .next()
            .transpose()
            .map_err(precommit_storage_error)?
            .map(|(key, _)| key.value().to_vec());
        let second = range.next().transpose().map_err(precommit_storage_error)?;
        match (first, second) {
            (None, None) => Ok(UniqueOccupancyKind::Vacant),
            (Some(key), None) if key.as_slice() == target.expected_entry().as_bytes() => {
                Ok(UniqueOccupancyKind::Owned)
            }
            (Some(_), None) => Ok(UniqueOccupancyKind::Conflict),
            _ => Err(corrupt()),
        }
    }

    fn finish(
        mut self,
        structural_end: Self::StructuralEnd,
        historical_end: Self::HistoricalEnd,
    ) -> Result<StructuralOpenOutcome<Self::DormantPorts, Self::MigrationPort>, StorageError> {
        if !self.structural_finished
            || !self.historical_finished
            || structural_end.cursor != self.next_structural
            || historical_end.cursor != self.next_historical
        {
            return Err(invariant());
        }
        if self.authoritative_finding_seen {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        drop(self.open_snapshot_read()?);
        let lease = self.lease.take().ok_or_else(invariant)?;
        if self.saw_v1_index {
            let next_cursor = IndexMigrationCursor::start(self.database_id, self.open_session_id);
            Ok(StructuralOpenOutcome::MigrationRequired(
                RedbStartupIndexMigrationPort {
                    shared: Arc::clone(&self.shared),
                    lease,
                    database_id: self.database_id,
                    open_session_id: self.open_session_id,
                    next_cursor,
                    after: None,
                    #[cfg(test)]
                    substitute_before_apply: None,
                },
            ))
        } else {
            drop(lease);
            Ok(StructuralOpenOutcome::Clean(
                StructurallyOpened::from_finished_session(
                    self.database_id,
                    self.open_session_id,
                    self.retained_metadata,
                    RedbDormantPorts {
                        shared: Arc::clone(&self.shared),
                    },
                    RedbCompletionAuthority { _private: () },
                ),
            ))
        }
    }
}

impl RedbStructuralEvidenceSession {
    fn open_snapshot_read(&self) -> Result<ReadTransaction, StorageError> {
        if self.shared.durable_commit_epoch() != self.durable_commit_epoch {
            return Err(corrupt());
        }
        let transaction = self
            .shared
            .database
            .begin_read()
            .map_err(transaction_error)?;
        // O(1) continuity: recount table lengths instead of full re-decode snapshot.
        self.verify_structural_counts(&transaction)?;
        if self.shared.durable_commit_epoch() != self.durable_commit_epoch {
            return Err(corrupt());
        }
        Ok(transaction)
    }

    fn ensure_structural_continuity(&self) -> Result<(), StorageError> {
        if self.shared.durable_commit_epoch() != self.durable_commit_epoch {
            return Err(corrupt());
        }
        let Some(transaction) = self.structural_read.as_ref() else {
            return Err(invariant());
        };
        self.verify_structural_counts(transaction)
    }

    fn verify_structural_counts(&self, transaction: &ReadTransaction) -> Result<(), StorageError> {
        let meta = transaction.open_table(META).map_err(table_error)?;
        let mut counts = [0u64; STRUCTURAL_TABLE_COUNT];
        counts[0] = meta.len().map_err(precommit_storage_error)?;
        drop(meta);
        for (index, definition) in [
            CONTRACT_BUNDLES,
            CATALOG_ACTIVE,
            QUERY_MODULES,
            QUERY_MODULE_ACTIVE,
            ENTITIES,
            SECONDARY_INDEXES,
            INDEX_EPOCHS,
            IDEMPOTENCY,
            IDEMPOTENCY_PENDING,
            COMMITS,
            PROVENANCE,
            EVENTS,
            OUTBOX,
            OUTBOX_STATUS,
            PROJECTION_STATE,
            PROJECTION_FRONTIER,
            PROJECTION_APPLIED,
            CAPABILITIES,
            CAPABILITY_TOKENS,
            AUDIT,
            AUDIT_BY_REQUEST,
        ]
        .into_iter()
        .enumerate()
        {
            counts[index + 1] = table_len(transaction, definition)?;
        }
        if counts != self.structural_counts {
            return Err(corrupt());
        }
        Ok(())
    }

    fn inspect_structural_forward(
        &mut self,
        position: u64,
    ) -> Result<Option<StructuralFinding>, StorageError> {
        // Positions are always requested in strictly ascending order by the page loop.
        if position == 0 {
            let transaction = self.structural_read.as_ref().ok_or_else(invariant)?;
            return inspect_header(transaction, self.database_id);
        }
        if self.structural_cursors.is_none() {
            self.structural_cursors = Some(StructuralCursors {
                phase: 0,
                consumed_in_phase: 0,
                meta: None,
                bytes: None,
            });
        }
        let (phase, index, key, value) = self.next_structural_row_raw()?;
        // Sanity: absolute position maps to this phase/index.
        let mut relative = position - 1;
        let mut expected_phase = 0usize;
        for (phase_idx, count) in self.structural_counts.iter().copied().enumerate() {
            if relative < count {
                expected_phase = phase_idx;
                break;
            }
            relative = relative.checked_sub(count).ok_or_else(invariant)?;
        }
        if phase != expected_phase || index != relative {
            return Err(invariant());
        }
        let transaction = self.structural_read.as_ref().ok_or_else(invariant)?;
        if phase == 0 {
            let key = std::str::from_utf8(&key).map_err(|_| corrupt())?;
            return Ok(inspect_meta_row(key, &value, self.database_id));
        }
        inspect_table_row_from_bytes(
            transaction,
            &self.inputs,
            self.database_id,
            phase,
            index,
            &key,
            &value,
        )
    }

    fn next_structural_row_raw(&mut self) -> Result<(usize, u64, Vec<u8>, Vec<u8>), StorageError> {
        loop {
            let phase = {
                let cursors = self.structural_cursors.as_ref().ok_or_else(invariant)?;
                cursors.phase
            };
            if phase >= STRUCTURAL_TABLE_COUNT {
                return Err(invariant());
            }
            if phase == 0 {
                let need_open = self
                    .structural_cursors
                    .as_ref()
                    .ok_or_else(invariant)?
                    .meta
                    .is_none();
                if need_open {
                    let transaction = self.structural_read.as_ref().ok_or_else(invariant)?;
                    let table: ReadOnlyTable<&'static str, &'static [u8]> =
                        transaction.open_table(META).map_err(table_error)?;
                    let range = table.range::<&str>(..).map_err(precommit_storage_error)?;
                    self.structural_cursors.as_mut().ok_or_else(invariant)?.meta = Some(range);
                }
                let next = {
                    let cursors = self.structural_cursors.as_mut().ok_or_else(invariant)?;
                    cursors
                        .meta
                        .as_mut()
                        .ok_or_else(invariant)?
                        .next()
                        .transpose()
                        .map_err(precommit_storage_error)?
                };
                if let Some((key, value)) = next {
                    let cursors = self.structural_cursors.as_mut().ok_or_else(invariant)?;
                    let index = cursors.consumed_in_phase;
                    cursors.consumed_in_phase = cursors
                        .consumed_in_phase
                        .checked_add(1)
                        .ok_or_else(limit_exceeded)?;
                    return Ok((
                        0,
                        index,
                        key.value().as_bytes().to_vec(),
                        value.value().to_vec(),
                    ));
                }
                let cursors = self.structural_cursors.as_mut().ok_or_else(invariant)?;
                cursors.meta = None;
                cursors.phase = 1;
                cursors.consumed_in_phase = 0;
                continue;
            }
            let need_open = self
                .structural_cursors
                .as_ref()
                .ok_or_else(invariant)?
                .bytes
                .is_none();
            if need_open {
                let transaction = self.structural_read.as_ref().ok_or_else(invariant)?;
                let table: ReadOnlyTable<&'static [u8], &'static [u8]> = match phase {
                    1 => transaction
                        .open_table(CONTRACT_BUNDLES)
                        .map_err(table_error)?,
                    2 => transaction
                        .open_table(CATALOG_ACTIVE)
                        .map_err(table_error)?,
                    3 => transaction.open_table(QUERY_MODULES).map_err(table_error)?,
                    4 => transaction
                        .open_table(QUERY_MODULE_ACTIVE)
                        .map_err(table_error)?,
                    5 => transaction.open_table(ENTITIES).map_err(table_error)?,
                    6 => transaction
                        .open_table(SECONDARY_INDEXES)
                        .map_err(table_error)?,
                    7 => transaction.open_table(INDEX_EPOCHS).map_err(table_error)?,
                    8 => transaction.open_table(IDEMPOTENCY).map_err(table_error)?,
                    9 => transaction
                        .open_table(IDEMPOTENCY_PENDING)
                        .map_err(table_error)?,
                    10 => transaction.open_table(COMMITS).map_err(table_error)?,
                    11 => transaction.open_table(PROVENANCE).map_err(table_error)?,
                    12 => transaction.open_table(EVENTS).map_err(table_error)?,
                    13 => transaction.open_table(OUTBOX).map_err(table_error)?,
                    14 => transaction.open_table(OUTBOX_STATUS).map_err(table_error)?,
                    15 => transaction
                        .open_table(PROJECTION_STATE)
                        .map_err(table_error)?,
                    16 => transaction
                        .open_table(PROJECTION_FRONTIER)
                        .map_err(table_error)?,
                    17 => transaction
                        .open_table(PROJECTION_APPLIED)
                        .map_err(table_error)?,
                    18 => transaction.open_table(CAPABILITIES).map_err(table_error)?,
                    19 => transaction
                        .open_table(CAPABILITY_TOKENS)
                        .map_err(table_error)?,
                    20 => transaction.open_table(AUDIT).map_err(table_error)?,
                    21 => transaction
                        .open_table(AUDIT_BY_REQUEST)
                        .map_err(table_error)?,
                    _ => return Err(invariant()),
                };
                let range = table.range::<&[u8]>(..).map_err(precommit_storage_error)?;
                self.structural_cursors
                    .as_mut()
                    .ok_or_else(invariant)?
                    .bytes = Some(range);
            }
            let next = {
                let cursors = self.structural_cursors.as_mut().ok_or_else(invariant)?;
                cursors
                    .bytes
                    .as_mut()
                    .ok_or_else(invariant)?
                    .next()
                    .transpose()
                    .map_err(precommit_storage_error)?
            };
            if let Some((key, value)) = next {
                let cursors = self.structural_cursors.as_mut().ok_or_else(invariant)?;
                let index = cursors.consumed_in_phase;
                let phase = cursors.phase;
                cursors.consumed_in_phase = cursors
                    .consumed_in_phase
                    .checked_add(1)
                    .ok_or_else(limit_exceeded)?;
                return Ok((phase, index, key.value().to_vec(), value.value().to_vec()));
            }
            let cursors = self.structural_cursors.as_mut().ok_or_else(invariant)?;
            cursors.bytes = None;
            cursors.phase = cursors.phase.checked_add(1).ok_or_else(invariant)?;
            cursors.consumed_in_phase = 0;
        }
    }
}

#[allow(dead_code)]
fn inspect_table_row_from_bytes(
    transaction: &ReadTransaction,
    inputs: &StartupValidationInputs,
    database_id: DatabaseId,
    phase: usize,
    index: u64,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    match phase {
        1 => inspect_bundle_row(transaction, key, value),
        2 => inspect_active_row(transaction, key, value),
        3 => inspect_query_module_row(transaction, key, value),
        4 => inspect_active_query_module_row(transaction, key, value),
        5 => inspect_entity_row(transaction, key, value),
        6 => inspect_index_row(transaction, key, value),
        7 => inspect_epoch_row(transaction, key, value),
        8 => inspect_terminal_row(transaction, inputs, database_id, key, value),
        9 => inspect_pending_row(transaction, inputs, database_id, key, value),
        10 => inspect_commit_row(transaction, index, key, value),
        11 => inspect_provenance_row(transaction, key, value),
        12 => inspect_event_row(transaction, key, value),
        13 => inspect_outbox_row(transaction, key, value),
        14 => inspect_outbox_status_row(transaction, key, value),
        15 => inspect_projection_state_row(transaction, key, value),
        16 => inspect_projection_control_row(transaction, key, value),
        17 => inspect_projection_apply_row(transaction, key, value),
        18 => inspect_capability_row(transaction, inputs, database_id, key, value),
        19 => inspect_capability_lookup_row(transaction, key, value),
        20 => inspect_audit_row(transaction, index, key, value),
        21 => inspect_audit_by_request_row(transaction, key, value),
        _ => Err(invariant()),
    }
}

fn allocate_open_session() -> Result<OpenSessionId, StorageError> {
    let value = NEXT_OPEN_SESSION
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(1)
        })
        .map_err(|_| storage_error(StorageErrorKind::SequenceExhausted))?;
    OpenSessionId::new(value).ok_or_else(|| storage_error(StorageErrorKind::SequenceExhausted))
}

struct StartupSnapshot {
    retained_metadata: RetainedMetadataV1,
    structural_counts: [u64; STRUCTURAL_TABLE_COUNT],
    structural_total: u64,
}

fn collect_startup_snapshot(
    transaction: &ReadTransaction,
) -> Result<StartupSnapshot, StorageError> {
    validate_table_inventory(transaction)?;
    let retained_metadata = read_retained_metadata(transaction)?;
    let meta = transaction.open_table(META).map_err(table_error)?;
    let counts = [
        meta.len().map_err(precommit_storage_error)?,
        table_len(transaction, CONTRACT_BUNDLES)?,
        table_len(transaction, CATALOG_ACTIVE)?,
        table_len(transaction, QUERY_MODULES)?,
        table_len(transaction, QUERY_MODULE_ACTIVE)?,
        table_len(transaction, ENTITIES)?,
        table_len(transaction, SECONDARY_INDEXES)?,
        table_len(transaction, INDEX_EPOCHS)?,
        table_len(transaction, IDEMPOTENCY)?,
        table_len(transaction, IDEMPOTENCY_PENDING)?,
        table_len(transaction, COMMITS)?,
        table_len(transaction, PROVENANCE)?,
        table_len(transaction, EVENTS)?,
        table_len(transaction, OUTBOX)?,
        table_len(transaction, OUTBOX_STATUS)?,
        table_len(transaction, PROJECTION_STATE)?,
        table_len(transaction, PROJECTION_FRONTIER)?,
        table_len(transaction, PROJECTION_APPLIED)?,
        table_len(transaction, CAPABILITIES)?,
        table_len(transaction, CAPABILITY_TOKENS)?,
        table_len(transaction, AUDIT)?,
        table_len(transaction, AUDIT_BY_REQUEST)?,
    ];
    let total = counts.iter().try_fold(1u64, |total, count| {
        total.checked_add(*count).ok_or_else(limit_exceeded)
    })?;
    Ok(StartupSnapshot {
        retained_metadata,
        structural_counts: counts,
        structural_total: total,
    })
}

fn read_retained_metadata(
    transaction: &ReadTransaction,
) -> Result<RetainedMetadataV1, StorageError> {
    let meta = transaction.open_table(META).map_err(table_error)?;
    let format = required_meta_value(
        &meta,
        META_FORMAT_VERSION,
        codec::decode_storage_format_version_v1,
    )?;
    let database_id =
        required_meta_value(&meta, META_DATABASE_ID, codec::decode_database_identity_v1)?;
    let application = required_meta_value(
        &meta,
        META_APPLICATION_SEQUENCE,
        codec::decode_application_sequence_allocator_v1,
    )?;
    let administration = required_meta_value(
        &meta,
        META_ADMINISTRATION_SEQUENCE,
        codec::decode_administration_sequence_allocator_v1,
    )?;
    let registry = required_meta_value(
        &meta,
        META_RECORD_REGISTRY,
        codec::decode_record_registry_v2,
    )?;
    if !startup_registry_is_supported(registry) {
        return Err(storage_error(StorageErrorKind::IncompatibleFormat));
    }
    let history_incarnation = required_meta_value(
        &meta,
        META_HISTORY_INCARNATION,
        codec::decode_history_incarnation_v1,
    )?;
    let bootstrap = meta
        .get(META_CAPABILITY_BOOTSTRAP)
        .map_err(precommit_storage_error)?
        .map(|value| {
            codec::decode_capability_bootstrap_marker_v1(value.value())
                .map(riffdb_storage_api::EncodedPageItem::into_parts)
                .map(|(value, _)| value)
        })
        .transpose()?;
    drop(meta);

    let active_table = transaction
        .open_table(CATALOG_ACTIVE)
        .map_err(table_error)?;
    if active_table.len().map_err(precommit_storage_error)? > 1 {
        return Err(corrupt());
    }
    let active = active_table
        .first()
        .map_err(precommit_storage_error)?
        .map(|(key, value)| {
            if key.value() != CATALOG_ACTIVE_KEY {
                return Err(corrupt());
            }
            codec::decode_active_catalog_pointer_v1(value.value())
                .map(riffdb_storage_api::EncodedPageItem::into_parts)
                .map(|(value, _)| value)
        })
        .transpose()?;

    RetainedMetadataV1::new(
        format,
        database_id,
        application,
        administration,
        history_incarnation,
        active,
        bootstrap,
    )
    .map_err(|_| corrupt())
}

fn required_meta_value<T, R>(
    table: &R,
    key: &'static str,
    decoder: impl FnOnce(&[u8]) -> Result<riffdb_storage_api::EncodedPageItem<T>, StorageError>,
) -> Result<T, StorageError>
where
    R: ReadableTable<&'static str, &'static [u8]>,
{
    let encoded = table
        .get(key)
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    decoder(encoded.value())
        .map(riffdb_storage_api::EncodedPageItem::into_parts)
        .map(|(value, _)| value)
}

fn validate_table_inventory(transaction: &ReadTransaction) -> Result<(), StorageError> {
    let tables = transaction
        .list_tables()
        .map_err(precommit_storage_error)?
        .map(|table| table.name().to_owned())
        .collect::<BTreeSet<_>>();
    let multimaps = transaction
        .list_multimap_tables()
        .map_err(precommit_storage_error)?
        .map(|table| table.name().to_owned())
        .collect::<BTreeSet<_>>();
    if !multimaps.is_empty() {
        return Err(storage_error(StorageErrorKind::IncompatibleFormat));
    }
    let expected = TABLE_NAMES
        .iter()
        .map(|name| (*name).to_owned())
        .collect::<BTreeSet<_>>();
    if tables == expected {
        Ok(())
    } else if tables.iter().any(|name| !expected.contains(name)) {
        Err(storage_error(StorageErrorKind::IncompatibleFormat))
    } else {
        Err(storage_error(StorageErrorKind::CorruptData))
    }
}

fn table_len(
    transaction: &ReadTransaction,
    definition: TableDefinition<&'static [u8], &'static [u8]>,
) -> Result<u64, StorageError> {
    transaction
        .open_table(definition)
        .map_err(table_error)?
        .len()
        .map_err(precommit_storage_error)
}

#[allow(dead_code)]
fn inspect_structural_item(
    transaction: &ReadTransaction,
    inputs: &StartupValidationInputs,
    database_id: DatabaseId,
    counts: &[u64; STRUCTURAL_TABLE_COUNT],
    position: u64,
) -> Result<Option<StructuralFinding>, StorageError> {
    if position == 0 {
        return inspect_header(transaction, database_id);
    }
    let mut relative = position - 1;
    for (phase, count) in counts.iter().copied().enumerate() {
        if relative < count {
            return inspect_table_row(transaction, inputs, database_id, phase, relative);
        }
        relative = relative.checked_sub(count).ok_or_else(invariant)?;
    }
    Err(invariant())
}

#[allow(dead_code)]
fn nth_meta_entry(
    transaction: &ReadTransaction,
    index: u64,
) -> Result<(String, Vec<u8>), StorageError> {
    let table = transaction.open_table(META).map_err(table_error)?;
    let mut iterator = table.iter().map_err(precommit_storage_error)?;
    let mut current = 0u64;
    for entry in &mut iterator {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        if current == index {
            return Ok((key.value().to_owned(), value.value().to_vec()));
        }
        current = current.checked_add(1).ok_or_else(limit_exceeded)?;
    }
    Err(invariant())
}

#[allow(dead_code)]
fn nth_bytes_entry(
    transaction: &ReadTransaction,
    definition: TableDefinition<&'static [u8], &'static [u8]>,
    index: u64,
) -> Result<(Vec<u8>, Vec<u8>), StorageError> {
    let table = transaction.open_table(definition).map_err(table_error)?;
    let mut iterator = table.iter().map_err(precommit_storage_error)?;
    let mut current = 0u64;
    for entry in &mut iterator {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        if current == index {
            return Ok((key.value().to_vec(), value.value().to_vec()));
        }
        current = current.checked_add(1).ok_or_else(limit_exceeded)?;
    }
    Err(invariant())
}

struct HistoricalCandidate {
    key: Vec<u8>,
    evidence: HistoricalSemanticEvidence,
}

#[allow(dead_code)]
fn inspect_table_row(
    transaction: &ReadTransaction,
    inputs: &StartupValidationInputs,
    database_id: DatabaseId,
    phase: usize,
    index: u64,
) -> Result<Option<StructuralFinding>, StorageError> {
    if phase == 0 {
        let (key, value) = nth_meta_entry(transaction, index)?;
        return Ok(inspect_meta_row(&key, &value, database_id));
    }
    let definition = match phase {
        1 => CONTRACT_BUNDLES,
        2 => CATALOG_ACTIVE,
        3 => QUERY_MODULES,
        4 => QUERY_MODULE_ACTIVE,
        5 => ENTITIES,
        6 => SECONDARY_INDEXES,
        7 => INDEX_EPOCHS,
        8 => IDEMPOTENCY,
        9 => IDEMPOTENCY_PENDING,
        10 => COMMITS,
        11 => PROVENANCE,
        12 => EVENTS,
        13 => OUTBOX,
        14 => OUTBOX_STATUS,
        15 => PROJECTION_STATE,
        16 => PROJECTION_FRONTIER,
        17 => PROJECTION_APPLIED,
        18 => CAPABILITIES,
        19 => CAPABILITY_TOKENS,
        20 => AUDIT,
        21 => AUDIT_BY_REQUEST,
        _ => return Err(invariant()),
    };
    let (key, value) = nth_bytes_entry(transaction, definition, index)?;
    match phase {
        1 => inspect_bundle_row(transaction, &key, &value),
        2 => inspect_active_row(transaction, &key, &value),
        3 => inspect_query_module_row(transaction, &key, &value),
        4 => inspect_active_query_module_row(transaction, &key, &value),
        5 => inspect_entity_row(transaction, &key, &value),
        6 => inspect_index_row(transaction, &key, &value),
        7 => inspect_epoch_row(transaction, &key, &value),
        8 => inspect_terminal_row(transaction, inputs, database_id, &key, &value),
        9 => inspect_pending_row(transaction, inputs, database_id, &key, &value),
        10 => inspect_commit_row(transaction, index, &key, &value),
        11 => inspect_provenance_row(transaction, &key, &value),
        12 => inspect_event_row(transaction, &key, &value),
        13 => inspect_outbox_row(transaction, &key, &value),
        14 => inspect_outbox_status_row(transaction, &key, &value),
        15 => inspect_projection_state_row(transaction, &key, &value),
        16 => inspect_projection_control_row(transaction, &key, &value),
        17 => inspect_projection_apply_row(transaction, &key, &value),
        18 => inspect_capability_row(transaction, inputs, database_id, &key, &value),
        19 => inspect_capability_lookup_row(transaction, &key, &value),
        20 => inspect_audit_row(transaction, index, &key, &value),
        21 => inspect_audit_by_request_row(transaction, &key, &value),
        _ => Err(invariant()),
    }
}

fn inspect_header(
    transaction: &ReadTransaction,
    database_id: DatabaseId,
) -> Result<Option<StructuralFinding>, StorageError> {
    let meta = transaction.open_table(META).map_err(table_error)?;
    let mut seen = BTreeSet::new();
    for entry in meta.iter().map_err(precommit_storage_error)? {
        let (key, _) = entry.map_err(precommit_storage_error)?;
        if !META_KEYS.contains(&key.value()) || !seen.insert(key.value().to_owned()) {
            return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
        }
    }
    if [
        META_FORMAT_VERSION,
        META_DATABASE_ID,
        META_APPLICATION_SEQUENCE,
        META_ADMINISTRATION_SEQUENCE,
        META_RECORD_REGISTRY,
    ]
    .iter()
    .any(|key| !seen.contains(*key))
    {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    }
    let application = match meta_decode(
        &meta,
        META_APPLICATION_SEQUENCE,
        codec::decode_application_sequence_allocator_v1,
    )? {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    let administration = match meta_decode(
        &meta,
        META_ADMINISTRATION_SEQUENCE,
        codec::decode_administration_sequence_allocator_v1,
    )? {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    if !application_allocator_matches(transaction, application)?
        || !administration_allocator_matches(transaction, administration)?
    {
        return Ok(Some(authoritative(
            StructuralFindingCode::SequenceDiscontinuity,
        )));
    }
    let active = match read_active_pointer(transaction)? {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    if let Some(active) = &active
        && !bundle_pointer_exists(transaction, active)?
    {
        return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
    }
    if !active_catalog_matches_last_activation(transaction, active.as_ref())? {
        return Ok(Some(authoritative(
            StructuralFindingCode::CrossLinkMismatch,
        )));
    }
    if active.is_none()
        && (table_len(transaction, CONTRACT_BUNDLES)? != 0
            || has_application_authoritative_state(transaction)?)
    {
        return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
    }
    if table_len(transaction, QUERY_MODULES)?
        > u64::try_from(MAX_RETAINED_QUERY_MODULES).map_err(|_| limit_exceeded())?
    {
        return Ok(Some(authoritative(StructuralFindingCode::LimitExceeded)));
    }
    if let Some(value) = meta
        .get(META_CAPABILITY_BOOTSTRAP)
        .map_err(precommit_storage_error)?
    {
        let marker = match decoded(codec::decode_capability_bootstrap_marker_v1(value.value())) {
            Ok(value) => value,
            Err(code) => return Ok(Some(authoritative(code))),
        };
        if !bootstrap_is_consistent(transaction, database_id, marker)? {
            return Ok(Some(authoritative(
                StructuralFindingCode::CrossLinkMismatch,
            )));
        }
    } else if table_len(transaction, CAPABILITIES)? != 0 {
        return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
    }
    Ok(None)
}

fn inspect_meta_row(key: &str, value: &[u8], database_id: DatabaseId) -> Option<StructuralFinding> {
    let valid = match key {
        META_FORMAT_VERSION => decoded(codec::decode_storage_format_version_v1(value))
            .is_ok_and(|version| version == riffdb_storage_api::StorageFormatVersion::V2),
        META_RECORD_REGISTRY => decoded(codec::decode_record_registry_v2(value))
            .is_ok_and(startup_registry_is_supported),
        META_DATABASE_ID => decoded(codec::decode_database_identity_v1(value))
            .is_ok_and(|identity| identity == database_id),
        META_APPLICATION_SEQUENCE => {
            decoded(codec::decode_application_sequence_allocator_v1(value)).is_ok()
        }
        META_ADMINISTRATION_SEQUENCE => {
            decoded(codec::decode_administration_sequence_allocator_v1(value)).is_ok()
        }
        META_CAPABILITY_BOOTSTRAP => {
            decoded(codec::decode_capability_bootstrap_marker_v1(value)).is_ok()
        }
        META_HISTORY_INCARNATION => decoded(codec::decode_history_incarnation_v1(value))
            .is_ok_and(|incarnation| incarnation >= 1),
        META_INDEX_EPOCH_ROWS_REPAIRED => value == [1u8].as_slice(),
        _ => false,
    };
    (!valid).then(|| authoritative(StructuralFindingCode::MalformedRecord))
}

fn inspect_bundle_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok((lineage, version)) = keys::decode_contract_bundle_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let bundle = match decoded(codec::decode_contract_bundle_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    if bundle.lineage() != &lineage
        || bundle.contract_version() != version
        || hash_contract_bundle(bundle.canonical_bytes()) != bundle.bundle_hash()
    {
        return Ok(Some(authoritative(
            StructuralFindingCode::CrossLinkMismatch,
        )));
    }
    Ok((!bundle_has_activation(transaction, &bundle)?)
        .then(|| authoritative(StructuralFindingCode::MissingCrossLink)))
}

fn inspect_active_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    if keys::decode_singleton_key(key).is_err() {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    }
    let active = match decoded(codec::decode_active_catalog_pointer_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    if !bundle_pointer_exists(transaction, &active)? {
        return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
    }
    Ok(
        (!active_catalog_matches_last_activation(transaction, Some(&active))?)
            .then(|| authoritative(StructuralFindingCode::CrossLinkMismatch)),
    )
}

fn inspect_query_module_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(module_hash) = keys::decode_query_module_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let module = match decoded(codec::decode_query_module_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    if module.module_hash() != module_hash
        || hash_query_module(module.canonical_bytes()) != module_hash
    {
        return Ok(Some(authoritative(
            StructuralFindingCode::CrossLinkMismatch,
        )));
    }
    if !query_module_contract_exists(transaction, &module)? {
        return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
    }
    Ok((!query_module_has_activation(transaction, &module)?)
        .then(|| authoritative(StructuralFindingCode::MissingCrossLink)))
}

fn inspect_active_query_module_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok((lineage, version, bundle_hash)) = keys::decode_active_query_module_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let record = match decoded(codec::decode_query_module_administration_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    let activated = record.activated();
    if activated.contract_lineage() != &lineage
        || activated.contract_version() != version
        || activated.contract_bundle_hash() != bundle_hash
    {
        return Ok(Some(authoritative(
            StructuralFindingCode::CrossLinkMismatch,
        )));
    }
    if !query_module_pointer_exists(transaction, activated)? {
        return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
    }
    Ok((!query_module_record_is_reciprocal(transaction, &record)?)
        .then(|| authoritative(StructuralFindingCode::CrossLinkMismatch)))
}

fn inspect_entity_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(key) = keys::decode_entity_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let record = match decoded(codec::decode_entity_record_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    if record.target().key() != &key {
        return Ok(Some(authoritative(
            StructuralFindingCode::CrossLinkMismatch,
        )));
    }
    Ok(
        (!binding_bundle_exists(transaction, record.schema_binding())?
            || !entity_history_matches(transaction, &record)?)
        .then(|| authoritative(StructuralFindingCode::MissingCrossLink)),
    )
}

fn inspect_index_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(key) = keys::decode_index_entry_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let record = match codec::decode_index_migration_row(&key, value) {
        Ok(value) => value,
        Err(error) => {
            let code = match error.kind() {
                StorageErrorKind::LimitExceeded => StructuralFindingCode::LimitExceeded,
                _ => StructuralFindingCode::MalformedRecord,
            };
            return Ok(Some(authoritative(code)));
        }
    };
    Ok(
        (!binding_bundle_exists(transaction, record.row().schema_binding())?)
            .then(|| authoritative(StructuralFindingCode::MissingCrossLink)),
    )
}

fn inspect_epoch_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    if let Ok(record) = decoded(codec::decode_legacy_index_epoch_v1(value)) {
        let Ok(key) = keys::decode_index_range_prefix_key(key) else {
            return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
        };
        return Ok((record.target() != &key
            || !binding_bundle_exists(transaction, record.schema_binding())?)
        .then(|| authoritative(StructuralFindingCode::MissingCrossLink)));
    }
    let Ok(key) = keys::decode_partition_index_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let record = match decoded(codec::decode_index_epoch_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    Ok(
        (record.target() != &key || !binding_bundle_exists(transaction, record.schema_binding())?)
            .then(|| authoritative(StructuralFindingCode::MissingCrossLink)),
    )
}

fn inspect_terminal_row(
    transaction: &ReadTransaction,
    inputs: &StartupValidationInputs,
    database_id: DatabaseId,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(physical) = keys::decode_idempotency_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let record = match decoded(codec::decode_idempotency_record_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    let (identity, plan) = match &record {
        IdempotencyRecordV1::StoredOutcome(value) => (value.identity(), value.plan()),
        IdempotencyRecordV1::ExecutionFailed(value) => {
            (value.pending().identity(), value.pending().plan())
        }
    };
    if identity.storage_key().ok().as_ref() != Some(&physical)
        || identity.database_id() != database_id
    {
        return Ok(Some(authoritative(
            StructuralFindingCode::CrossLinkMismatch,
        )));
    }
    if !idempotency_digest_is_readable(inputs, identity.caller_key_digest()) {
        return Ok(Some(authoritative(
            StructuralFindingCode::DigestUnavailable,
        )));
    }
    if !plan_bundle_exists(transaction, plan)? || raw_exists(transaction, IDEMPOTENCY_PENDING, key)?
    {
        return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
    }
    if let IdempotencyRecordV1::StoredOutcome(outcome) = &record
        && !outcome_graph_is_reciprocal(transaction, outcome)?
    {
        return Ok(Some(authoritative(
            StructuralFindingCode::CrossLinkMismatch,
        )));
    }
    Ok(None)
}

fn inspect_pending_row(
    transaction: &ReadTransaction,
    inputs: &StartupValidationInputs,
    database_id: DatabaseId,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(physical) = keys::decode_idempotency_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let pending = match decoded(codec::decode_pending_admission_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    if pending.identity().storage_key().ok().as_ref() != Some(&physical)
        || pending.identity().database_id() != database_id
    {
        return Ok(Some(authoritative(
            StructuralFindingCode::CrossLinkMismatch,
        )));
    }
    if !idempotency_digest_is_readable(inputs, pending.identity().caller_key_digest()) {
        return Ok(Some(authoritative(
            StructuralFindingCode::DigestUnavailable,
        )));
    }
    Ok((!plan_bundle_exists(transaction, pending.plan())?
        || raw_exists(transaction, IDEMPOTENCY, key)?)
    .then(|| authoritative(StructuralFindingCode::MissingCrossLink)))
}

fn inspect_commit_row(
    transaction: &ReadTransaction,
    index: u64,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(sequence) = keys::decode_application_sequence_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let events = transaction.open_table(EVENTS).map_err(table_error)?;
    let record = match decoded(codec::decode_commit_with_event_table(value, &events)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    if index.checked_add(1).and_then(CommitSequence::new) != Some(sequence)
        || record.commit_sequence() != sequence
    {
        return Ok(Some(authoritative(
            StructuralFindingCode::SequenceDiscontinuity,
        )));
    }
    Ok((!plan_bundle_exists(transaction, record.plan())?
        || !commit_graph_is_reciprocal(transaction, &record)?)
    .then(|| authoritative(StructuralFindingCode::MissingCrossLink)))
}

fn inspect_provenance_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(id) = keys::decode_provenance_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let record = match decoded(codec::decode_provenance_record_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    Ok((record.provenance_id() != id
        || !plan_bundle_exists(transaction, record.plan())?
        || !provenance_graph_is_reciprocal(transaction, &record)?)
    .then(|| authoritative(StructuralFindingCode::CrossLinkMismatch)))
}

fn inspect_event_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(id) = keys::decode_event_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let event = match decoded(codec::decode_durable_event_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    Ok(
        (event.event_id() != id || !event_graph_is_reciprocal(transaction, &event)?)
            .then(|| authoritative(StructuralFindingCode::CrossLinkMismatch)),
    )
}

fn inspect_outbox_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(id) = keys::decode_event_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let events = transaction.open_table(EVENTS).map_err(table_error)?;
    let intent = match decoded(codec::decode_outbox_with_event_table(value, &events)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    Ok(
        (intent.event_id() != id || !event_graph_is_reciprocal(transaction, intent.event())?)
            .then(|| authoritative(StructuralFindingCode::CrossLinkMismatch)),
    )
}

fn inspect_outbox_status_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(id) = keys::decode_event_key(key) else {
        return Ok(Some(derived_outbox(StructuralFindingCode::MalformedRecord)));
    };
    let status = match decoded(codec::decode_outbox_status_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(derived_outbox(code))),
    };
    Ok(
        (status.event_id() != id || !raw_exists(transaction, OUTBOX, key)?)
            .then(|| derived_outbox(StructuralFindingCode::OrphanedOutboxStatus)),
    )
}

fn inspect_projection_state_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(key) = keys::decode_projection_group_key(key) else {
        return Ok(Some(derived_projection(
            StructuralFindingCode::MalformedRecord,
        )));
    };
    let state = match decoded(codec::decode_projection_state_structural_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(derived_projection(code))),
    };
    if state.key() != &key {
        return Ok(Some(derived_projection(
            StructuralFindingCode::ProjectionStateMismatch,
        )));
    }
    let control = match projection_control(transaction, state.identity())? {
        Ok(Some(value)) => value,
        Ok(None) => {
            return Ok(Some(derived_projection(
                StructuralFindingCode::MissingCrossLink,
            )));
        }
        Err(code) => return Ok(Some(derived_projection(code))),
    };
    if state.generation() > control.highest_allocated_generation() {
        return Ok(Some(derived_projection(
            StructuralFindingCode::ProjectionStateMismatch,
        )));
    }
    let Some(frontier) = control.frontier_for(state.generation()) else {
        // Rows from retired generations are canonical but inert.
        return Ok(None);
    };
    let FrontierPosition::AppliedThrough(frontier) = frontier else {
        return Ok(Some(derived_projection(
            StructuralFindingCode::ProjectionStateMismatch,
        )));
    };
    if state.last_changed_sequence() > frontier {
        return Ok(Some(derived_projection(
            StructuralFindingCode::ProjectionStateMismatch,
        )));
    }
    if let Some(finding) =
        missing_projection_source_finding(transaction, state.last_changed_sequence())?
    {
        return Ok(Some(finding));
    }
    let marker_key = riffdb_types::ProjectionApplyKey::new(
        state.identity().clone(),
        state.generation(),
        state.last_changed_sequence(),
    );
    Ok((!projection_marker_exists(transaction, &marker_key)?)
        .then(|| derived_projection(StructuralFindingCode::MissingCrossLink)))
}

fn inspect_projection_control_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(key) = keys::decode_projection_frontier_key(key) else {
        return Ok(Some(derived_projection(
            StructuralFindingCode::MalformedRecord,
        )));
    };
    let control = match decoded(codec::decode_projection_control_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(derived_projection(code))),
    };
    if control.identity() != key.identity() {
        return Ok(Some(derived_projection(
            StructuralFindingCode::ProjectionStateMismatch,
        )));
    }
    for position in [control.published(), control.candidate()]
        .into_iter()
        .flatten()
    {
        match position.frontier() {
            FrontierPosition::BeforeFirst => {
                if projection_state_namespace_has_row(
                    transaction,
                    control.identity(),
                    position.generation(),
                )? || projection_marker_namespace_has_row(
                    transaction,
                    control.identity(),
                    position.generation(),
                )? {
                    return Ok(Some(derived_projection(
                        StructuralFindingCode::ProjectionStateMismatch,
                    )));
                }
            }
            FrontierPosition::AppliedThrough(sequence) => {
                if let Some(finding) = missing_projection_source_finding(transaction, sequence)? {
                    return Ok(Some(finding));
                }
                let first = riffdb_types::ProjectionApplyKey::new(
                    control.identity().clone(),
                    position.generation(),
                    CommitSequence::first(),
                );
                let last = riffdb_types::ProjectionApplyKey::new(
                    control.identity().clone(),
                    position.generation(),
                    sequence,
                );
                if !projection_marker_exists(transaction, &first)?
                    || !projection_marker_exists(transaction, &last)?
                    || projection_marker_exists_after(
                        transaction,
                        control.identity(),
                        position.generation(),
                        sequence,
                    )?
                {
                    return Ok(Some(derived_projection(
                        StructuralFindingCode::ProjectionStateMismatch,
                    )));
                }
            }
        }
    }
    Ok(None)
}

fn inspect_projection_apply_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(key) = keys::decode_projection_apply_key(key) else {
        return Ok(Some(derived_projection(
            StructuralFindingCode::MalformedRecord,
        )));
    };
    let marker = match decoded(codec::decode_projection_apply_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(derived_projection(code))),
    };
    if marker.key() != &key {
        return Ok(Some(derived_projection(
            StructuralFindingCode::ProjectionStateMismatch,
        )));
    }
    let control = match projection_control(transaction, key.identity())? {
        Ok(Some(value)) => value,
        Ok(None) => {
            return Ok(Some(derived_projection(
                StructuralFindingCode::MissingCrossLink,
            )));
        }
        Err(code) => return Ok(Some(derived_projection(code))),
    };
    if key.generation() > control.highest_allocated_generation() {
        return Ok(Some(derived_projection(
            StructuralFindingCode::ProjectionStateMismatch,
        )));
    }
    if let Some(frontier) = control.frontier_for(key.generation()) {
        match frontier {
            FrontierPosition::BeforeFirst => {
                return Ok(Some(derived_projection(
                    StructuralFindingCode::ProjectionStateMismatch,
                )));
            }
            FrontierPosition::AppliedThrough(frontier) if key.commit_sequence() > frontier => {
                return Ok(Some(derived_projection(
                    StructuralFindingCode::ProjectionStateMismatch,
                )));
            }
            FrontierPosition::AppliedThrough(_) => {}
        }
        if key.commit_sequence() != CommitSequence::first() {
            let previous = CommitSequence::new(key.commit_sequence().get() - 1)
                .expect("a non-first commit sequence has a nonzero predecessor");
            let predecessor = riffdb_types::ProjectionApplyKey::new(
                key.identity().clone(),
                key.generation(),
                previous,
            );
            if !projection_marker_exists(transaction, &predecessor)? {
                return Ok(Some(derived_projection(
                    StructuralFindingCode::ProjectionStateMismatch,
                )));
            }
        }
    }
    missing_projection_source_finding(transaction, key.commit_sequence())
}

fn inspect_capability_row(
    transaction: &ReadTransaction,
    inputs: &StartupValidationInputs,
    database_id: DatabaseId,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(id) = keys::decode_capability_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let capability = match decoded(codec::decode_capability_record_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    if capability.capability_id() != id || capability.database_id() != database_id {
        return Ok(Some(authoritative(
            StructuralFindingCode::CrossLinkMismatch,
        )));
    }
    let lookup_key = keys::encode_capability_token_key(capability.token_digest());
    let lookup = get_decoded(
        transaction,
        CAPABILITY_TOKENS,
        &lookup_key,
        codec::decode_capability_token_lookup_v1,
    )?;
    if !matches!(lookup, Ok(Some(value)) if value.capability_id() == id) {
        return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
    }
    if matches!(capability.lifecycle(), CapabilityLifecycleV1::Active)
        && inputs.authorization_time() < capability.expires_at()
        && !inputs
            .capability_digests()
            .as_slice()
            .contains(&ReadableDigestKey::v1(capability.token_digest().key_id()))
    {
        return Ok(Some(authoritative(
            StructuralFindingCode::DigestUnavailable,
        )));
    }
    Ok(
        match capability_record_audit_status(transaction, &capability)? {
            CrossLinkStatus::Exact => None,
            CrossLinkStatus::Missing => {
                Some(authoritative(StructuralFindingCode::MissingCrossLink))
            }
            CrossLinkStatus::Mismatch => {
                Some(authoritative(StructuralFindingCode::CrossLinkMismatch))
            }
        },
    )
}

fn inspect_capability_lookup_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(digest) = keys::decode_capability_token_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let lookup = match decoded(codec::decode_capability_token_lookup_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    let capability_key = keys::encode_capability_key(lookup.capability_id());
    let capability = get_decoded(
        transaction,
        CAPABILITIES,
        &capability_key,
        codec::decode_capability_record_v1,
    )?;
    Ok(
        (!matches!(capability, Ok(Some(value)) if value.token_digest() == digest))
            .then(|| authoritative(StructuralFindingCode::MissingCrossLink)),
    )
}

fn inspect_audit_row(
    transaction: &ReadTransaction,
    index: u64,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(sequence) = keys::decode_audit_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let record = match decoded(codec::decode_administration_audit_record_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    if index
        .checked_add(1)
        .and_then(riffdb_types::AdministrationSequence::new)
        != Some(sequence)
        || record.administration_sequence() != sequence
    {
        return Ok(Some(authoritative(
            StructuralFindingCode::SequenceDiscontinuity,
        )));
    }
    let finding = match &record {
        riffdb_storage_api::StoredAdministrationAuditRecordV1::Catalog(record) => {
            if !bundle_pointer_exists(transaction, record.activated())? {
                Some(authoritative(StructuralFindingCode::MissingCrossLink))
            } else if !catalog_record_is_reciprocal(transaction, record)? {
                Some(authoritative(StructuralFindingCode::CrossLinkMismatch))
            } else {
                None
            }
        }
        riffdb_storage_api::StoredAdministrationAuditRecordV1::QueryModule(record) => {
            if !query_module_pointer_exists(transaction, record.activated())? {
                Some(authoritative(StructuralFindingCode::MissingCrossLink))
            } else if !query_module_record_is_reciprocal(transaction, record)? {
                Some(authoritative(StructuralFindingCode::CrossLinkMismatch))
            } else {
                None
            }
        }
        riffdb_storage_api::StoredAdministrationAuditRecordV1::Capability(record) => {
            match capability_administration_status(transaction, record)? {
                CrossLinkStatus::Exact => None,
                CrossLinkStatus::Missing => {
                    Some(authoritative(StructuralFindingCode::MissingCrossLink))
                }
                CrossLinkStatus::Mismatch => {
                    Some(authoritative(StructuralFindingCode::CrossLinkMismatch))
                }
            }
        }
        riffdb_storage_api::StoredAdministrationAuditRecordV1::Service(record) => {
            (!service_lifecycle_is_reciprocal(transaction, record)?)
                .then(|| authoritative(StructuralFindingCode::CrossLinkMismatch))
        }
    };
    Ok(finding)
}

fn inspect_audit_by_request_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok((request_id, sequence)) = keys::decode_audit_by_request_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let index = match decoded(codec::decode_service_audit_request_index_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    if index.request_id() != request_id || index.administration_sequence() != sequence {
        return Ok(Some(authoritative(
            StructuralFindingCode::CrossLinkMismatch,
        )));
    }
    let audit_key = keys::encode_audit_key(sequence);
    let audit = get_decoded(
        transaction,
        AUDIT,
        audit_key.as_slice(),
        codec::decode_administration_audit_record_v1,
    )?;
    Ok((!matches!(
        audit,
        Ok(Some(riffdb_storage_api::StoredAdministrationAuditRecordV1::Service(record)))
            if record.request_id() == request_id
                && record.administration_sequence() == sequence
    ))
    .then(|| authoritative(StructuralFindingCode::MissingCrossLink)))
}

const MAX_STARTUP_EVIDENCE_INDEX_BYTES: usize = 512 * 1024 * 1024;

fn build_historical_evidence_plan(
    transaction: &ReadTransaction,
    inputs: &StartupValidationInputs,
) -> Result<HistoricalEvidencePlan, StorageError> {
    // One linear pass: gather every candidate with existing per-row validation, then sort.
    let mut candidates = Vec::new();
    gather_historical_candidates(transaction, inputs, &mut candidates)?;
    let mut index_bytes = 0usize;
    let mut ordered: std::collections::BTreeMap<Vec<u8>, EvidenceLocator> =
        std::collections::BTreeMap::new();
    for candidate in candidates {
        if ordered.contains_key(&candidate.key) {
            continue;
        }
        let charge = candidate
            .key
            .len()
            .checked_add(64)
            .ok_or_else(limit_exceeded)?;
        index_bytes = index_bytes.checked_add(charge).ok_or_else(limit_exceeded)?;
        if index_bytes > MAX_STARTUP_EVIDENCE_INDEX_BYTES {
            return Err(limit_exceeded());
        }
        ordered.insert(
            candidate.key,
            EvidenceLocator::Materialized(Some(candidate.evidence)),
        );
    }
    Ok(HistoricalEvidencePlan {
        entries: ordered.into_iter().collect(),
        next_index: 0,
    })
}

fn gather_historical_candidates(
    transaction: &ReadTransaction,
    inputs: &StartupValidationInputs,
    out: &mut Vec<HistoricalCandidate>,
) -> Result<(), StorageError> {
    gather_from_scan(transaction, out, |txn, after, selected| {
        scan_bundle_candidates(txn, after, selected)
    })?;
    gather_from_scan(transaction, out, |txn, after, selected| {
        scan_plan_candidates(txn, after, selected)
    })?;
    gather_from_scan(transaction, out, |txn, after, selected| {
        scan_active_candidate(txn, after, selected)
    })?;
    gather_from_scan(transaction, out, |txn, after, selected| {
        scan_persisted_key_candidates(txn, after, selected)
    })?;
    gather_from_scan(transaction, out, |txn, after, selected| {
        scan_capability_partition_candidates(txn, inputs, after, selected)
    })?;
    Ok(())
}

fn gather_from_scan(
    transaction: &ReadTransaction,
    out: &mut Vec<HistoricalCandidate>,
    mut scan: impl FnMut(
        &ReadTransaction,
        Option<&[u8]>,
        &mut Option<HistoricalCandidate>,
    ) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    // Each scan_* function walks the entire table(s) and keeps only the minimum
    // key > after. Calling it once with after=None only yields one item. To gather
    // all rows linearly, run the scan body with a collector — for parity with the
    // existing validation, walk by repeatedly advancing after. That is O(n^2).
    //
    // Linear alternative used here: invoke the scan with after=None into a custom
    // "selected" that appends every considered candidate. We do that by temporarily
    // replacing consider_candidate behavior via a thread-local is overkill.
    //
    // Practical linear gather: call the original select_next loop once (full stream).
    let mut after: Option<Vec<u8>> = None;
    loop {
        let mut selected = None;
        scan(transaction, after.as_deref(), &mut selected)?;
        let Some(candidate) = selected else {
            break;
        };
        after = Some(candidate.key.clone());
        out.push(candidate);
    }
    Ok(())
}

fn peek_historical_evidence(
    locator: &EvidenceLocator,
) -> Result<&HistoricalSemanticEvidence, StorageError> {
    match locator {
        EvidenceLocator::Materialized(Some(evidence)) => Ok(evidence),
        EvidenceLocator::Materialized(None) => Err(invariant()),
    }
}

fn take_historical_evidence(
    locator: &mut EvidenceLocator,
) -> Result<HistoricalSemanticEvidence, StorageError> {
    match locator {
        EvidenceLocator::Materialized(slot) => slot.take().ok_or_else(invariant),
    }
}

#[allow(dead_code)]
fn select_next_historical(
    transaction: &ReadTransaction,
    inputs: &StartupValidationInputs,
    after: Option<&[u8]>,
) -> Result<Option<HistoricalCandidate>, StorageError> {
    let mut selected = None;
    scan_bundle_candidates(transaction, after, &mut selected)?;
    scan_plan_candidates(transaction, after, &mut selected)?;
    scan_active_candidate(transaction, after, &mut selected)?;
    scan_persisted_key_candidates(transaction, after, &mut selected)?;
    scan_capability_partition_candidates(transaction, inputs, after, &mut selected)?;
    Ok(selected)
}

fn consider_candidate(
    selected: &mut Option<HistoricalCandidate>,
    after: Option<&[u8]>,
    candidate: HistoricalCandidate,
) {
    if after.is_some_and(|after| candidate.key.as_slice() <= after) {
        return;
    }
    if selected
        .as_ref()
        .is_none_or(|current| candidate.key < current.key)
    {
        *selected = Some(candidate);
    }
}

fn scan_bundle_candidates(
    transaction: &ReadTransaction,
    after: Option<&[u8]>,
    selected: &mut Option<HistoricalCandidate>,
) -> Result<(), StorageError> {
    let table = transaction
        .open_table(CONTRACT_BUNDLES)
        .map_err(table_error)?;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let (lineage, version) =
            keys::decode_contract_bundle_key(key.value()).map_err(|_| corrupt())?;
        let bundle =
            decoded(codec::decode_contract_bundle_v1(value.value())).map_err(|_| corrupt())?;
        if bundle.lineage() != &lineage
            || bundle.contract_version() != version
            || hash_contract_bundle(bundle.canonical_bytes()) != bundle.bundle_hash()
        {
            return Err(corrupt());
        }
        let evidence = HistoricalSemanticEvidence::Bundle(HistoricalBundleEvidence::new(
            lineage,
            version,
            bundle.bundle_hash(),
            HistoricalBundleBytes::new(bundle.canonical_bytes().to_vec())
                .map_err(value_error_as_storage)?,
        ));
        consider_evidence(selected, after, evidence);
    }
    Ok(())
}

fn scan_plan_candidates(
    transaction: &ReadTransaction,
    after: Option<&[u8]>,
    selected: &mut Option<HistoricalCandidate>,
) -> Result<(), StorageError> {
    let terminal = transaction.open_table(IDEMPOTENCY).map_err(table_error)?;
    for entry in terminal.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let physical = keys::decode_idempotency_key(key.value()).map_err(|_| corrupt())?;
        let record =
            decoded(codec::decode_idempotency_record_v1(value.value())).map_err(|_| corrupt())?;
        let (identity, plan) = match &record {
            IdempotencyRecordV1::StoredOutcome(value) => (value.identity(), value.plan()),
            IdempotencyRecordV1::ExecutionFailed(value) => {
                (value.pending().identity(), value.pending().plan())
            }
        };
        if identity.storage_key().ok().as_ref() != Some(&physical) {
            return Err(corrupt());
        }
        consider_plan(selected, after, plan.clone());
    }
    let pending = transaction
        .open_table(IDEMPOTENCY_PENDING)
        .map_err(table_error)?;
    for entry in pending.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let physical = keys::decode_idempotency_key(key.value()).map_err(|_| corrupt())?;
        let record =
            decoded(codec::decode_pending_admission_v1(value.value())).map_err(|_| corrupt())?;
        if record.identity().storage_key().ok().as_ref() != Some(&physical) {
            return Err(corrupt());
        }
        consider_plan(selected, after, record.plan().clone());
    }
    let commits = transaction.open_table(COMMITS).map_err(table_error)?;
    let events = transaction.open_table(EVENTS).map_err(table_error)?;
    for entry in commits.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let sequence = keys::decode_application_sequence_key(key.value()).map_err(|_| corrupt())?;
        let record = decoded(codec::decode_commit_with_event_table(
            value.value(),
            &events,
        ))
        .map_err(|_| corrupt())?;
        if record.commit_sequence() != sequence {
            return Err(corrupt());
        }
        consider_plan(selected, after, record.plan().clone());
    }
    let provenance = transaction.open_table(PROVENANCE).map_err(table_error)?;
    for entry in provenance.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let id = keys::decode_provenance_key(key.value()).map_err(|_| corrupt())?;
        let record =
            decoded(codec::decode_provenance_record_v1(value.value())).map_err(|_| corrupt())?;
        if record.provenance_id() != id {
            return Err(corrupt());
        }
        consider_plan(selected, after, record.plan().clone());
    }
    Ok(())
}

fn scan_active_candidate(
    transaction: &ReadTransaction,
    after: Option<&[u8]>,
    selected: &mut Option<HistoricalCandidate>,
) -> Result<(), StorageError> {
    let active = read_active_pointer(transaction)?.map_err(|_| corrupt())?;
    let evidence = HistoricalSemanticEvidence::ActiveCatalog(active.map(|active| {
        HistoricalActiveCatalogEvidence::new(
            active.lineage().clone(),
            active.contract_version(),
            active.bundle_hash(),
        )
    }));
    consider_evidence(selected, after, evidence);
    Ok(())
}

fn scan_persisted_key_candidates(
    transaction: &ReadTransaction,
    after: Option<&[u8]>,
    selected: &mut Option<HistoricalCandidate>,
) -> Result<(), StorageError> {
    let entities = transaction.open_table(ENTITIES).map_err(table_error)?;
    for entry in entities.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let physical = keys::decode_entity_key(key.value()).map_err(|_| corrupt())?;
        let record =
            decoded(codec::decode_entity_record_v1(value.value())).map_err(|_| corrupt())?;
        if record.target().key() != &physical {
            return Err(corrupt());
        }
        consider_evidence(
            selected,
            after,
            HistoricalSemanticEvidence::PersistedKey(
                HistoricalPersistedKeyEvidenceV1::from_entity(&record),
            ),
        );
    }
    let indexes = transaction
        .open_table(SECONDARY_INDEXES)
        .map_err(table_error)?;
    for entry in indexes.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let physical = keys::decode_index_entry_key(key.value()).map_err(|_| corrupt())?;
        let record = codec::decode_index_migration_row(&physical, value.value())?;
        consider_evidence(
            selected,
            after,
            HistoricalSemanticEvidence::IndexMigrationRow(record),
        );
    }
    let epochs = transaction.open_table(INDEX_EPOCHS).map_err(table_error)?;
    for entry in epochs.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        if let Ok(record) = decoded(codec::decode_legacy_index_epoch_v1(value.value())) {
            let physical =
                keys::decode_index_range_prefix_key(key.value()).map_err(|_| corrupt())?;
            if record.target() != &physical {
                return Err(corrupt());
            }
            consider_evidence(
                selected,
                after,
                HistoricalSemanticEvidence::PersistedKey(
                    HistoricalPersistedKeyEvidenceV1::from_legacy_index_epoch(&record),
                ),
            );
        } else {
            let physical = keys::decode_partition_index_key(key.value()).map_err(|_| corrupt())?;
            let record =
                decoded(codec::decode_index_epoch_v1(value.value())).map_err(|_| corrupt())?;
            if record.target() != &physical {
                return Err(corrupt());
            }
            consider_evidence(
                selected,
                after,
                HistoricalSemanticEvidence::PersistedKey(
                    HistoricalPersistedKeyEvidenceV1::from_index_epoch(&record),
                ),
            );
        }
    }
    Ok(())
}

fn scan_capability_partition_candidates(
    transaction: &ReadTransaction,
    inputs: &StartupValidationInputs,
    after: Option<&[u8]>,
    selected: &mut Option<HistoricalCandidate>,
) -> Result<(), StorageError> {
    let capabilities = transaction.open_table(CAPABILITIES).map_err(table_error)?;
    for entry in capabilities.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let physical = keys::decode_capability_key(key.value()).map_err(|_| corrupt())?;
        let capability =
            decoded(codec::decode_capability_record_v1(value.value())).map_err(|_| corrupt())?;
        if capability.capability_id() != physical {
            return Err(corrupt());
        }
        if !matches!(capability.lifecycle(), CapabilityLifecycleV1::Active)
            || inputs.authorization_time() >= capability.expires_at()
        {
            continue;
        }
        let Some(entries) = capability.grant().partition_scope().explicit_entries() else {
            continue;
        };
        for ordinal in 0..entries.len() {
            let evidence = HistoricalCapabilityPartitionEvidenceV1::from_capability_entry(
                &capability,
                ordinal,
            )
            .map_err(value_error_as_storage)?;
            consider_evidence(
                selected,
                after,
                HistoricalSemanticEvidence::CapabilityPartition(evidence),
            );
        }
    }
    Ok(())
}

fn consider_plan(
    selected: &mut Option<HistoricalCandidate>,
    after: Option<&[u8]>,
    plan: riffdb_storage_api::ExecutablePlanRef,
) {
    consider_evidence(
        selected,
        after,
        HistoricalSemanticEvidence::PlanReference(plan),
    );
}

fn consider_evidence(
    selected: &mut Option<HistoricalCandidate>,
    after: Option<&[u8]>,
    evidence: HistoricalSemanticEvidence,
) {
    consider_candidate(
        selected,
        after,
        HistoricalCandidate {
            key: historical_order_key(&evidence),
            evidence,
        },
    );
}

fn historical_order_key(evidence: &HistoricalSemanticEvidence) -> Vec<u8> {
    let mut key = Vec::new();
    match evidence {
        HistoricalSemanticEvidence::Bundle(bundle) => {
            key.push(0x01);
            push_lineage(&mut key, bundle.lineage());
            key.extend_from_slice(&bundle.version().to_be_bytes());
            key.extend_from_slice(bundle.bundle_hash().as_bytes());
        }
        HistoricalSemanticEvidence::PlanReference(plan) => {
            key.push(0x02);
            push_lineage(&mut key, plan.contract_lineage());
            key.extend_from_slice(&plan.contract_version().to_be_bytes());
            key.extend_from_slice(plan.contract_bundle_hash().as_bytes());
            key.extend_from_slice(&plan.command_id().to_be_bytes());
            key.extend_from_slice(plan.command_plan_hash().as_bytes());
        }
        HistoricalSemanticEvidence::ActiveCatalog(None) => key.extend_from_slice(&[0x03, 0x00]),
        HistoricalSemanticEvidence::ActiveCatalog(Some(active)) => {
            key.extend_from_slice(&[0x03, 0x01]);
            push_lineage(&mut key, active.lineage());
            key.extend_from_slice(&active.version().to_be_bytes());
            key.extend_from_slice(active.bundle_hash().as_bytes());
        }
        HistoricalSemanticEvidence::PersistedKey(persisted) => {
            key.push(0x04);
            push_lineage(&mut key, persisted.schema().lineage());
            key.extend_from_slice(&persisted.schema().contract_version().to_be_bytes());
            key.extend_from_slice(persisted.schema().bundle_hash().as_bytes());
            match persisted.key() {
                riffdb_storage_api::IrOpaquePersistedKeyV1::Entity {
                    entity_type_id,
                    key: entity_key,
                } => {
                    key.push(0x01);
                    key.extend_from_slice(&entity_type_id.to_be_bytes());
                    push_bytes(&mut key, entity_key.as_bytes());
                }
                riffdb_storage_api::IrOpaquePersistedKeyV1::IndexRangePrefix(prefix) => {
                    key.push(0x02);
                    key.extend_from_slice(&prefix.index_id().to_be_bytes());
                    push_bytes(&mut key, prefix.as_bytes());
                }
                riffdb_storage_api::IrOpaquePersistedKeyV1::PartitionIndex(target) => {
                    key.push(0x03);
                    key.extend_from_slice(&target.index_id().to_be_bytes());
                    push_bytes(&mut key, target.partition_key().as_bytes());
                }
            }
        }
        HistoricalSemanticEvidence::IndexMigrationRow(evidence) => {
            key.push(0x04);
            push_lineage(&mut key, evidence.row().schema_binding().lineage());
            key.extend_from_slice(
                &evidence
                    .row()
                    .schema_binding()
                    .contract_version()
                    .to_be_bytes(),
            );
            key.extend_from_slice(evidence.row().schema_binding().bundle_hash().as_bytes());
            key.push(0x02);
            key.extend_from_slice(&evidence.physical_key().index_id().to_be_bytes());
            push_bytes(&mut key, evidence.physical_key().as_bytes());
        }
        HistoricalSemanticEvidence::CapabilityPartition(evidence) => {
            return evidence.evidence_order_key();
        }
    }
    key
}

fn historical_semantic_bytes(evidence: &HistoricalSemanticEvidence) -> Result<usize, StorageError> {
    let bytes = match evidence {
        HistoricalSemanticEvidence::Bundle(bundle) => bundle
            .bytes()
            .as_bytes()
            .len()
            .checked_add(1 + 4 + bundle.lineage().as_bytes().len() + 8 + 32 + 4),
        HistoricalSemanticEvidence::PlanReference(plan) => {
            (1 + 4 + plan.contract_lineage().as_bytes().len()).checked_add(8 + 32 + 4 + 32)
        }
        HistoricalSemanticEvidence::ActiveCatalog(None) => Some(2),
        HistoricalSemanticEvidence::ActiveCatalog(Some(active)) => {
            (2 + 4 + active.lineage().as_bytes().len()).checked_add(8 + 32)
        }
        HistoricalSemanticEvidence::PersistedKey(persisted) => {
            let key_bytes = match persisted.key() {
                riffdb_storage_api::IrOpaquePersistedKeyV1::Entity { key, .. } => key.as_bytes(),
                riffdb_storage_api::IrOpaquePersistedKeyV1::IndexRangePrefix(prefix) => {
                    prefix.as_bytes()
                }
                riffdb_storage_api::IrOpaquePersistedKeyV1::PartitionIndex(target) => {
                    target.partition_key().as_bytes()
                }
            };
            (1 + 4 + persisted.schema().lineage().as_bytes().len())
                .checked_add(8 + 32 + 1 + 4 + 4)
                .and_then(|value| value.checked_add(key_bytes.len()))
        }
        HistoricalSemanticEvidence::IndexMigrationRow(evidence) => {
            Some(evidence.evidence_page_charge())
        }
        HistoricalSemanticEvidence::CapabilityPartition(evidence) => {
            Some(evidence.semantic_bytes().map_err(value_error_as_storage)?)
        }
    };
    bytes.ok_or_else(limit_exceeded)
}

fn read_historical_bundle(
    transaction: &ReadTransaction,
    lineage: &ContractLineage,
    version: ContractVersion,
    hash: ContractBundleHash,
) -> Result<Option<HistoricalBundleEvidence>, StorageError> {
    let key = keys::encode_contract_bundle_key(lineage, version).map_err(|_| invariant())?;
    let table = transaction
        .open_table(CONTRACT_BUNDLES)
        .map_err(table_error)?;
    let Some(value) = table.get(key.as_slice()).map_err(precommit_storage_error)? else {
        return Ok(None);
    };
    let bundle = decoded(codec::decode_contract_bundle_v1(value.value())).map_err(|_| corrupt())?;
    if bundle.lineage() != lineage
        || bundle.contract_version() != version
        || bundle.bundle_hash() != hash
        || hash_contract_bundle(bundle.canonical_bytes()) != hash
    {
        return Err(corrupt());
    }
    Ok(Some(HistoricalBundleEvidence::new(
        lineage.clone(),
        version,
        hash,
        HistoricalBundleBytes::new(bundle.canonical_bytes().to_vec())
            .map_err(value_error_as_storage)?,
    )))
}

fn decoded<T>(
    value: Result<riffdb_storage_api::EncodedPageItem<T>, StorageError>,
) -> Result<T, StructuralFindingCode> {
    value
        .map(riffdb_storage_api::EncodedPageItem::into_parts)
        .map(|(value, _)| value)
        .map_err(|error| match error.kind() {
            StorageErrorKind::LimitExceeded => StructuralFindingCode::LimitExceeded,
            _ => StructuralFindingCode::MalformedRecord,
        })
}

fn get_decoded<T>(
    transaction: &ReadTransaction,
    definition: TableDefinition<&'static [u8], &'static [u8]>,
    key: &[u8],
    decoder: impl FnOnce(&[u8]) -> Result<riffdb_storage_api::EncodedPageItem<T>, StorageError>,
) -> Result<Result<Option<T>, StructuralFindingCode>, StorageError> {
    let table = transaction.open_table(definition).map_err(table_error)?;
    let Some(value) = table.get(key).map_err(precommit_storage_error)? else {
        return Ok(Ok(None));
    };
    Ok(decoded(decoder(value.value())).map(Some))
}

fn meta_decode<T, R>(
    table: &R,
    key: &'static str,
    decoder: impl FnOnce(&[u8]) -> Result<riffdb_storage_api::EncodedPageItem<T>, StorageError>,
) -> Result<Result<T, StructuralFindingCode>, StorageError>
where
    R: ReadableTable<&'static str, &'static [u8]>,
{
    let Some(value) = table.get(key).map_err(precommit_storage_error)? else {
        return Ok(Err(StructuralFindingCode::MalformedRecord));
    };
    Ok(decoded(decoder(value.value())))
}

fn raw_exists(
    transaction: &ReadTransaction,
    definition: TableDefinition<&'static [u8], &'static [u8]>,
    key: &[u8],
) -> Result<bool, StorageError> {
    let table = transaction.open_table(definition).map_err(table_error)?;
    let found = table.get(key).map_err(precommit_storage_error)?.is_some();
    Ok(found)
}

fn plan_bundle_exists(
    transaction: &ReadTransaction,
    plan: &riffdb_storage_api::ExecutablePlanRef,
) -> Result<bool, StorageError> {
    bundle_exists(
        transaction,
        plan.contract_lineage(),
        plan.contract_version(),
        plan.contract_bundle_hash(),
    )
}

fn binding_bundle_exists(
    transaction: &ReadTransaction,
    binding: &riffdb_storage_api::DurableKeySchemaBindingV1,
) -> Result<bool, StorageError> {
    bundle_exists(
        transaction,
        binding.lineage(),
        binding.contract_version(),
        binding.bundle_hash(),
    )
}

fn bundle_pointer_exists(
    transaction: &ReadTransaction,
    pointer: &riffdb_storage_api::ActiveCatalogPointerV1,
) -> Result<bool, StorageError> {
    bundle_exists(
        transaction,
        pointer.lineage(),
        pointer.contract_version(),
        pointer.bundle_hash(),
    )
}

fn bundle_has_activation(
    transaction: &ReadTransaction,
    bundle: &riffdb_storage_api::StoredContractBundleV1,
) -> Result<bool, StorageError> {
    let table = transaction.open_table(AUDIT).map_err(table_error)?;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (physical_key, value) = entry.map_err(precommit_storage_error)?;
        let Ok(sequence) = keys::decode_audit_key(physical_key.value()) else {
            return Ok(false);
        };
        let Ok(record) = decoded(codec::decode_administration_audit_record_v1(value.value()))
        else {
            return Ok(false);
        };
        if record.administration_sequence() != sequence {
            return Ok(false);
        }
        if matches!(
            record,
            riffdb_storage_api::StoredAdministrationAuditRecordV1::Catalog(ref activation)
                if activation.activated().matches_bundle(bundle)
        ) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn query_module_contract_exists(
    transaction: &ReadTransaction,
    module: &riffdb_storage_api::StoredQueryModuleV1,
) -> Result<bool, StorageError> {
    bundle_exists(
        transaction,
        module.contract_lineage(),
        module.contract_version(),
        module.contract_bundle_hash(),
    )
}

fn query_module_pointer_exists(
    transaction: &ReadTransaction,
    pointer: &riffdb_storage_api::ActiveQueryModulePointerV1,
) -> Result<bool, StorageError> {
    if !bundle_exists(
        transaction,
        pointer.contract_lineage(),
        pointer.contract_version(),
        pointer.contract_bundle_hash(),
    )? {
        return Ok(false);
    }
    let key = keys::encode_query_module_key(pointer.module_hash());
    let module = get_decoded(
        transaction,
        QUERY_MODULES,
        &key,
        codec::decode_query_module_v1,
    )?;
    Ok(matches!(module, Ok(Some(module)) if pointer.matches_module(&module)))
}

fn query_module_has_activation(
    transaction: &ReadTransaction,
    module: &riffdb_storage_api::StoredQueryModuleV1,
) -> Result<bool, StorageError> {
    let table = transaction.open_table(AUDIT).map_err(table_error)?;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (physical_key, value) = entry.map_err(precommit_storage_error)?;
        let Ok(sequence) = keys::decode_audit_key(physical_key.value()) else {
            return Ok(false);
        };
        let Ok(record) = decoded(codec::decode_administration_audit_record_v1(value.value()))
        else {
            return Ok(false);
        };
        if record.administration_sequence() != sequence {
            return Ok(false);
        }
        if matches!(
            record,
            riffdb_storage_api::StoredAdministrationAuditRecordV1::QueryModule(ref activation)
                if activation.activated().matches_module(module)
        ) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn same_query_module_contract(
    left: &riffdb_storage_api::ActiveQueryModulePointerV1,
    right: &riffdb_storage_api::ActiveQueryModulePointerV1,
) -> bool {
    left.contract_lineage() == right.contract_lineage()
        && left.contract_version() == right.contract_version()
        && left.contract_bundle_hash() == right.contract_bundle_hash()
}

fn query_module_record_is_reciprocal(
    transaction: &ReadTransaction,
    record: &riffdb_storage_api::StoredQueryModuleAdministrationV1,
) -> Result<bool, StorageError> {
    if !query_module_pointer_exists(transaction, record.activated())? {
        return Ok(false);
    }
    let table = transaction.open_table(AUDIT).map_err(table_error)?;
    let mut previous = None;
    let mut last = None;
    let mut found = false;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (physical_key, value) = entry.map_err(precommit_storage_error)?;
        let Ok(sequence) = keys::decode_audit_key(physical_key.value()) else {
            return Ok(false);
        };
        let Ok(candidate) = decoded(codec::decode_administration_audit_record_v1(value.value()))
        else {
            return Ok(false);
        };
        if candidate.administration_sequence() != sequence {
            return Ok(false);
        }
        if let riffdb_storage_api::StoredAdministrationAuditRecordV1::QueryModule(candidate) =
            candidate
            && same_query_module_contract(candidate.activated(), record.activated())
        {
            if sequence < record.administration_sequence() {
                previous = Some(candidate.activated().clone());
            }
            if sequence == record.administration_sequence() {
                found = candidate == *record;
            }
            last = Some(candidate);
        }
    }
    let active_key = keys::encode_active_query_module_key(
        record.activated().contract_lineage(),
        record.activated().contract_version(),
        record.activated().contract_bundle_hash(),
    )
    .map_err(|_| invariant())?;
    let active = get_decoded(
        transaction,
        QUERY_MODULE_ACTIVE,
        &active_key,
        codec::decode_query_module_administration_v1,
    )?;
    Ok(found
        && record.previous_active() == previous.as_ref()
        && matches!(&active, Ok(Some(active)) if Some(active) == last.as_ref()))
}

fn catalog_record_is_reciprocal(
    transaction: &ReadTransaction,
    record: &riffdb_storage_api::StoredCatalogAdministrationV1,
) -> Result<bool, StorageError> {
    if !bundle_pointer_exists(transaction, record.activated())? {
        return Ok(false);
    }
    let table = transaction.open_table(AUDIT).map_err(table_error)?;
    let mut previous = None;
    let mut found = false;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (physical_key, value) = entry.map_err(precommit_storage_error)?;
        let Ok(sequence) = keys::decode_audit_key(physical_key.value()) else {
            return Ok(false);
        };
        let Ok(candidate) = decoded(codec::decode_administration_audit_record_v1(value.value()))
        else {
            return Ok(false);
        };
        if candidate.administration_sequence() != sequence {
            return Ok(false);
        }
        if sequence < record.administration_sequence() {
            if let riffdb_storage_api::StoredAdministrationAuditRecordV1::Catalog(candidate) =
                candidate
            {
                previous = Some(candidate.activated().clone());
            }
            continue;
        }
        if sequence == record.administration_sequence() {
            found = matches!(
                candidate,
                riffdb_storage_api::StoredAdministrationAuditRecordV1::Catalog(candidate)
                    if candidate == *record
            );
        }
        break;
    }
    Ok(found && record.previous_active() == previous.as_ref())
}

fn active_catalog_matches_last_activation(
    transaction: &ReadTransaction,
    active: Option<&riffdb_storage_api::ActiveCatalogPointerV1>,
) -> Result<bool, StorageError> {
    let table = transaction.open_table(AUDIT).map_err(table_error)?;
    let mut last = None;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (physical_key, value) = entry.map_err(precommit_storage_error)?;
        let Ok(sequence) = keys::decode_audit_key(physical_key.value()) else {
            return Ok(false);
        };
        let Ok(record) = decoded(codec::decode_administration_audit_record_v1(value.value()))
        else {
            return Ok(false);
        };
        if record.administration_sequence() != sequence {
            return Ok(false);
        }
        if let riffdb_storage_api::StoredAdministrationAuditRecordV1::Catalog(record) = record {
            last = Some(record.activated().clone());
        }
    }
    Ok(last.as_ref() == active)
}

fn bundle_exists(
    transaction: &ReadTransaction,
    lineage: &ContractLineage,
    version: ContractVersion,
    hash: ContractBundleHash,
) -> Result<bool, StorageError> {
    let key = keys::encode_contract_bundle_key(lineage, version).map_err(|_| invariant())?;
    let bundle = get_decoded(
        transaction,
        CONTRACT_BUNDLES,
        &key,
        codec::decode_contract_bundle_v1,
    )?;
    Ok(matches!(bundle, Ok(Some(value))
        if value.lineage() == lineage
            && value.contract_version() == version
            && value.bundle_hash() == hash
            && hash_contract_bundle(value.canonical_bytes()) == hash))
}

fn commit_exists(
    transaction: &ReadTransaction,
    sequence: CommitSequence,
) -> Result<bool, StorageError> {
    Ok(get_commit(transaction, sequence)?.is_some())
}

fn missing_projection_source_finding(
    transaction: &ReadTransaction,
    sequence: CommitSequence,
) -> Result<Option<StructuralFinding>, StorageError> {
    if commit_exists(transaction, sequence)? {
        return Ok(None);
    }
    let meta = transaction.open_table(META).map_err(table_error)?;
    let allocator = match meta_decode(
        &meta,
        META_APPLICATION_SEQUENCE,
        codec::decode_application_sequence_allocator_v1,
    )? {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    let was_allocated = match allocator {
        ApplicationSequenceAllocator::Next(next) => sequence < next,
        ApplicationSequenceAllocator::Exhausted => true,
    };
    Ok(Some(if was_allocated {
        authoritative(StructuralFindingCode::MissingCrossLink)
    } else {
        derived_projection(StructuralFindingCode::ProjectionStateMismatch)
    }))
}

fn get_commit(
    transaction: &ReadTransaction,
    sequence: CommitSequence,
) -> Result<Option<riffdb_storage_api::StoredCommitRecordV1>, StorageError> {
    let key = keys::encode_application_sequence_key(sequence);
    let commits = transaction.open_table(COMMITS).map_err(table_error)?;
    let events = transaction.open_table(EVENTS).map_err(table_error)?;
    let Some(value) = commits
        .get(key.as_slice())
        .map_err(precommit_storage_error)?
    else {
        return Ok(None);
    };
    match decoded(codec::decode_commit_with_event_table(
        value.value(),
        &events,
    )) {
        Ok(record) if record.commit_sequence() == sequence => Ok(Some(record)),
        Ok(_) | Err(_) => Ok(None),
    }
}

fn get_provenance(
    transaction: &ReadTransaction,
    id: riffdb_types::ProvenanceId,
) -> Result<Option<riffdb_storage_api::StoredProvenanceRecordV1>, StorageError> {
    let key = keys::encode_provenance_key(id);
    match get_decoded(
        transaction,
        PROVENANCE,
        &key,
        codec::decode_provenance_record_v1,
    )? {
        Ok(value) => Ok(value.filter(|record| record.provenance_id() == id)),
        Err(_) => Ok(None),
    }
}

fn get_terminal(
    transaction: &ReadTransaction,
    identity: &riffdb_storage_api::IdempotencyIdentity,
) -> Result<Option<IdempotencyRecordV1>, StorageError> {
    let key = identity.storage_key().map_err(|_| corrupt())?;
    match get_decoded(
        transaction,
        IDEMPOTENCY,
        key.as_bytes(),
        codec::decode_idempotency_record_v1,
    )? {
        Ok(value) => Ok(value),
        Err(_) => Ok(None),
    }
}

fn get_event(
    transaction: &ReadTransaction,
    id: riffdb_types::EventId,
) -> Result<Option<riffdb_storage_api::StoredDurableEventV1>, StorageError> {
    let key = keys::encode_event_key(id);
    match get_decoded(transaction, EVENTS, &key, codec::decode_durable_event_v1)? {
        Ok(value) => Ok(value.filter(|event| event.event_id() == id)),
        Err(_) => Ok(None),
    }
}

fn get_outbox(
    transaction: &ReadTransaction,
    id: riffdb_types::EventId,
) -> Result<Option<riffdb_storage_api::StoredOutboxIntentV1>, StorageError> {
    let key = keys::encode_event_key(id);
    let outbox = transaction.open_table(OUTBOX).map_err(table_error)?;
    let events = transaction.open_table(EVENTS).map_err(table_error)?;
    let Some(value) = outbox
        .get(key.as_slice())
        .map_err(precommit_storage_error)?
    else {
        return Ok(None);
    };
    match decoded(codec::decode_outbox_with_event_table(
        value.value(),
        &events,
    )) {
        Ok(intent) if intent.event_id() == id => Ok(Some(intent)),
        Ok(_) | Err(_) => Ok(None),
    }
}

fn outcome_graph_is_reciprocal(
    transaction: &ReadTransaction,
    outcome: &riffdb_storage_api::StoredOutcomeV1,
) -> Result<bool, StorageError> {
    let Some(commit) = get_commit(transaction, outcome.commit_sequence())? else {
        return Ok(false);
    };
    let Some(provenance) = get_provenance(transaction, outcome.provenance_id())? else {
        return Ok(false);
    };
    Ok(outcome_matches_commit(outcome, &commit)
        && provenance_matches(outcome, &commit, &provenance))
}

fn commit_graph_is_reciprocal(
    transaction: &ReadTransaction,
    commit: &riffdb_storage_api::StoredCommitRecordV1,
) -> Result<bool, StorageError> {
    let Some(provenance) = get_provenance(transaction, commit.provenance_id())? else {
        return Ok(false);
    };
    let Some(IdempotencyRecordV1::StoredOutcome(outcome)) =
        get_terminal(transaction, provenance.identity())?
    else {
        return Ok(false);
    };
    if !outcome_matches_commit(&outcome, commit)
        || !provenance_matches(&outcome, commit, &provenance)
    {
        return Ok(false);
    }
    for event in commit.events() {
        if get_event(transaction, event.event_id())?.as_ref() != Some(event)
            || get_outbox(transaction, event.event_id())?
                .as_ref()
                .map(riffdb_storage_api::StoredOutboxIntentV1::event)
                != Some(event)
        {
            return Ok(false);
        }
    }
    for mutation in commit.mutations() {
        if !current_entity_covers_mutation(transaction, mutation)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn current_entity_covers_mutation(
    transaction: &ReadTransaction,
    mutation: &riffdb_storage_api::CommittedEntityMutationV1,
) -> Result<bool, StorageError> {
    let post_image = mutation.post_image();
    let current = get_decoded(
        transaction,
        ENTITIES,
        keys::encode_entity_key(post_image.target().key()),
        codec::decode_entity_record_v1,
    )?;
    Ok(matches!(current, Ok(Some(record))
        if record.target() == post_image.target()
            && record.entity_version() >= post_image.entity_version()))
}

fn entity_history_matches(
    transaction: &ReadTransaction,
    current: &riffdb_storage_api::StoredEntityRecordV1,
) -> Result<bool, StorageError> {
    let table = transaction.open_table(COMMITS).map_err(table_error)?;
    let events = transaction.open_table(EVENTS).map_err(table_error)?;
    let mut prior: Option<riffdb_storage_api::StoredEntityRecordV1> = None;
    let mut saw_mutation = false;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (physical_key, value) = entry.map_err(precommit_storage_error)?;
        let Ok(sequence) = keys::decode_application_sequence_key(physical_key.value()) else {
            return Ok(false);
        };
        let Ok(commit) = decoded(codec::decode_commit_with_event_table(
            value.value(),
            &events,
        )) else {
            return Ok(false);
        };
        if commit.commit_sequence() != sequence {
            return Ok(false);
        }

        let mut matching = commit
            .mutations()
            .iter()
            .filter(|mutation| mutation.post_image().target() == current.target());
        let Some(mutation) = matching.next() else {
            continue;
        };
        if matching.next().is_some() {
            return Ok(false);
        }
        let expected_matches = match (prior.as_ref(), mutation.expected()) {
            (None, riffdb_storage_api::ExpectedEntityState::Absent) => true,
            (Some(prior), riffdb_storage_api::ExpectedEntityState::Present(version)) => {
                prior.entity_version() == version
            }
            (None, riffdb_storage_api::ExpectedEntityState::Present(_))
            | (Some(_), riffdb_storage_api::ExpectedEntityState::Absent) => false,
        };
        if !expected_matches {
            return Ok(false);
        }
        prior = Some(mutation.post_image().clone());
        saw_mutation = true;
    }
    Ok(saw_mutation && prior.as_ref() == Some(current))
}

fn provenance_graph_is_reciprocal(
    transaction: &ReadTransaction,
    provenance: &riffdb_storage_api::StoredProvenanceRecordV1,
) -> Result<bool, StorageError> {
    let Some(commit) = get_commit(transaction, provenance.commit_sequence())? else {
        return Ok(false);
    };
    let Some(IdempotencyRecordV1::StoredOutcome(outcome)) =
        get_terminal(transaction, provenance.identity())?
    else {
        return Ok(false);
    };
    Ok(provenance_matches(&outcome, &commit, provenance))
}

fn event_graph_is_reciprocal(
    transaction: &ReadTransaction,
    event: &riffdb_storage_api::StoredDurableEventV1,
) -> Result<bool, StorageError> {
    let Some(commit) = get_commit(transaction, event.event_id().commit_sequence())? else {
        return Ok(false);
    };
    let Ok(ordinal) = usize::try_from(event.event_id().event_ordinal()) else {
        return Ok(false);
    };
    let Some(embedded) = commit.events().get(ordinal) else {
        return Ok(false);
    };
    let Some(intent) = get_outbox(transaction, event.event_id())? else {
        return Ok(false);
    };
    Ok(embedded == event && intent.event() == event)
}

fn outcome_matches_commit(
    outcome: &riffdb_storage_api::StoredOutcomeV1,
    commit: &riffdb_storage_api::StoredCommitRecordV1,
) -> bool {
    outcome.commit_sequence() == commit.commit_sequence()
        && outcome.admission_request_id() == commit.admission_request_id()
        && outcome.plan() == commit.plan()
        && outcome.canonical_input_hash() == commit.canonical_input_hash()
        && outcome.actor() == commit.actor()
        && outcome.logical_time() == commit.logical_time()
        && outcome.partition_hash() == commit.partition_hash()
        && outcome.conflict_hashes() == commit.conflict_hashes()
        && outcome.declared_outcome() == commit.declared_outcome()
        && outcome.provenance_id() == commit.provenance_id()
        && outcome.durability_mode() == commit.durability_mode()
}

fn provenance_matches(
    outcome: &riffdb_storage_api::StoredOutcomeV1,
    commit: &riffdb_storage_api::StoredCommitRecordV1,
    provenance: &riffdb_storage_api::StoredProvenanceRecordV1,
) -> bool {
    provenance.provenance_id() == outcome.provenance_id()
        && provenance.commit_sequence() == commit.commit_sequence()
        && provenance.identity() == outcome.identity()
        && provenance.admission_request_id() == commit.admission_request_id()
        && provenance.plan() == commit.plan()
        && provenance.canonical_input_hash() == commit.canonical_input_hash()
        && provenance.actor() == commit.actor()
        && provenance.logical_time() == commit.logical_time()
        && provenance.partition_hash() == commit.partition_hash()
        && provenance.conflict_hashes() == commit.conflict_hashes()
        && provenance.outcome_id() == commit.declared_outcome().outcome_id()
        && provenance.admitted_claims() == outcome.admitted_claims()
        && provenance.event_ids() == commit.outbox_event_ids()
        && provenance.affected_entities().len() == commit.mutations().len()
        && provenance
            .affected_entities()
            .iter()
            .zip(commit.mutations())
            .all(|(affected, mutation)| {
                affected
                    == &riffdb_storage_api::AffectedEntityV1::from_record(mutation.post_image())
            })
}

fn idempotency_digest_is_readable(
    inputs: &StartupValidationInputs,
    digest: riffdb_storage_api::IdempotencyKeyDigest,
) -> bool {
    ReadableDigestKey::new(digest.scheme(), digest.key_id())
        .is_ok_and(|key| inputs.idempotency_digests().as_slice().contains(&key))
}

fn projection_control(
    transaction: &ReadTransaction,
    identity: &riffdb_types::ProjectionIdentity,
) -> Result<
    Result<Option<riffdb_storage_api::StoredProjectionControlV1>, StructuralFindingCode>,
    StorageError,
> {
    let key = riffdb_types::ProjectionFrontierKey::new(identity.clone());
    let control = get_decoded(
        transaction,
        PROJECTION_FRONTIER,
        key.as_bytes(),
        codec::decode_projection_control_v1,
    )?;
    Ok(match control {
        Ok(Some(value)) if value.identity() == identity => Ok(Some(value)),
        Ok(Some(_)) => Err(StructuralFindingCode::CrossLinkMismatch),
        Ok(None) => Ok(None),
        Err(code) => Err(code),
    })
}

fn projection_marker_exists(
    transaction: &ReadTransaction,
    key: &riffdb_types::ProjectionApplyKey,
) -> Result<bool, StorageError> {
    let marker = get_decoded(
        transaction,
        PROJECTION_APPLIED,
        key.as_bytes(),
        codec::decode_projection_apply_v1,
    )?;
    Ok(matches!(marker, Ok(Some(value)) if value.key() == key))
}

fn projection_state_namespace_has_row(
    transaction: &ReadTransaction,
    identity: &riffdb_types::ProjectionIdentity,
    generation: riffdb_types::ProjectionGeneration,
) -> Result<bool, StorageError> {
    let prefix =
        riffdb_types::ProjectionGroupPrefixBuilder::new(identity.clone(), generation).finish();
    let table = transaction
        .open_table(PROJECTION_STATE)
        .map_err(table_error)?;
    let mut rows = table
        .range(prefix.as_bytes()..)
        .map_err(precommit_storage_error)?;
    let Some(entry) = rows.next() else {
        return Ok(false);
    };
    let (key, _) = entry.map_err(precommit_storage_error)?;
    Ok(key.value().starts_with(prefix.as_bytes()))
}

fn projection_apply_namespace_prefix(
    identity: &riffdb_types::ProjectionIdentity,
    generation: riffdb_types::ProjectionGeneration,
) -> Vec<u8> {
    let first = riffdb_types::ProjectionApplyKey::new(
        identity.clone(),
        generation,
        CommitSequence::first(),
    );
    first.as_bytes()[..first.as_bytes().len() - std::mem::size_of::<u64>()].to_vec()
}

fn projection_marker_namespace_has_row(
    transaction: &ReadTransaction,
    identity: &riffdb_types::ProjectionIdentity,
    generation: riffdb_types::ProjectionGeneration,
) -> Result<bool, StorageError> {
    let prefix = projection_apply_namespace_prefix(identity, generation);
    let table = transaction
        .open_table(PROJECTION_APPLIED)
        .map_err(table_error)?;
    let mut markers = table
        .range(prefix.as_slice()..)
        .map_err(precommit_storage_error)?;
    let Some(entry) = markers.next() else {
        return Ok(false);
    };
    let (key, _) = entry.map_err(precommit_storage_error)?;
    Ok(key.value().starts_with(&prefix))
}

fn projection_marker_exists_after(
    transaction: &ReadTransaction,
    identity: &riffdb_types::ProjectionIdentity,
    generation: riffdb_types::ProjectionGeneration,
    sequence: CommitSequence,
) -> Result<bool, StorageError> {
    let Some(next) = sequence.checked_next() else {
        return Ok(false);
    };
    let prefix = projection_apply_namespace_prefix(identity, generation);
    let start = riffdb_types::ProjectionApplyKey::new(identity.clone(), generation, next);
    let table = transaction
        .open_table(PROJECTION_APPLIED)
        .map_err(table_error)?;
    let mut markers = table
        .range(start.as_bytes()..)
        .map_err(precommit_storage_error)?;
    let Some(entry) = markers.next() else {
        return Ok(false);
    };
    let (key, _) = entry.map_err(precommit_storage_error)?;
    Ok(key.value().starts_with(&prefix))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CrossLinkStatus {
    Exact,
    Missing,
    Mismatch,
}

fn audit_record_at(
    transaction: &ReadTransaction,
    sequence: riffdb_types::AdministrationSequence,
) -> Result<
    Result<Option<riffdb_storage_api::StoredAdministrationAuditRecordV1>, StructuralFindingCode>,
    StorageError,
> {
    let key = keys::encode_audit_key(sequence);
    get_decoded(
        transaction,
        AUDIT,
        &key,
        codec::decode_administration_audit_record_v1,
    )
}

fn capability_record_at(
    transaction: &ReadTransaction,
    capability_id: riffdb_types::CapabilityId,
) -> Result<
    Result<Option<riffdb_storage_api::StoredCapabilityRecordV1>, StructuralFindingCode>,
    StorageError,
> {
    let key = keys::encode_capability_key(capability_id);
    get_decoded(
        transaction,
        CAPABILITIES,
        &key,
        codec::decode_capability_record_v1,
    )
}

fn bootstrap_marker(
    transaction: &ReadTransaction,
) -> Result<
    Result<Option<riffdb_storage_api::CapabilityBootstrapMarkerV1>, StructuralFindingCode>,
    StorageError,
> {
    let meta = transaction.open_table(META).map_err(table_error)?;
    let Some(value) = meta
        .get(META_CAPABILITY_BOOTSTRAP)
        .map_err(precommit_storage_error)?
    else {
        return Ok(Ok(None));
    };
    Ok(decoded(codec::decode_capability_bootstrap_marker_v1(value.value())).map(Some))
}

fn capability_record_audit_status(
    transaction: &ReadTransaction,
    capability: &riffdb_storage_api::StoredCapabilityRecordV1,
) -> Result<CrossLinkStatus, StorageError> {
    let marker = match bootstrap_marker(transaction)? {
        Ok(value) => value,
        Err(_) => return Ok(CrossLinkStatus::Mismatch),
    };
    let expected_operation =
        if marker.is_some_and(|marker| marker.capability_id() == capability.capability_id()) {
            riffdb_storage_api::CapabilityAdministrationOperationV1::Bootstrap
        } else {
            riffdb_storage_api::CapabilityAdministrationOperationV1::Create
        };
    let creation = match audit_record_at(transaction, capability.creation_sequence())? {
        Ok(Some(riffdb_storage_api::StoredAdministrationAuditRecordV1::Capability(record))) => {
            record
        }
        Ok(None) => return Ok(CrossLinkStatus::Missing),
        Ok(Some(_)) | Err(_) => return Ok(CrossLinkStatus::Mismatch),
    };
    if creation.operation() != expected_operation
        || creation.request_id() != capability.creation_request_id()
        || creation.timestamp() != capability.issued_at()
        || creation.target_capability_id() != capability.capability_id()
        || creation.resulting_revision().get() != 1
    {
        return Ok(CrossLinkStatus::Mismatch);
    }

    if let CapabilityLifecycleV1::Revoked {
        revoked_at,
        administration_sequence,
        reason,
    } = capability.lifecycle()
    {
        let revocation = match audit_record_at(transaction, *administration_sequence)? {
            Ok(Some(riffdb_storage_api::StoredAdministrationAuditRecordV1::Capability(record))) => {
                record
            }
            Ok(None) => return Ok(CrossLinkStatus::Missing),
            Ok(Some(_)) | Err(_) => return Ok(CrossLinkStatus::Mismatch),
        };
        if revocation.operation() != riffdb_storage_api::CapabilityAdministrationOperationV1::Revoke
            || revocation.timestamp() != *revoked_at
            || revocation.target_capability_id() != capability.capability_id()
            || revocation.resulting_revision() != capability.revision()
            || revocation.revocation_reason() != Some(*reason)
        {
            return Ok(CrossLinkStatus::Mismatch);
        }
    }
    Ok(CrossLinkStatus::Exact)
}

fn capability_administration_status(
    transaction: &ReadTransaction,
    record: &riffdb_storage_api::StoredCapabilityAdministrationV1,
) -> Result<CrossLinkStatus, StorageError> {
    let capability = match capability_record_at(transaction, record.target_capability_id())? {
        Ok(Some(value)) => value,
        Ok(None) => return Ok(CrossLinkStatus::Missing),
        Err(_) => return Ok(CrossLinkStatus::Mismatch),
    };
    let marker = match bootstrap_marker(transaction)? {
        Ok(value) => value,
        Err(_) => return Ok(CrossLinkStatus::Mismatch),
    };
    let exact = match record.operation() {
        riffdb_storage_api::CapabilityAdministrationOperationV1::Bootstrap
        | riffdb_storage_api::CapabilityAdministrationOperationV1::Create => {
            let operation_matches_marker = match record.operation() {
                riffdb_storage_api::CapabilityAdministrationOperationV1::Bootstrap => marker
                    .is_some_and(|marker| marker.capability_id() == record.target_capability_id()),
                riffdb_storage_api::CapabilityAdministrationOperationV1::Create => marker
                    .is_none_or(|marker| marker.capability_id() != record.target_capability_id()),
                riffdb_storage_api::CapabilityAdministrationOperationV1::Revoke => false,
            };
            operation_matches_marker
                && capability.creation_sequence() == record.administration_sequence()
                && capability.creation_request_id() == record.request_id()
                && capability.issued_at() == record.timestamp()
                && record.resulting_revision().get() == 1
        }
        riffdb_storage_api::CapabilityAdministrationOperationV1::Revoke => {
            matches!(
                capability.lifecycle(),
                CapabilityLifecycleV1::Revoked {
                    revoked_at,
                    administration_sequence,
                    reason,
                } if *administration_sequence == record.administration_sequence()
                    && *revoked_at == record.timestamp()
                    && Some(*reason) == record.revocation_reason()
                    && capability.revision() == record.resulting_revision()
            )
        }
    };
    Ok(if exact {
        CrossLinkStatus::Exact
    } else {
        CrossLinkStatus::Mismatch
    })
}

fn bootstrap_is_consistent(
    transaction: &ReadTransaction,
    database_id: DatabaseId,
    marker: riffdb_storage_api::CapabilityBootstrapMarkerV1,
) -> Result<bool, StorageError> {
    if marker.database_id() != database_id {
        return Ok(false);
    }
    let capability = match capability_record_at(transaction, marker.capability_id())? {
        Ok(Some(value)) => value,
        Ok(None) | Err(_) => return Ok(false),
    };
    if capability.creation_sequence() != marker.administration_sequence()
        || capability_record_audit_status(transaction, &capability)? != CrossLinkStatus::Exact
    {
        return Ok(false);
    }
    let transition = match audit_record_at(transaction, marker.administration_sequence())? {
        Ok(Some(riffdb_storage_api::StoredAdministrationAuditRecordV1::Capability(record))) => {
            record
        }
        Ok(None) | Ok(Some(_)) | Err(_) => return Ok(false),
    };
    if transition.operation() != riffdb_storage_api::CapabilityAdministrationOperationV1::Bootstrap
        || transition.request_id() != capability.creation_request_id()
        || transition.timestamp() != capability.issued_at()
        || transition.target_capability_id() != marker.capability_id()
        || transition.resulting_revision().get() != 1
        || transition.initiator().is_some()
    {
        return Ok(false);
    }
    let Some(started_sequence) = marker
        .administration_sequence()
        .get()
        .checked_sub(1)
        .and_then(riffdb_types::AdministrationSequence::new)
    else {
        return Ok(false);
    };
    let started = match audit_record_at(transaction, started_sequence)? {
        Ok(Some(riffdb_storage_api::StoredAdministrationAuditRecordV1::Service(record))) => record,
        Ok(None) | Ok(Some(_)) | Err(_) => return Ok(false),
    };
    Ok(started.request_id() == transition.request_id()
        && started.timestamp() == transition.timestamp()
        && started.operation() == riffdb_types::ServiceOperationV1::CreateCapability
        && started.phase() == riffdb_types::ServiceAuditPhaseV1::Started
        && started.principal().is_none()
        && started.approval_id() == transition.approval_id()
        && started.link()
            == (riffdb_types::ServiceAuditLinkV1::ControlPlane {
                administration_sequence: marker.administration_sequence(),
            })
        && started
            .targets()
            .as_slice()
            .contains(&riffdb_types::ServiceAuditTargetV1::Capability(
                marker.capability_id(),
            ))
        && service_lifecycle_is_reciprocal(transaction, &started)?)
}

enum ObservedServiceLifecycle {
    Standalone,
    Started {
        started: riffdb_storage_api::StoredServiceAuditRecordV1,
        terminal_seen: bool,
    },
}

fn service_lifecycle_is_reciprocal(
    transaction: &ReadTransaction,
    target: &riffdb_storage_api::StoredServiceAuditRecordV1,
) -> Result<bool, StorageError> {
    let table = transaction.open_table(AUDIT).map_err(table_error)?;
    let mut lifecycle = None;
    let mut target_seen = false;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (physical_key, value) = entry.map_err(precommit_storage_error)?;
        let Ok(sequence) = keys::decode_audit_key(physical_key.value()) else {
            return Ok(false);
        };
        let Ok(record) = decoded(codec::decode_administration_audit_record_v1(value.value()))
        else {
            return Ok(false);
        };
        if record.administration_sequence() != sequence {
            return Ok(false);
        }
        let riffdb_storage_api::StoredAdministrationAuditRecordV1::Service(record) = record else {
            continue;
        };
        if record.request_id() != target.request_id() {
            continue;
        }
        target_seen |= record.administration_sequence() == target.administration_sequence()
            && record == *target;
        if !service_link_is_valid(transaction, &record)? {
            return Ok(false);
        }
        lifecycle = Some(match lifecycle {
            None if record.phase() == riffdb_types::ServiceAuditPhaseV1::Started => {
                ObservedServiceLifecycle::Started {
                    started: record,
                    terminal_seen: false,
                }
            }
            None if record.principal().is_some()
                && record.link() == riffdb_types::ServiceAuditLinkV1::None
                && matches!(
                    record.phase(),
                    riffdb_types::ServiceAuditPhaseV1::Denied
                        | riffdb_types::ServiceAuditPhaseV1::Cancelled
                        | riffdb_types::ServiceAuditPhaseV1::Failed
                ) =>
            {
                ObservedServiceLifecycle::Standalone
            }
            Some(ObservedServiceLifecycle::Started {
                started,
                terminal_seen: false,
            }) if record.administration_sequence() > started.administration_sequence()
                && record.phase() != riffdb_types::ServiceAuditPhaseV1::Started
                && service_audit_common_matches(&started, &record) =>
            {
                ObservedServiceLifecycle::Started {
                    started,
                    terminal_seen: true,
                }
            }
            Some(ObservedServiceLifecycle::Standalone)
            | Some(ObservedServiceLifecycle::Started {
                terminal_seen: true,
                ..
            })
            | Some(ObservedServiceLifecycle::Started {
                terminal_seen: false,
                ..
            })
            | None => return Ok(false),
        });
    }
    Ok(target_seen && lifecycle.is_some())
}

fn service_audit_common_matches(
    started: &riffdb_storage_api::StoredServiceAuditRecordV1,
    terminal: &riffdb_storage_api::StoredServiceAuditRecordV1,
) -> bool {
    terminal.request_id() == started.request_id()
        && terminal.operation() == started.operation()
        && terminal.principal() == started.principal()
        && terminal.ingress() == started.ingress()
        && terminal.targets() == started.targets()
        && terminal.approval_id() == started.approval_id()
        && (started.principal().is_some() || terminal.link() == started.link())
}

fn service_link_is_valid(
    transaction: &ReadTransaction,
    record: &riffdb_storage_api::StoredServiceAuditRecordV1,
) -> Result<bool, StorageError> {
    match record.link() {
        riffdb_types::ServiceAuditLinkV1::None => Ok(record.principal().is_some()
            && (record.phase() != riffdb_types::ServiceAuditPhaseV1::Succeeded
                || !matches!(
                    record.operation(),
                    riffdb_types::ServiceOperationV1::ExecuteCommand
                        | riffdb_types::ServiceOperationV1::ResolveCommandOutcome
                        | riffdb_types::ServiceOperationV1::DeployContract
                        | riffdb_types::ServiceOperationV1::DeployQueryModule
                        | riffdb_types::ServiceOperationV1::CreateCapability
                        | riffdb_types::ServiceOperationV1::RevokeCapability
                ))),
        riffdb_types::ServiceAuditLinkV1::Command {
            commit_sequence,
            provenance_id,
        } => Ok(
            record.phase() == riffdb_types::ServiceAuditPhaseV1::Succeeded
                && matches!(
                    record.operation(),
                    riffdb_types::ServiceOperationV1::ExecuteCommand
                        | riffdb_types::ServiceOperationV1::ResolveCommandOutcome
                )
                && get_commit(transaction, commit_sequence)?
                    .is_some_and(|commit| commit.provenance_id() == provenance_id)
                && get_provenance(transaction, provenance_id)?
                    .is_some_and(|provenance| provenance.commit_sequence() == commit_sequence),
        ),
        riffdb_types::ServiceAuditLinkV1::ControlPlane {
            administration_sequence,
        } => {
            let target = match audit_record_at(transaction, administration_sequence)? {
                Ok(Some(value)) => value,
                Ok(None) | Err(_) => return Ok(false),
            };
            let operation_matches = match (&target, record.operation()) {
                (
                    riffdb_storage_api::StoredAdministrationAuditRecordV1::Catalog(_),
                    riffdb_types::ServiceOperationV1::DeployContract,
                ) => true,
                (
                    riffdb_storage_api::StoredAdministrationAuditRecordV1::QueryModule(_),
                    riffdb_types::ServiceOperationV1::DeployQueryModule,
                ) => true,
                (
                    riffdb_storage_api::StoredAdministrationAuditRecordV1::Capability(target),
                    riffdb_types::ServiceOperationV1::CreateCapability,
                ) => matches!(
                    target.operation(),
                    riffdb_storage_api::CapabilityAdministrationOperationV1::Bootstrap
                        | riffdb_storage_api::CapabilityAdministrationOperationV1::Create
                ),
                (
                    riffdb_storage_api::StoredAdministrationAuditRecordV1::Capability(target),
                    riffdb_types::ServiceOperationV1::RevokeCapability,
                ) => {
                    target.operation()
                        == riffdb_storage_api::CapabilityAdministrationOperationV1::Revoke
                }
                _ => false,
            };
            if !operation_matches {
                return Ok(false);
            }
            if let riffdb_storage_api::StoredAdministrationAuditRecordV1::Capability(target) =
                &target
                && !record.targets().as_slice().contains(
                    &riffdb_types::ServiceAuditTargetV1::Capability(target.target_capability_id()),
                )
            {
                return Ok(false);
            }
            Ok(
                if record.phase() == riffdb_types::ServiceAuditPhaseV1::Started {
                    record.principal().is_none()
                        && matches!(
                            target,
                            riffdb_storage_api::StoredAdministrationAuditRecordV1::Capability(target)
                                if target.operation()
                                    == riffdb_storage_api::CapabilityAdministrationOperationV1::Bootstrap
                        )
                } else {
                    record.phase() == riffdb_types::ServiceAuditPhaseV1::Succeeded
                },
            )
        }
    }
}

fn application_allocator_matches(
    transaction: &ReadTransaction,
    allocator: ApplicationSequenceAllocator,
) -> Result<bool, StorageError> {
    let table = transaction.open_table(COMMITS).map_err(table_error)?;
    let events = transaction.open_table(EVENTS).map_err(table_error)?;
    let expected = match table.last().map_err(precommit_storage_error)? {
        None => ApplicationSequenceAllocator::initial(),
        Some((key, value)) => {
            let Ok(sequence) = keys::decode_application_sequence_key(key.value()) else {
                return Ok(false);
            };
            let Ok(record) = decoded(codec::decode_commit_with_event_table(
                value.value(),
                &events,
            )) else {
                return Ok(false);
            };
            if record.commit_sequence() != sequence {
                return Ok(false);
            }
            sequence.checked_next().map_or(
                ApplicationSequenceAllocator::Exhausted,
                ApplicationSequenceAllocator::next,
            )
        }
    };
    Ok(allocator == expected)
}

fn administration_allocator_matches(
    transaction: &ReadTransaction,
    allocator: riffdb_storage_api::AdministrationSequenceAllocator,
) -> Result<bool, StorageError> {
    let table = transaction.open_table(AUDIT).map_err(table_error)?;
    let expected = match table.last().map_err(precommit_storage_error)? {
        None => riffdb_storage_api::AdministrationSequenceAllocator::initial(),
        Some((key, value)) => {
            let Ok(sequence) = keys::decode_audit_key(key.value()) else {
                return Ok(false);
            };
            let Ok(record) = decoded(codec::decode_administration_audit_record_v1(value.value()))
            else {
                return Ok(false);
            };
            if record.administration_sequence() != sequence {
                return Ok(false);
            }
            sequence.checked_next().map_or(
                riffdb_storage_api::AdministrationSequenceAllocator::Exhausted,
                riffdb_storage_api::AdministrationSequenceAllocator::next,
            )
        }
    };
    Ok(allocator == expected)
}

fn read_active_pointer(
    transaction: &ReadTransaction,
) -> Result<
    Result<Option<riffdb_storage_api::ActiveCatalogPointerV1>, StructuralFindingCode>,
    StorageError,
> {
    let table = transaction
        .open_table(CATALOG_ACTIVE)
        .map_err(table_error)?;
    if table.len().map_err(precommit_storage_error)? > 1 {
        return Ok(Err(StructuralFindingCode::CrossLinkMismatch));
    }
    let Some((key, value)) = table.first().map_err(precommit_storage_error)? else {
        return Ok(Ok(None));
    };
    if key.value() != CATALOG_ACTIVE_KEY {
        return Ok(Err(StructuralFindingCode::MalformedRecord));
    }
    Ok(decoded(codec::decode_active_catalog_pointer_v1(value.value())).map(Some))
}

fn has_application_authoritative_state(
    transaction: &ReadTransaction,
) -> Result<bool, StorageError> {
    for definition in [
        ENTITIES,
        SECONDARY_INDEXES,
        INDEX_EPOCHS,
        IDEMPOTENCY,
        IDEMPOTENCY_PENDING,
        COMMITS,
        PROVENANCE,
        EVENTS,
        OUTBOX,
    ] {
        if table_len(transaction, definition)? != 0 {
            return Ok(true);
        }
    }
    Ok(false)
}

fn push_lineage(output: &mut Vec<u8>, lineage: &ContractLineage) {
    let length =
        u32::try_from(lineage.as_bytes().len()).expect("foundational lineage hard bound fits u32");
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(lineage.as_bytes());
}

fn push_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    let length = u32::try_from(bytes.len()).expect("foundational key hard bound fits u32");
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(bytes);
}

fn authoritative(code: StructuralFindingCode) -> StructuralFinding {
    StructuralFinding::new(StructuralFindingScope::Authoritative, code)
}

fn derived_outbox(code: StructuralFindingCode) -> StructuralFinding {
    StructuralFinding::new(StructuralFindingScope::OutboxDelivery, code)
}

fn derived_projection(code: StructuralFindingCode) -> StructuralFinding {
    StructuralFinding::new(StructuralFindingScope::Projection, code)
}

fn invariant() -> StorageError {
    storage_error(StorageErrorKind::InvariantViolation)
}

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

fn limit_exceeded() -> StorageError {
    storage_error(StorageErrorKind::LimitExceeded)
}

fn value_error_as_storage(error: StorageValueError) -> StorageError {
    match error {
        StorageValueError::LimitExceeded | StorageValueError::SizeOverflow => limit_exceeded(),
        _ => invariant(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, OnceLock};

    use riffdb_catalog::{
        CatalogHistoryOutcome, CatalogIndexMigrationDriveError, CatalogIndexMigrationDriver,
        ValidatedContractBundle, validate_catalog_history,
    };
    use riffdb_contract_compiler::compile_contract_source;
    use riffdb_storage_api::{
        ActiveCatalogPointerV1, AdministrationSequenceAllocator, AuditPrincipalV1,
        CapabilityAdministrationOperationV1, CapabilityBootstrapMarkerV1, CapabilityGrantV1,
        CapabilityPermissionKindV1, CapabilityPermissionV1, CapabilityPermissionsV1,
        CapabilityRequestedRecordV1, CatalogActivationIntentV1, CatalogAdministrationRepository,
        DatabaseInitializationPort, DurableKeySchemaBindingV1, HistoricalEvidencePage,
        PartitionScopeV1, ProjectionGenerationPosition, ProjectionLifecycleV1,
        PublishedApplyModeV1, ReadableCapabilityDigestInventory,
        ReadableIdempotencyDigestInventory, RevocationReasonCodeV1,
        StoredAdministrationAuditRecordV1, StoredCapabilityAdministrationV1,
        StoredCapabilityRecordV1, StoredCatalogAdministrationV1, StoredContractBundleV1,
        StoredEntityRecordV1, StoredIndexEntryV1, StoredIndexEntryV2, StoredProjectionApplyV1,
        StoredProjectionControlV1, StoredServiceAuditRecordV1, StructuralEvidenceEnd,
        StructuralEvidencePage,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdministrationSequence, AggregateTypeId, Audience, CanonicalRecord,
        CanonicalValue, CapabilityId, CapabilityTokenDigest, DigestKeyId, EntityKeyBuilder,
        EntityTypeId, EntityVersion, Environment, FieldId, IndexEntryKeyBuilder,
        PartitionKeyBuilder, ProjectionApplyHash, ProjectionApplyKey, ProjectionGeneration,
        ProjectionId, ProjectionIdentity, ProjectionPlanHash, RequestId, ScopedPartitionV1,
        ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetsV1, ServiceIngressKindV1,
        ServiceOperationV1, TenantScope, Timestamp,
    };

    use super::*;

    static NEXT_TEST_PATH: AtomicU64 = AtomicU64::new(1);

    const REDB_MIGRATION_CONTRACT: &str = r#"
contract RedbMigration version 1 {
  entity Row {
    key (id: u64)
    field value: u64
    index ByValue(value)
  }

  aggregate Rows {
    root Row
    partition_by id
    conflict_key (id)
  }
}
"#;

    fn validated_migration_bundle() -> &'static ValidatedContractBundle {
        static BUNDLE: OnceLock<ValidatedContractBundle> = OnceLock::new();
        BUNDLE.get_or_init(|| {
            ValidatedContractBundle::from_compiler_bundle(
                compile_contract_source(REDB_MIGRATION_CONTRACT)
                    .expect("compile indexed redb migration contract"),
            )
            .expect("validate indexed redb migration bundle")
        })
    }

    struct TestDatabasePath(PathBuf);

    impl TestDatabasePath {
        fn new(label: &str) -> Self {
            let ordinal = NEXT_TEST_PATH.fetch_add(1, Ordering::Relaxed);
            Self(std::env::temp_dir().join(format!(
                "riffdb-redb-startup-{label}-{}-{ordinal}.redb",
                std::process::id()
            )))
        }
    }

    impl Drop for TestDatabasePath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn database_id(seed: u8) -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [seed; 10])
            .expect("valid deterministic UUIDv7")
    }

    fn uuid_bytes(seed: u8) -> [u8; 16] {
        let mut bytes = [seed; 16];
        bytes[6] = 0x70 | (seed & 0x0f);
        bytes[8] = 0x80 | (seed & 0x3f);
        bytes
    }

    fn capability_id(seed: u8) -> CapabilityId {
        CapabilityId::from_bytes(uuid_bytes(seed)).expect("capability ID")
    }

    fn request_id(seed: u8) -> RequestId {
        RequestId::from_bytes(uuid_bytes(seed)).expect("request ID")
    }

    fn audit_principal(seed: u8) -> AuditPrincipalV1 {
        AuditPrincipalV1::new(
            ActorId::new("operator").expect("actor ID"),
            ActorKind::Human,
            capability_id(seed),
            NonZeroU64::MIN,
        )
    }

    fn inputs() -> StartupValidationInputs {
        inputs_at(1)
    }

    fn inputs_at(seconds: i64) -> StartupValidationInputs {
        let key = ReadableDigestKey::v1(DigestKeyId::new(1).expect("digest key"));
        StartupValidationInputs::new(
            Timestamp::new(seconds, 0).expect("timestamp"),
            ReadableCapabilityDigestInventory::new(vec![key]).expect("capability inventory"),
            ReadableIdempotencyDigestInventory::new(vec![key]).expect("idempotency inventory"),
        )
    }

    fn initialized_store(path: &TestDatabasePath, id: DatabaseId) -> RedbStore {
        let mut store = RedbStore::open(&path.0).expect("open store");
        store.initialize_database(id).expect("initialize store");
        store
    }

    fn deployed_migration_store(path: &TestDatabasePath, id: DatabaseId) -> RedbStore {
        let store = initialized_store(path, id);
        let dormant = RedbDormantPorts {
            shared: Arc::clone(&store.shared),
        };
        drop(store);
        let mut ports = dormant
            .into_operational_after_catalog_validation()
            .expect("activate migration fixture ports");
        let bundle = validated_migration_bundle()
            .to_stored()
            .expect("stored migration bundle");
        let intent = CatalogActivationIntentV1::new(
            None,
            bundle,
            request_id(0x51),
            audit_principal(0x52),
            Timestamp::new(1, 0).expect("activation timestamp"),
            None,
        );
        ports
            .activate_catalog(&intent)
            .expect("activate migration catalog");
        drop(ports);
        RedbStore::open(&path.0).expect("reopen deployed migration store")
    }

    fn compiled_migration_legacy_row(value: u64) -> StoredIndexEntryV1 {
        let bundle = validated_migration_bundle();
        let entity_schema = bundle
            .bundle()
            .schema()
            .entities()
            .first()
            .expect("migration entity");
        let index_schema = entity_schema.indexes().first().expect("migration index");
        let mut entity = EntityKeyBuilder::new(entity_schema.id());
        entity.push_u64(value).expect("entity key component");
        let mut index = IndexEntryKeyBuilder::new(index_schema.id());
        index.push_u64(value).expect("index key component");
        StoredIndexEntryV1::new(
            index
                .finish(entity.finish().expect("entity key"))
                .expect("index key"),
            DurableKeySchemaBindingV1::new(
                bundle.lineage().clone(),
                bundle.contract_version(),
                bundle.bundle_hash(),
            ),
            CanonicalRecord::new(Vec::new()).expect("covered values"),
        )
        .expect("legacy migration row")
    }

    fn compiled_migration_partition(value: u64) -> riffdb_types::PartitionKey {
        let aggregate = validated_migration_bundle()
            .bundle()
            .schema()
            .aggregates()
            .first()
            .expect("migration aggregate");
        let mut partition = PartitionKeyBuilder::new(aggregate.id());
        partition.push_u64(value).expect("partition key component");
        partition.finish().expect("partition key")
    }

    fn stored_bundle(lineage: &str, version: u64, bytes: &[u8]) -> StoredContractBundleV1 {
        StoredContractBundleV1::new(
            ContractLineage::new(lineage).expect("lineage"),
            ContractVersion::new(version).expect("version"),
            hash_contract_bundle(bytes),
            bytes.to_vec(),
        )
        .expect("stored bundle")
    }

    fn requested_capability(database_id: DatabaseId) -> CapabilityRequestedRecordV1 {
        requested_capability_with_scope(database_id, PartitionScopeV1::All)
    }

    fn requested_capability_with_scope(
        database_id: DatabaseId,
        partition_scope: PartitionScopeV1,
    ) -> CapabilityRequestedRecordV1 {
        let permissions = CapabilityPermissionsV1::new(vec![
            CapabilityPermissionV1::unparameterized(
                CapabilityPermissionKindV1::AdministerCapabilities,
            )
            .expect("permission"),
        ])
        .expect("permissions");
        let grant = CapabilityGrantV1::new(
            TenantScope::Global,
            partition_scope,
            permissions,
            Vec::new(),
            NonZeroU16::MIN,
            Vec::new(),
        )
        .expect("grant");
        CapabilityRequestedRecordV1::new(
            database_id,
            Environment::new("test").expect("environment"),
            ActorId::new("subject").expect("subject"),
            ActorKind::Human,
            NonZeroU32::new(60).expect("duration"),
            vec![Audience::new("riffdb-test").expect("audience")],
            grant,
        )
        .expect("requested capability")
    }

    fn scoped_partition(lineage: &ContractLineage, value: u64) -> ScopedPartitionV1 {
        let mut builder =
            PartitionKeyBuilder::new(AggregateTypeId::new(0x0102_0304).expect("aggregate ID"));
        builder.push_u64(value).expect("partition component");
        ScopedPartitionV1::new(lineage.clone(), builder.finish().expect("partition key"))
    }

    fn explicit_scope(lineage: &ContractLineage, values: &[u64]) -> PartitionScopeV1 {
        PartitionScopeV1::explicit(
            values
                .iter()
                .map(|value| scoped_partition(lineage, *value))
                .collect(),
        )
        .expect("explicit scope")
    }

    fn active_capability(
        database_id: DatabaseId,
        capability_id: CapabilityId,
        partition_scope: PartitionScopeV1,
        issued_at: i64,
        request_seed: u8,
    ) -> StoredCapabilityRecordV1 {
        StoredCapabilityRecordV1::active(
            capability_id,
            CapabilityTokenDigest::from_hmac_bytes(
                DigestKeyId::new(1).expect("digest key ID"),
                [request_seed; 32],
            ),
            requested_capability_with_scope(database_id, partition_scope),
            Timestamp::new(issued_at, 0).expect("issued at"),
            Timestamp::new(issued_at + 60, 0).expect("expires at"),
            AdministrationSequence::first(),
            request_id(request_seed),
        )
        .expect("active capability")
    }

    fn insert_capabilities(store: &RedbStore, records: &[StoredCapabilityRecordV1]) {
        let write = store
            .shared
            .database
            .begin_write()
            .expect("write transaction");
        {
            let mut capabilities = write.open_table(CAPABILITIES).expect("capability table");
            for record in records {
                let key = keys::encode_capability_key(record.capability_id());
                let value = codec::encode_capability_record_v1(record).expect("encode capability");
                capabilities
                    .insert(key.as_slice(), value.as_bytes())
                    .expect("insert capability");
            }
        }
        write.commit().expect("commit capabilities");
    }

    fn projection_identity() -> ProjectionIdentity {
        ProjectionIdentity::new(
            ContractLineage::new("projection-integrity").expect("lineage"),
            ProjectionId::first(),
            ProjectionPlanHash::from_bytes([0x91; 32]),
        )
    }

    fn collect_structural(
        session: &mut RedbStructuralEvidenceSession,
    ) -> (RedbStructuralEvidenceEnd, Vec<StructuralFinding>) {
        let mut cursor =
            StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
        let mut collected = Vec::new();
        loop {
            match session
                .read_structural_evidence(cursor, EvidencePageLimit::new(1).expect("page limit"))
                .expect("structural page")
            {
                StructuralEvidencePage::Page {
                    start,
                    findings,
                    next,
                } => {
                    assert_eq!(start, cursor);
                    collected.extend(findings);
                    cursor = next;
                }
                StructuralEvidencePage::ExactEnd(end) => {
                    assert_eq!(end.cursor(), cursor);
                    return (end, collected);
                }
            }
        }
    }

    fn finish_structural(session: &mut RedbStructuralEvidenceSession) -> RedbStructuralEvidenceEnd {
        let (end, findings) = collect_structural(session);
        assert!(findings.is_empty());
        end
    }

    fn finish_historical(session: &mut RedbStructuralEvidenceSession) -> RedbHistoricalEvidenceEnd {
        let (end, evidence, _) = collect_historical(session, 1);
        assert!(
            evidence
                .iter()
                .any(|item| { matches!(item, HistoricalSemanticEvidence::ActiveCatalog(None)) })
        );
        end
    }

    fn collect_historical(
        session: &mut RedbStructuralEvidenceSession,
        page_limit: u32,
    ) -> (
        RedbHistoricalEvidenceEnd,
        Vec<HistoricalSemanticEvidence>,
        Vec<usize>,
    ) {
        let mut cursor =
            HistoricalEvidenceCursor::start(session.database_id(), session.open_session_id());
        let mut collected = Vec::new();
        let mut page_lengths = Vec::new();
        loop {
            match session
                .read_historical_evidence(
                    cursor,
                    EvidencePageLimit::new(page_limit).expect("page limit"),
                )
                .expect("historical page")
            {
                HistoricalEvidencePage::Page {
                    start,
                    evidence,
                    next,
                } => {
                    assert_eq!(start, cursor);
                    page_lengths.push(evidence.len());
                    collected.extend(evidence);
                    cursor = next;
                }
                HistoricalEvidencePage::ExactEnd(end) => {
                    assert_eq!(end.cursor(), cursor);
                    return (end, collected, page_lengths);
                }
            }
        }
    }

    #[test]
    fn initialized_empty_store_reaches_both_exact_ends_and_reopens() {
        let path = TestDatabasePath::new("empty");
        let id = database_id(0x11);
        let store = initialized_store(&path, id);
        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin evidence");
        assert_eq!(session.database_id(), id);
        assert!(
            session
                .read_historical_bundle(
                    &ContractLineage::new("missing").expect("lineage"),
                    ContractVersion::new(1).expect("version"),
                    ContractBundleHash::from_bytes([0x33; 32]),
                )
                .expect("point lookup")
                .is_none()
        );
        let structural_end = finish_structural(&mut session);
        let historical_end = finish_historical(&mut session);
        assert!(
            session
                .read_historical_bundle(
                    &ContractLineage::new("missing").expect("lineage"),
                    ContractVersion::new(1).expect("version"),
                    ContractBundleHash::from_bytes([0x33; 32]),
                )
                .expect("point lookup remains live after exact ends")
                .is_none()
        );
        let outcome = session
            .finish(structural_end, historical_end)
            .expect("finish evidence");
        let StructuralOpenOutcome::Clean(opened) = outcome else {
            panic!("empty V2 store must finish cleanly");
        };
        assert_eq!(opened.database_id(), id);
        assert_eq!(opened.retained_metadata(), &RetainedMetadataV1::initial(id));
        let (opened_id, opened_session, metadata, dormant) = opened.into_parts();
        assert_eq!(opened_id, id);
        assert!(opened_session.get() > 0);
        assert_eq!(metadata, RetainedMetadataV1::initial(id));
        drop(dormant);

        let reopened = RedbStore::open(&path.0).expect("reopen after handoff drop");
        assert_eq!(
            riffdb_storage_api::DatabaseIdentityProbePort::probe_database_identity(&reopened)
                .expect("probe reopened"),
            riffdb_storage_api::DatabaseIdentityProbe::Existing(id)
        );
    }

    #[test]
    fn migration_compare_mismatch_rolls_back_the_complete_catalog_batch() {
        let path = TestDatabasePath::new("migration-compare-mismatch");
        let id = database_id(0x18);
        let store = deployed_migration_store(&path, id);
        let rows = [
            compiled_migration_legacy_row(1),
            compiled_migration_legacy_row(2),
        ];
        let envelopes = rows
            .iter()
            .map(|row| {
                riffdb_storage_api::encode_index_entry_v1_fixture(row)
                    .expect("encode V1 migration row")
                    .into_bytes()
            })
            .collect::<Vec<_>>();
        let write = store
            .shared
            .database
            .begin_write()
            .expect("seed migration transaction");
        {
            let mut table = write
                .open_table(SECONDARY_INDEXES)
                .expect("secondary index table");
            for (row, envelope) in rows.iter().zip(&envelopes) {
                table
                    .insert(row.key().as_bytes(), envelope.as_slice())
                    .expect("insert V1 migration row");
            }
        }
        write.commit().expect("commit V1 migration rows");

        let substituted = StoredIndexEntryV2::new(
            rows[1].key().clone(),
            rows[1].schema_binding().clone(),
            CanonicalRecord::new(vec![(FieldId::first(), CanonicalValue::U64(999))])
                .expect("substituted covered values"),
            compiled_migration_partition(2),
        )
        .expect("substituted V2 row");
        let substituted_envelope = codec::encode_index_entry_v2(&substituted)
            .expect("encode substituted V2 row")
            .into_bytes();

        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin migration evidence");
        let structural_end = finish_structural(&mut session);
        let (catalog_outcome, historical_end) = validate_catalog_history(&mut session)
            .expect("catalog validates migration history")
            .into_parts();
        let CatalogHistoryOutcome::MigrationRequired(context) = catalog_outcome else {
            panic!("V1 catalog history must require migration");
        };
        let StructuralOpenOutcome::MigrationRequired(mut port) = session
            .finish(structural_end, historical_end)
            .expect("finish migration evidence")
        else {
            panic!("V1 storage history must retain a migration port");
        };
        port.substitute_before_apply = Some(substituted);
        let error = CatalogIndexMigrationDriver::new(context, port)
            .expect("bind same-session migration driver")
            .run()
            .expect_err("stale compare must reject the complete batch");
        assert!(matches!(
            error,
            CatalogIndexMigrationDriveError::Storage(ref error)
                if error.kind() == StorageErrorKind::CorruptData
        ));

        let reopened = RedbStore::open(&path.0).expect("reopen after rejected batch");
        let read = reopened
            .shared
            .database
            .begin_read()
            .expect("read rejected batch state");
        let table = read
            .open_table(SECONDARY_INDEXES)
            .expect("secondary index table");
        assert_eq!(
            table
                .get(rows[0].key().as_bytes())
                .expect("read first row")
                .expect("first row exists")
                .value(),
            envelopes[0].as_slice(),
            "the first rewrite must roll back when the second compare is stale"
        );
        assert_eq!(
            table
                .get(rows[1].key().as_bytes())
                .expect("read second row")
                .expect("second row exists")
                .value(),
            substituted_envelope.as_slice(),
            "the independently committed stale row must remain exact"
        );
    }

    #[test]
    fn retained_metadata_decode_reads_all_six_categories_from_one_snapshot() {
        let path = TestDatabasePath::new("retained-metadata-six-categories");
        let id = database_id(0x19);
        let store = initialized_store(&path, id);
        let active = ActiveCatalogPointerV1::new(
            ContractLineage::new("retained").expect("lineage"),
            ContractVersion::new(11).expect("version"),
            ContractBundleHash::from_bytes([0x41; 32]),
        );
        let marker = CapabilityBootstrapMarkerV1::new(
            id,
            capability_id(0x42),
            AdministrationSequence::new(13).expect("administration sequence"),
        );
        let application = codec::encode_application_sequence_allocator_v1(
            ApplicationSequenceAllocator::Exhausted,
        )
        .expect("encode application allocator");
        let administration = codec::encode_administration_sequence_allocator_v1(
            AdministrationSequenceAllocator::Exhausted,
        )
        .expect("encode administration allocator");
        let encoded_active =
            codec::encode_active_catalog_pointer_v1(&active).expect("encode active pointer");
        let encoded_marker =
            codec::encode_capability_bootstrap_marker_v1(marker).expect("encode bootstrap marker");
        let write = store
            .shared
            .database
            .begin_write()
            .expect("write transaction");
        {
            let mut meta = write.open_table(META).expect("metadata table");
            meta.insert(META_APPLICATION_SEQUENCE, application.as_bytes())
                .expect("replace application allocator");
            meta.insert(META_ADMINISTRATION_SEQUENCE, administration.as_bytes())
                .expect("replace administration allocator");
            meta.insert(META_CAPABILITY_BOOTSTRAP, encoded_marker.as_bytes())
                .expect("insert bootstrap marker");
        }
        {
            let mut catalog = write.open_table(CATALOG_ACTIVE).expect("active table");
            catalog
                .insert(CATALOG_ACTIVE_KEY.as_slice(), encoded_active.as_bytes())
                .expect("insert active pointer");
        }
        write.commit().expect("commit retained metadata fixture");

        let read = store
            .shared
            .database
            .begin_read()
            .expect("read transaction");
        let observed = read_retained_metadata(&read).expect("decode retained metadata");
        assert_eq!(observed.storage_format_version().get(), 2);
        assert_eq!(observed.database_id(), id);
        assert_eq!(
            observed.application_sequence(),
            ApplicationSequenceAllocator::Exhausted
        );
        assert_eq!(
            observed.administration_sequence(),
            AdministrationSequenceAllocator::Exhausted
        );
        assert_eq!(observed.active_catalog(), Some(&active));
        assert_eq!(observed.capability_bootstrap(), Some(marker));
    }

    #[test]
    fn mismatched_retained_metadata_is_rejected_before_handoff() {
        let path = TestDatabasePath::new("retained-metadata-mismatch");
        let store = initialized_store(&path, database_id(0x1a));
        let marker = CapabilityBootstrapMarkerV1::new(
            database_id(0x1b),
            capability_id(0x43),
            AdministrationSequence::first(),
        );
        let encoded_marker =
            codec::encode_capability_bootstrap_marker_v1(marker).expect("encode marker");
        let write = store
            .shared
            .database
            .begin_write()
            .expect("write transaction");
        {
            let mut meta = write.open_table(META).expect("metadata table");
            meta.insert(META_CAPABILITY_BOOTSTRAP, encoded_marker.as_bytes())
                .expect("insert mismatched marker");
        }
        write.commit().expect("commit corrupt fixture");

        assert_eq!(
            store
                .begin_structural_evidence(inputs())
                .expect_err("database-mismatched marker cannot publish a session")
                .kind(),
            StorageErrorKind::CorruptData
        );
        RedbStore::open(&path.0).expect("failed open releases the database handle");
    }

    #[test]
    fn capability_partition_history_emits_exact_qualifying_inventory() {
        let path = TestDatabasePath::new("capability-partition-inventory");
        let database_id = database_id(0x12);
        let store = initialized_store(&path, database_id);
        let lineage = ContractLineage::new("budget").expect("lineage");
        let active_first_id = capability_id(0x21);
        let active_duplicate_id = capability_id(0x22);
        let all_id = capability_id(0x23);
        let expired_id = capability_id(0x24);
        let exact_expiry_id = capability_id(0x25);
        let future_issued_id = capability_id(0x26);
        let revoked_id = capability_id(0x27);

        let active_first = active_capability(
            database_id,
            active_first_id,
            explicit_scope(&lineage, &[1, 2]),
            20,
            0x61,
        );
        let active_duplicate = active_capability(
            database_id,
            active_duplicate_id,
            explicit_scope(&lineage, &[1]),
            20,
            0x62,
        );
        let all = active_capability(database_id, all_id, PartitionScopeV1::All, 20, 0x63);
        let expired = active_capability(
            database_id,
            expired_id,
            explicit_scope(&lineage, &[3]),
            0,
            0x64,
        );
        let exact_expiry = active_capability(
            database_id,
            exact_expiry_id,
            explicit_scope(&lineage, &[4]),
            10,
            0x65,
        );
        let future_issued = active_capability(
            database_id,
            future_issued_id,
            explicit_scope(&lineage, &[5]),
            100,
            0x66,
        );
        let revoked = active_capability(
            database_id,
            revoked_id,
            explicit_scope(&lineage, &[6]),
            20,
            0x67,
        )
        .revoked(
            NonZeroU64::MIN,
            Timestamp::new(30, 0).expect("revoked at"),
            AdministrationSequence::new(2).expect("revoke sequence"),
            RevocationReasonCodeV1::Requested,
        )
        .expect("revoked capability");
        insert_capabilities(
            &store,
            &[
                active_first,
                active_duplicate,
                all,
                expired,
                exact_expiry,
                future_issued,
                revoked,
            ],
        );

        let mut session = store
            .begin_structural_evidence(inputs_at(70))
            .expect("begin evidence");
        let (_, evidence, page_lengths) = collect_historical(&mut session, 2);
        assert_eq!(page_lengths, vec![2, 2, 1]);
        assert!(matches!(
            evidence.first(),
            Some(HistoricalSemanticEvidence::ActiveCatalog(None))
        ));
        let partitions = evidence
            .iter()
            .filter_map(|item| match item {
                HistoricalSemanticEvidence::CapabilityPartition(evidence) => Some(evidence),
                _ => None,
            })
            .collect::<Vec<_>>();
        let observed = partitions
            .iter()
            .map(|evidence| (evidence.capability_id(), evidence.entry_ordinal()))
            .collect::<BTreeSet<_>>();
        let expected = BTreeSet::from([
            (active_first_id, 0),
            (active_first_id, 1),
            (active_duplicate_id, 0),
            (future_issued_id, 0),
        ]);
        assert_eq!(observed, expected);
        let duplicated_first = partitions
            .iter()
            .find(|evidence| {
                evidence.capability_id() == active_first_id && evidence.entry_ordinal() == 0
            })
            .expect("first duplicate");
        let duplicated_second = partitions
            .iter()
            .find(|evidence| evidence.capability_id() == active_duplicate_id)
            .expect("second duplicate");
        assert_eq!(
            duplicated_first.scoped_partition(),
            duplicated_second.scoped_partition(),
            "equal keys in distinct capabilities remain distinct evidence items"
        );
    }

    #[test]
    fn capability_partition_history_preserves_all_1024_ordinals_across_pages() {
        let path = TestDatabasePath::new("capability-partition-pages");
        let database_id = database_id(0x13);
        let store = initialized_store(&path, database_id);
        let lineage = ContractLineage::new("budget").expect("lineage");
        let capability_id = capability_id(0x31);
        let values = (0..1_024).collect::<Vec<u64>>();
        let capability = active_capability(
            database_id,
            capability_id,
            explicit_scope(&lineage, &values),
            20,
            0x71,
        );
        insert_capabilities(&store, &[capability]);

        let mut session = store
            .begin_structural_evidence(inputs_at(70))
            .expect("begin evidence");
        let (_, evidence, page_lengths) = collect_historical(&mut session, 500);
        assert_eq!(page_lengths, vec![500, 500, 25]);
        let partitions = evidence
            .into_iter()
            .filter_map(|item| match item {
                HistoricalSemanticEvidence::CapabilityPartition(evidence) => Some(evidence),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(partitions.len(), 1_024);
        assert!(
            partitions
                .iter()
                .all(|evidence| evidence.capability_id() == capability_id)
        );
        assert_eq!(
            partitions
                .iter()
                .map(|evidence| evidence.entry_ordinal())
                .collect::<Vec<_>>(),
            (0..1_024).collect::<Vec<u16>>()
        );
        assert!(
            partitions
                .windows(2)
                .all(|pair| { pair[0].evidence_order_key() < pair[1].evidence_order_key() })
        );
    }

    #[test]
    fn capability_partition_history_matches_the_shared_golden_vector() {
        let path = TestDatabasePath::new("capability-partition-golden");
        let database_id = database_id(0x14);
        let store = initialized_store(&path, database_id);
        let capability_id = CapabilityId::from_bytes([
            0x01, 0x8f, 0x00, 0x00, 0x00, 0x00, 0x70, 0x01, 0x80, 0x02, 0x11, 0x22, 0x33, 0x44,
            0x55, 0x66,
        ])
        .expect("golden capability UUIDv7");
        let budget = ContractLineage::new("budget").expect("lineage");
        let scope = PartitionScopeV1::explicit(vec![
            scoped_partition(&ContractLineage::new("a").expect("lineage"), 1),
            scoped_partition(&budget, 2),
            scoped_partition(&budget, 0x0102_0304_0506_0708),
        ])
        .expect("golden scope");
        let capability = active_capability(database_id, capability_id, scope, 20, 0x72);
        insert_capabilities(&store, &[capability]);

        let mut session = store
            .begin_structural_evidence(inputs_at(70))
            .expect("begin evidence");
        let (_, evidence, _) = collect_historical(&mut session, 500);
        let golden = evidence
            .into_iter()
            .find_map(|item| match item {
                HistoricalSemanticEvidence::CapabilityPartition(evidence)
                    if evidence.entry_ordinal() == 2 =>
                {
                    Some(evidence)
                }
                _ => None,
            })
            .expect("golden capability evidence");
        let expected = vec![
            0x05, 0x01, 0x8f, 0x00, 0x00, 0x00, 0x00, 0x70, 0x01, 0x80, 0x02, 0x11, 0x22, 0x33,
            0x44, 0x55, 0x66, 0x00, 0x02, 0x00, 0x00, 0x00, 0x06, b'b', b'u', b'd', b'g', b'e',
            b't', 0x01, 0x02, 0x03, 0x04, 0x00, 0x00, 0x00, 0x0e, 0x50, 0x01, 0x01, 0x02, 0x03,
            0x04, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
        ];
        assert_eq!(golden.capability_id(), capability_id);
        assert_eq!(golden.semantic_bytes().expect("semantic charge"), 51);
        assert_eq!(golden.evidence_order_key(), expected);
        assert_eq!(
            historical_order_key(&HistoricalSemanticEvidence::CapabilityPartition(golden)),
            expected
        );
    }

    #[test]
    fn cursors_are_session_bound_and_single_use() {
        let path = TestDatabasePath::new("cursor");
        let store = initialized_store(&path, database_id(0x22));
        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin evidence");
        let start =
            StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
        let page = session
            .read_structural_evidence(start, EvidencePageLimit::new(1).expect("page limit"))
            .expect("first page");
        let StructuralEvidencePage::Page { next, .. } = page else {
            panic!("initialized metadata requires a non-final page");
        };
        assert_eq!(
            session
                .read_structural_evidence(start, EvidencePageLimit::new(1).expect("page limit"),)
                .expect_err("cursor replay must fail")
                .kind(),
            StorageErrorKind::InvariantViolation
        );
        let wrong_session = StructuralEvidenceCursor::start(
            session.database_id(),
            OpenSessionId::new(session.open_session_id().get() + 1).expect("session ID"),
        );
        assert_eq!(
            session
                .read_structural_evidence(
                    wrong_session,
                    EvidencePageLimit::new(1).expect("page limit"),
                )
                .expect_err("wrong session must fail")
                .kind(),
            StorageErrorKind::InvariantViolation
        );
        assert!(next.position() > start.position());
    }

    #[test]
    fn same_count_value_drift_is_rejected_by_the_session_commit_epoch() {
        let path = TestDatabasePath::new("same-count-drift");
        let store = initialized_store(&path, database_id(0x23));
        let seed = store
            .shared
            .database
            .begin_write()
            .expect("begin seed transaction");
        {
            let mut table = seed
                .open_table(SECONDARY_INDEXES)
                .expect("open secondary-index table");
            table
                .insert(b"same-key".as_slice(), b"before".as_slice())
                .expect("insert seed value");
        }
        store
            .shared
            .commit_durable(seed)
            .expect("commit seed value");

        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin anchored evidence session");
        let drift = session
            .shared
            .database
            .begin_write()
            .expect("begin deliberate internal bypass");
        {
            let mut table = drift
                .open_table(SECONDARY_INDEXES)
                .expect("open secondary-index table");
            assert!(
                table
                    .insert(b"same-key".as_slice(), b"after!".as_slice())
                    .expect("replace same-count value")
                    .is_some()
            );
        }
        session
            .shared
            .commit_durable(drift)
            .expect("commit deliberate same-count drift");

        let start =
            StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
        assert_eq!(
            session
                .read_structural_evidence(start, EvidencePageLimit::new(1).expect("page limit"))
                .expect_err("commit-epoch drift must fail closed")
                .kind(),
            StorageErrorKind::CorruptData
        );
    }

    #[test]
    fn evidence_requires_initialization_and_dropped_session_releases_the_open() {
        let path = TestDatabasePath::new("exclusive");
        let store = RedbStore::open(&path.0).expect("open empty container");
        assert_eq!(
            store
                .begin_structural_evidence(inputs())
                .expect_err("uninitialized evidence must fail")
                .kind(),
            StorageErrorKind::CorruptData
        );

        let store = initialized_store(&path, database_id(0x44));
        let session = store
            .begin_structural_evidence(inputs())
            .expect("begin evidence");
        assert!(RedbStore::open(&path.0).is_err());
        drop(session);
        RedbStore::open(&path.0).expect("dropped session releases database open");
    }

    #[test]
    fn independent_stores_receive_process_unique_open_session_ids() {
        let first_path = TestDatabasePath::new("session-a");
        let second_path = TestDatabasePath::new("session-b");
        let first = initialized_store(&first_path, database_id(0x55))
            .begin_structural_evidence(inputs())
            .expect("first session");
        let second = initialized_store(&second_path, database_id(0x66))
            .begin_structural_evidence(inputs())
            .expect("second session");
        assert_ne!(first.open_session_id(), second.open_session_id());
    }

    #[test]
    fn current_entity_without_any_committed_post_image_is_not_reciprocal() {
        let path = TestDatabasePath::new("orphan-entity");
        let store = initialized_store(&path, database_id(0x71));
        let entity_type = EntityTypeId::first();
        let mut key = EntityKeyBuilder::new(entity_type);
        key.push_u64(1).expect("entity key component");
        let target =
            riffdb_storage_api::EntityTarget::new(entity_type, key.finish().expect("entity key"))
                .expect("entity target");
        let bundle = stored_bundle("entity-history", 1, b"entity-history-bundle");
        let current = StoredEntityRecordV1::new(
            target,
            EntityVersion::first(),
            bundle.contract_version(),
            DurableKeySchemaBindingV1::new(
                bundle.lineage().clone(),
                bundle.contract_version(),
                bundle.bundle_hash(),
            ),
            CanonicalRecord::new(Vec::new()).expect("entity fields"),
        )
        .expect("entity row");
        let transaction = store
            .shared
            .database
            .begin_read()
            .expect("read transaction");
        assert!(!entity_history_matches(&transaction, &current).expect("history check"));
    }

    #[test]
    fn catalog_chain_accepts_reactivation_and_rejects_a_wrong_previous_pointer() {
        let path = TestDatabasePath::new("catalog-chain");
        let store = initialized_store(&path, database_id(0x72));
        let first_bundle = stored_bundle("catalog-chain", 1, b"catalog-one");
        let second_bundle = stored_bundle("catalog-chain", 2, b"catalog-two");
        let first_pointer = ActiveCatalogPointerV1::from_bundle(&first_bundle);
        let second_pointer = ActiveCatalogPointerV1::from_bundle(&second_bundle);
        let first = StoredCatalogAdministrationV1::from_stored_parts(
            AdministrationSequence::first(),
            request_id(0x31),
            Timestamp::new(1, 0).expect("timestamp"),
            audit_principal(0x41),
            None,
            first_pointer.clone(),
            None,
        );
        let second = StoredCatalogAdministrationV1::from_stored_parts(
            AdministrationSequence::new(2).expect("sequence"),
            request_id(0x32),
            Timestamp::new(2, 0).expect("timestamp"),
            audit_principal(0x41),
            Some(first_pointer.clone()),
            second_pointer.clone(),
            None,
        );
        let third = StoredCatalogAdministrationV1::from_stored_parts(
            AdministrationSequence::new(3).expect("sequence"),
            request_id(0x33),
            Timestamp::new(3, 0).expect("timestamp"),
            audit_principal(0x41),
            Some(second_pointer),
            first_pointer,
            None,
        );
        let encoded_first_bundle =
            codec::encode_contract_bundle_v1(&first_bundle).expect("encode first bundle");
        let encoded_second_bundle =
            codec::encode_contract_bundle_v1(&second_bundle).expect("encode second bundle");
        let encoded_first = codec::encode_administration_audit_record_v1(
            &StoredAdministrationAuditRecordV1::Catalog(first.clone()),
        )
        .expect("encode first activation");
        let encoded_second = codec::encode_administration_audit_record_v1(
            &StoredAdministrationAuditRecordV1::Catalog(second.clone()),
        )
        .expect("encode second activation");
        let encoded_third = codec::encode_administration_audit_record_v1(
            &StoredAdministrationAuditRecordV1::Catalog(third.clone()),
        )
        .expect("encode third activation");
        let first_bundle_key = keys::encode_contract_bundle_key(
            first_bundle.lineage(),
            first_bundle.contract_version(),
        )
        .expect("first bundle key");
        let second_bundle_key = keys::encode_contract_bundle_key(
            second_bundle.lineage(),
            second_bundle.contract_version(),
        )
        .expect("second bundle key");
        let write = store
            .shared
            .database
            .begin_write()
            .expect("write transaction");
        {
            let mut bundles = write.open_table(CONTRACT_BUNDLES).expect("bundle table");
            bundles
                .insert(first_bundle_key.as_slice(), encoded_first_bundle.as_bytes())
                .expect("insert first bundle");
            bundles
                .insert(
                    second_bundle_key.as_slice(),
                    encoded_second_bundle.as_bytes(),
                )
                .expect("insert second bundle");
        }
        {
            let mut audit = write.open_table(AUDIT).expect("audit table");
            audit
                .insert(
                    keys::encode_audit_key(first.administration_sequence()).as_slice(),
                    encoded_first.as_bytes(),
                )
                .expect("insert first activation");
            audit
                .insert(
                    keys::encode_audit_key(second.administration_sequence()).as_slice(),
                    encoded_second.as_bytes(),
                )
                .expect("insert second activation");
            audit
                .insert(
                    keys::encode_audit_key(third.administration_sequence()).as_slice(),
                    encoded_third.as_bytes(),
                )
                .expect("insert third activation");
        }
        write.commit().expect("commit fixture");

        let read = store
            .shared
            .database
            .begin_read()
            .expect("read transaction");
        assert!(catalog_record_is_reciprocal(&read, &first).expect("first chain link"));
        assert!(catalog_record_is_reciprocal(&read, &second).expect("second chain link"));
        assert!(catalog_record_is_reciprocal(&read, &third).expect("reactivation chain link"));
        assert!(bundle_has_activation(&read, &first_bundle).expect("first activation"));
        assert!(bundle_has_activation(&read, &second_bundle).expect("second activation"));
        assert!(
            active_catalog_matches_last_activation(&read, Some(third.activated()))
                .expect("latest activation")
        );
        drop(read);

        let wrong_second = StoredCatalogAdministrationV1::from_stored_parts(
            second.administration_sequence(),
            second.request_id(),
            second.timestamp(),
            second.principal().clone(),
            None,
            second.activated().clone(),
            second.approval_id().cloned(),
        );
        let encoded_wrong_second = codec::encode_administration_audit_record_v1(
            &StoredAdministrationAuditRecordV1::Catalog(wrong_second.clone()),
        )
        .expect("encode wrong second activation");
        let write = store
            .shared
            .database
            .begin_write()
            .expect("write transaction");
        {
            let mut audit = write.open_table(AUDIT).expect("audit table");
            audit
                .insert(
                    keys::encode_audit_key(wrong_second.administration_sequence()).as_slice(),
                    encoded_wrong_second.as_bytes(),
                )
                .expect("replace second activation");
        }
        write.commit().expect("commit corruption");
        let read = store
            .shared
            .database
            .begin_read()
            .expect("read transaction");
        assert!(
            !catalog_record_is_reciprocal(&read, &wrong_second).expect("wrong previous pointer")
        );
    }

    #[test]
    fn capability_create_and_revoke_audits_are_checked_in_both_directions() {
        let path = TestDatabasePath::new("capability-audit");
        let database_id = database_id(0x73);
        let store = initialized_store(&path, database_id);
        let capability_id = capability_id(0x51);
        let issued_at = Timestamp::new(10, 0).expect("issued at");
        let active = StoredCapabilityRecordV1::active(
            capability_id,
            CapabilityTokenDigest::from_hmac_bytes(
                DigestKeyId::new(1).expect("digest key ID"),
                [0x61; 32],
            ),
            requested_capability(database_id),
            issued_at,
            Timestamp::new(70, 0).expect("expires at"),
            AdministrationSequence::first(),
            request_id(0x52),
        )
        .expect("active capability");
        let revoked_at = Timestamp::new(20, 0).expect("revoked at");
        let revoked = active
            .revoked(
                NonZeroU64::MIN,
                revoked_at,
                AdministrationSequence::new(2).expect("revoke sequence"),
                RevocationReasonCodeV1::Requested,
            )
            .expect("revoked capability");
        let create = StoredCapabilityAdministrationV1::new(
            AdministrationSequence::first(),
            active.creation_request_id(),
            CapabilityAdministrationOperationV1::Create,
            issued_at,
            Some(audit_principal(0x41)),
            capability_id,
            NonZeroU64::MIN,
            None,
            None,
        )
        .expect("create audit");
        let revoke = StoredCapabilityAdministrationV1::new(
            AdministrationSequence::new(2).expect("revoke sequence"),
            request_id(0x53),
            CapabilityAdministrationOperationV1::Revoke,
            revoked_at,
            Some(audit_principal(0x41)),
            capability_id,
            NonZeroU64::new(2).expect("revision"),
            None,
            Some(RevocationReasonCodeV1::Requested),
        )
        .expect("revoke audit");
        let encoded_capability =
            codec::encode_capability_record_v1(&revoked).expect("encode capability");
        let encoded_create = codec::encode_administration_audit_record_v1(
            &StoredAdministrationAuditRecordV1::Capability(create.clone()),
        )
        .expect("encode create");
        let encoded_revoke = codec::encode_administration_audit_record_v1(
            &StoredAdministrationAuditRecordV1::Capability(revoke.clone()),
        )
        .expect("encode revoke");
        let write = store
            .shared
            .database
            .begin_write()
            .expect("write transaction");
        {
            let mut capabilities = write.open_table(CAPABILITIES).expect("capability table");
            capabilities
                .insert(
                    keys::encode_capability_key(capability_id).as_slice(),
                    encoded_capability.as_bytes(),
                )
                .expect("insert capability");
        }
        {
            let mut audit = write.open_table(AUDIT).expect("audit table");
            audit
                .insert(
                    keys::encode_audit_key(create.administration_sequence()).as_slice(),
                    encoded_create.as_bytes(),
                )
                .expect("insert create");
            audit
                .insert(
                    keys::encode_audit_key(revoke.administration_sequence()).as_slice(),
                    encoded_revoke.as_bytes(),
                )
                .expect("insert revoke");
        }
        write.commit().expect("commit fixture");
        {
            let read = store
                .shared
                .database
                .begin_read()
                .expect("read transaction");
            assert_eq!(
                capability_record_audit_status(&read, &revoked).expect("record links"),
                CrossLinkStatus::Exact
            );
            assert_eq!(
                capability_administration_status(&read, &create).expect("create link"),
                CrossLinkStatus::Exact
            );
            assert_eq!(
                capability_administration_status(&read, &revoke).expect("revoke link"),
                CrossLinkStatus::Exact
            );
        }

        let wrong_revoke = StoredCapabilityAdministrationV1::new(
            revoke.administration_sequence(),
            revoke.request_id(),
            CapabilityAdministrationOperationV1::Revoke,
            revoke.timestamp(),
            Some(audit_principal(0x41)),
            capability_id,
            revoke.resulting_revision(),
            None,
            Some(RevocationReasonCodeV1::PolicyChange),
        )
        .expect("type-valid wrong revoke");
        let encoded_wrong = codec::encode_administration_audit_record_v1(
            &StoredAdministrationAuditRecordV1::Capability(wrong_revoke.clone()),
        )
        .expect("encode wrong revoke");
        let write = store
            .shared
            .database
            .begin_write()
            .expect("write transaction");
        {
            let mut audit = write.open_table(AUDIT).expect("audit table");
            audit
                .insert(
                    keys::encode_audit_key(wrong_revoke.administration_sequence()).as_slice(),
                    encoded_wrong.as_bytes(),
                )
                .expect("replace revoke");
        }
        write.commit().expect("commit corruption");
        let read = store
            .shared
            .database
            .begin_read()
            .expect("read transaction");
        assert_eq!(
            capability_record_audit_status(&read, &revoked).expect("record mismatch"),
            CrossLinkStatus::Mismatch
        );
        assert_eq!(
            capability_administration_status(&read, &wrong_revoke).expect("audit mismatch"),
            CrossLinkStatus::Mismatch
        );
    }

    #[test]
    fn duplicate_standalone_service_lifecycle_is_authoritative_corruption() {
        let path = TestDatabasePath::new("duplicate-service");
        let store = initialized_store(&path, database_id(0x74));
        let request_id = request_id(0x61);
        let first = StoredServiceAuditRecordV1::from_stored_parts(
            AdministrationSequence::first(),
            request_id,
            Timestamp::new(1, 0).expect("timestamp"),
            ServiceOperationV1::GetHealth,
            ServiceAuditPhaseV1::Denied,
            Some(audit_principal(0x62)),
            ServiceIngressKindV1::Grpc,
            ServiceAuditTargetsV1::empty(),
            None,
            ServiceAuditLinkV1::None,
        )
        .expect("first standalone audit");
        let second = StoredServiceAuditRecordV1::from_stored_parts(
            AdministrationSequence::new(2).expect("sequence"),
            request_id,
            Timestamp::new(2, 0).expect("timestamp"),
            ServiceOperationV1::GetHealth,
            ServiceAuditPhaseV1::Denied,
            Some(audit_principal(0x62)),
            ServiceIngressKindV1::Grpc,
            ServiceAuditTargetsV1::empty(),
            None,
            ServiceAuditLinkV1::None,
        )
        .expect("second standalone audit");
        let encoded_first = codec::encode_administration_audit_record_v1(
            &StoredAdministrationAuditRecordV1::Service(first),
        )
        .expect("encode first service audit");
        let encoded_second = codec::encode_administration_audit_record_v1(
            &StoredAdministrationAuditRecordV1::Service(second),
        )
        .expect("encode second service audit");
        let allocator = codec::encode_administration_sequence_allocator_v1(
            AdministrationSequenceAllocator::next(
                AdministrationSequence::new(3).expect("allocator sequence"),
            ),
        )
        .expect("encode allocator");
        let write = store
            .shared
            .database
            .begin_write()
            .expect("write transaction");
        {
            let mut audit = write.open_table(AUDIT).expect("audit table");
            audit
                .insert(
                    keys::encode_audit_key(AdministrationSequence::first()).as_slice(),
                    encoded_first.as_bytes(),
                )
                .expect("insert first service audit");
            audit
                .insert(
                    keys::encode_audit_key(
                        AdministrationSequence::new(2).expect("second sequence"),
                    )
                    .as_slice(),
                    encoded_second.as_bytes(),
                )
                .expect("insert second service audit");
        }
        {
            let mut meta = write.open_table(META).expect("metadata table");
            meta.insert(META_ADMINISTRATION_SEQUENCE, allocator.as_bytes())
                .expect("advance allocator");
        }
        write.commit().expect("commit fixture");

        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin evidence");
        let (structural_end, findings) = collect_structural(&mut session);
        assert!(findings.iter().all(|finding| {
            finding.scope() == StructuralFindingScope::Authoritative
                && finding.code() == StructuralFindingCode::CrossLinkMismatch
        }));
        assert_eq!(findings.len(), 2);
        let historical_end = finish_historical(&mut session);
        let error = session
            .finish(structural_end, historical_end)
            .expect_err("authoritative findings must withhold ports");
        assert_eq!(error.kind(), StorageErrorKind::CorruptData);

        let reopened = RedbStore::open(&path.0).expect("reopen corrupt fixture");
        let mut repeated = reopened
            .begin_structural_evidence(inputs())
            .expect("repeat evidence");
        let (repeated_structural_end, repeated_findings) = collect_structural(&mut repeated);
        assert_eq!(repeated_findings, findings);
        let repeated_historical_end = finish_historical(&mut repeated);
        let repeated_error = repeated
            .finish(repeated_structural_end, repeated_historical_end)
            .expect_err("repeated authoritative findings must withhold ports");
        assert_eq!(repeated_error.kind(), StorageErrorKind::CorruptData);
    }

    #[test]
    fn projection_frontier_and_marker_prefix_defects_are_derived_only() {
        let path = TestDatabasePath::new("before-first-marker");
        let store = initialized_store(&path, database_id(0x75));
        let identity = projection_identity();
        let control = StoredProjectionControlV1::initial(identity.clone());
        let marker_key = ProjectionApplyKey::new(
            identity,
            ProjectionGeneration::first(),
            CommitSequence::first(),
        );
        let marker = StoredProjectionApplyV1::new(
            marker_key.clone(),
            ProjectionApplyHash::from_bytes([0x92; 32]),
        );
        let gap_identity = ProjectionIdentity::new(
            ContractLineage::new("projection-gap").expect("lineage"),
            ProjectionId::new(2).expect("projection ID"),
            ProjectionPlanHash::from_bytes([0x93; 32]),
        );
        let gap_frontier = CommitSequence::new(3).expect("gap frontier");
        let gap_control = StoredProjectionControlV1::new(
            gap_identity.clone(),
            ProjectionGeneration::first(),
            Some(ProjectionGenerationPosition::new(
                ProjectionGeneration::first(),
                FrontierPosition::AppliedThrough(gap_frontier),
            )),
            None,
            Some(PublishedApplyModeV1::Enabled),
            ProjectionLifecycleV1::Ready,
            None,
        )
        .expect("gap control");
        let gap_first_key = ProjectionApplyKey::new(
            gap_identity.clone(),
            ProjectionGeneration::first(),
            CommitSequence::first(),
        );
        let gap_last_key =
            ProjectionApplyKey::new(gap_identity, ProjectionGeneration::first(), gap_frontier);
        let gap_first = StoredProjectionApplyV1::new(
            gap_first_key.clone(),
            ProjectionApplyHash::from_bytes([0x94; 32]),
        );
        let gap_last = StoredProjectionApplyV1::new(
            gap_last_key.clone(),
            ProjectionApplyHash::from_bytes([0x95; 32]),
        );
        let encoded_control =
            codec::encode_projection_control_v1(&control).expect("encode control");
        let encoded_marker = codec::encode_projection_apply_v1(&marker).expect("encode marker");
        let encoded_gap_control =
            codec::encode_projection_control_v1(&gap_control).expect("encode gap control");
        let encoded_gap_first =
            codec::encode_projection_apply_v1(&gap_first).expect("encode first gap marker");
        let encoded_gap_last =
            codec::encode_projection_apply_v1(&gap_last).expect("encode last gap marker");
        let control_key = riffdb_types::ProjectionFrontierKey::new(control.identity().clone());
        let gap_control_key =
            riffdb_types::ProjectionFrontierKey::new(gap_control.identity().clone());
        let write = store
            .shared
            .database
            .begin_write()
            .expect("write transaction");
        {
            let mut controls = write
                .open_table(PROJECTION_FRONTIER)
                .expect("control table");
            controls
                .insert(control_key.as_bytes(), encoded_control.as_bytes())
                .expect("insert control");
            controls
                .insert(gap_control_key.as_bytes(), encoded_gap_control.as_bytes())
                .expect("insert gap control");
        }
        {
            let mut markers = write.open_table(PROJECTION_APPLIED).expect("marker table");
            markers
                .insert(marker_key.as_bytes(), encoded_marker.as_bytes())
                .expect("insert marker");
            markers
                .insert(gap_first_key.as_bytes(), encoded_gap_first.as_bytes())
                .expect("insert first gap marker");
            markers
                .insert(gap_last_key.as_bytes(), encoded_gap_last.as_bytes())
                .expect("insert last gap marker");
        }
        write.commit().expect("commit fixture");

        {
            let read = store
                .shared
                .database
                .begin_read()
                .expect("read transaction");
            assert_eq!(
                inspect_projection_apply_row(
                    &read,
                    gap_last_key.as_bytes(),
                    encoded_gap_last.as_bytes(),
                )
                .expect("inspect marker gap"),
                Some(derived_projection(
                    StructuralFindingCode::ProjectionStateMismatch,
                ))
            );
        }

        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin evidence");
        let (structural_end, findings) = collect_structural(&mut session);
        assert!(!findings.is_empty());
        assert!(findings.iter().all(|finding| {
            finding.scope() == StructuralFindingScope::Projection
                && finding.code() == StructuralFindingCode::ProjectionStateMismatch
        }));
        let historical_end = finish_historical(&mut session);
        let outcome = session
            .finish(structural_end, historical_end)
            .expect("derived findings do not withhold ports");
        let StructuralOpenOutcome::Clean(opened) = outcome else {
            panic!("V2 derived-state fixture must finish cleanly");
        };
        assert_eq!(opened.database_id(), database_id(0x75));
    }
}
