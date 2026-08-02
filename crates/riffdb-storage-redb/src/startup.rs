//! Exclusive read-only startup evidence over one immutable redb snapshot.

use std::collections::{BTreeSet, VecDeque};
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
    CommitSequence, ContractBundleHash, ContractLineage, ContractMigrationOperationId,
    ContractVersion, DatabaseId, FrontierPosition, MigrationBundleHash, hash_contract_bundle,
    hash_query_module,
};

use crate::codec::{self, IdempotencyRecordV1};
use crate::error::{precommit_storage_error, storage_error, table_error, transaction_error};
use crate::gate::ExclusiveLease;
use crate::hooks::RedbTestOperation;
use crate::keys;
use crate::layout::{
    AUDIT, AUDIT_BY_REQUEST, CAPABILITIES, CAPABILITY_TOKENS, CATALOG_ACTIVE, CATALOG_ACTIVE_KEY,
    COMMITS, CONTRACT_BUNDLES, CONTRACT_MIGRATION_JOURNAL, CONTRACT_MIGRATIONS,
    CONTRACT_WRITE_RETIREMENTS, ENTITIES, EVENT_ROUTES, EVENTS, HISTORY_TOMBSTONES, IDEMPOTENCY,
    IDEMPOTENCY_PENDING, INDEX_EPOCHS, META, META_ADMINISTRATION_SEQUENCE,
    META_APPLICATION_SEQUENCE, META_CAPABILITY_BOOTSTRAP, META_DATABASE_ID, META_FORMAT_VERSION,
    META_HISTORY_INCARNATION, META_INDEX_EPOCH_ROWS_REPAIRED, META_KEYS, META_RECORD_REGISTRY,
    META_RETENTION_HOLDS, META_RETENTION_WATERMARK, META_VALIDATED_PREFIX_CHECKPOINT, OUTBOX,
    OUTBOX_STATUS, PROJECTION_APPLIED, PROJECTION_FRONTIER, PROJECTION_STATE, PROVENANCE,
    QUERY_MODULE_ACTIVE, QUERY_MODULES, RETIRED_ENTITIES, SECONDARY_INDEXES, TABLE_NAMES,
};
use crate::store::{
    PRE_AUDIT_REQUEST_INDEX_REGISTRY_DIGEST, PRE_CONTRACT_MIGRATION_REGISTRY_DIGEST,
    PRE_ENTITY_REFERENCE_REGISTRY_DIGEST, PRE_EVENT_ROUTE_REGISTRY_DIGEST,
    PRE_HISTORY_INCARNATION_REGISTRY_DIGEST, PRE_INDEX_GENERATION_REGISTRY_DIGEST,
    PRE_RETENTION_WATERMARK_REGISTRY_DIGEST, PRE_VALIDATED_PREFIX_CHECKPOINT_REGISTRY_DIGEST,
    RedbDormantPorts, RedbStore, SharedRedb,
};

static NEXT_OPEN_SESSION: AtomicU64 = AtomicU64::new(1);
const STRUCTURAL_TABLE_COUNT: usize = 28;

fn startup_registry_is_supported(digest: riffdb_types::SchemaHash) -> bool {
    digest == riffdb_storage_api::proto_codec::current_record_registry_digest()
        || digest == riffdb_types::SchemaHash::from_bytes(PRE_EVENT_ROUTE_REGISTRY_DIGEST)
        || digest == riffdb_types::SchemaHash::from_bytes(PRE_ENTITY_REFERENCE_REGISTRY_DIGEST)
        || digest == riffdb_types::SchemaHash::from_bytes(PRE_CONTRACT_MIGRATION_REGISTRY_DIGEST)
        || digest
            == riffdb_types::SchemaHash::from_bytes(PRE_VALIDATED_PREFIX_CHECKPOINT_REGISTRY_DIGEST)
        || digest == riffdb_types::SchemaHash::from_bytes(PRE_RETENTION_WATERMARK_REGISTRY_DIGEST)
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

/// One entity's continuity state built from a single forward COMMITS pass.
struct EntityChain {
    version: riffdb_types::EntityVersion,
    /// Known after a command commit; absent after an archive-proven migration transition.
    hash: Option<riffdb_types::EntityRecordHash>,
    /// Exact successor bundle expected while the terminal hash is not commit-derived.
    expected_bundle: Option<ContractBundleHash>,
    /// Next ordered migration that may affect this entity.
    migration_cursor: usize,
    intact: bool,
    /// Set when an ENTITIES structural row claims this chain.
    consumed: bool,
    /// Seeded from a verified checkpoint's at-S map and never touched by a
    /// suffix commit: the chain carries a fingerprint-verified version but no
    /// post-image hash, so row matching accepts version-only for it. Hash
    /// checks resume the moment a suffix reference touches the chain.
    seeded: bool,
}

/// Cap on reported orphan targets (hostile COMMITS must not allocate unboundedly).
const ENTITY_CHAIN_ORPHAN_REPORT_CAP: usize = 16;
/// Max structural findings emitted for unconsumed/overflow orphans:
/// up to [`ENTITY_CHAIN_ORPHAN_REPORT_CAP`] detail findings plus one aggregate
/// when the true problem count exceeds the cap (truncation-visible).
const ENTITY_CHAIN_ORPHAN_FINDING_CAP: usize = ENTITY_CHAIN_ORPHAN_REPORT_CAP + 1;
// Finishing-page reservation requires room for the bounded orphan set.
const _: () = assert!(riffdb_storage_api::MAX_INTEGRITY_FINDINGS > ENTITY_CHAIN_ORPHAN_FINDING_CAP);

/// Compact locator for one historical evidence item; page serve re-materializes
/// by point lookup (no retained canonical payloads).
#[derive(Clone, Debug)]
enum EvidenceLocator {
    Bundle(Vec<u8>),
    ContractMigrationEdge(ContractBundleHash),
    PlanReference(riffdb_storage_api::ExecutablePlanRef),
    ActiveCatalog,
    PersistedKeyEntity(Vec<u8>),
    IndexMigration(Vec<u8>),
    PersistedKeyEpoch(Vec<u8>),
    CapabilityPartition {
        capability_id: riffdb_types::CapabilityId,
        entry_ordinal: usize,
    },
}

impl EvidenceLocator {
    fn retained_bytes(&self) -> usize {
        match self {
            Self::Bundle(key)
            | Self::PersistedKeyEntity(key)
            | Self::IndexMigration(key)
            | Self::PersistedKeyEpoch(key) => key.len().saturating_add(8),
            Self::ContractMigrationEdge(_) => 32 + 8,
            Self::PlanReference(plan) => plan
                .contract_lineage()
                .as_bytes()
                .len()
                .saturating_add(8 + 32 + 4 + 32 + 8),
            Self::ActiveCatalog => 8,
            Self::CapabilityPartition { .. } => 16 + 8 + 8,
        }
    }
}

struct HistoricalEvidencePlan {
    /// Sorted unique (order_key, compact locator) pairs — no payload retention.
    entries: Vec<(Vec<u8>, EvidenceLocator)>,
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
    /// Walk plan counts (full or suffix-adjusted under a verified checkpoint).
    structural_counts: [u64; STRUCTURAL_TABLE_COUNT],
    /// Full table lens from the immutable snapshot (for below-S count verification).
    full_structural_counts: [u64; STRUCTURAL_TABLE_COUNT],
    structural_total: u64,
    next_structural: StructuralEvidenceCursor,
    next_historical: HistoricalEvidenceCursor,
    last_historical_key: Option<Vec<u8>>,
    structural_finished: bool,
    historical_finished: bool,
    authoritative_finding_seen: bool,
    /// True once ANY structural finding of ANY scope was produced (including a
    /// sample-window finding that dropped the fast path). Gates the checkpoint
    /// write: a finding of any severity vetoes it (ADR-0019 A1).
    any_finding_seen: bool,
    saw_v1_index: bool,
    /// Held for the structural evidence pass only; must not outlive the session.
    structural_read: Option<ReadTransaction>,
    structural_cursors: Option<StructuralCursors>,
    /// Built at ENTITIES phase entry; dropped after orphan findings are queued.
    entity_chains: Option<EntityChainState>,
    /// Bounded cross-link findings for unconsumed/orphan chains (VecDeque: O(1) drain).
    pending_entity_orphan_findings: VecDeque<StructuralFinding>,
    historical_plan: Option<HistoricalEvidencePlan>,
    /// Verified active checkpoint enabling prefix-skipping startup.
    checkpoint: Option<crate::validated_prefix::ActiveCheckpoint>,
    /// Rows inspected via deterministic sample windows (test observability).
    sampled_window_rows_inspected: u64,
    /// True when a checkpoint was verified and the fast path is active.
    checkpoint_verified: bool,
    /// Why a present-but-unusable checkpoint was ignored (test/operator observability).
    checkpoint_ignored_reason: Option<crate::validated_prefix::CheckpointIgnoreReason>,
    /// Walked suffix rows for range-skipped sequence-keyed tables.
    walked_suffix_counts: [u64; STRUCTURAL_TABLE_COUNT],
    /// Prefix rows classified-and-skipped during the full walks of the
    /// non-sequence-prefixed tables (EVENT_ROUTES/IDEMPOTENCY/AUDIT_BY_REQUEST).
    walked_prefix_counts: [u64; STRUCTURAL_TABLE_COUNT],
}

/// Cached single-pass entity history state for the ENTITIES structural phase.
struct EntityChainState {
    chains: std::collections::BTreeMap<riffdb_storage_api::EntityTarget, EntityChain>,
    migrations: Vec<EntityMigrationEvidence>,
    /// Bounded sample of commit-referenced targets with no ENTITIES slot left.
    orphan_targets: Vec<riffdb_storage_api::EntityTarget>,
    /// True once any target was rejected from the map (capacity = entity_count).
    /// Production-meaningful: drives the aggregate truncation finding in
    /// [`RedbStructuralEvidenceSession::queue_entity_orphan_findings`].
    overflow: bool,
    orphans_queued: bool,
    /// Verified retention watermark at chain build (0 = unpruned). Below this
    /// floor, commit bodies are tombstone-covered absence: chains may START at
    /// any version, and an ENTITIES row untouched by any retained commit
    /// validates by fingerprint-of-current shape, not by replay (ADR-0085 A2).
    pruned_floor: u64,
}

/// Bounded permanent evidence for one successful catalog migration.
#[derive(Clone, Copy)]
struct EntityMigrationEvidence {
    operation_id: ContractMigrationOperationId,
    migration: MigrationBundleHash,
    candidate: ContractBundleHash,
    successor_frontier: Option<CommitSequence>,
    administration_sequence: riffdb_types::AdministrationSequence,
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
        let full_structural_counts = snapshot.structural_counts;
        let mut structural_counts = snapshot.structural_counts;
        let mut structural_total = snapshot.structural_total;
        // Re-validate retention watermark bindings (never advance). Verify the
        // tombstone chain covers [1, watermark] when pruned history exists —
        // against the rooting registry digest RECORDED in the watermark
        // record, never the process's current digest (ADR-0085 A2), so a
        // registry migration cannot invalidate an existing chain.
        {
            let watermark = crate::retention::load_watermark(&transaction)?;
            if let Some(wm) = watermark.as_ref() {
                if wm.history_incarnation() != snapshot.retained_metadata.history_incarnation() {
                    return Err(corrupt());
                }
                let recomputed = wm
                    .computed_hash()
                    .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
                if recomputed != wm.watermark_hash() {
                    return Err(corrupt());
                }
                // The watermark must never pass the durable application head:
                // sequences committed later must not be born below it.
                let head = crate::retention::durable_application_head(&transaction)?;
                if wm.watermark_sequence() > head {
                    return Err(corrupt());
                }
                crate::retention::verify_tombstone_chain(
                    &transaction,
                    wm.watermark_sequence(),
                    wm.chain_root_registry_digest(),
                )?;
            } else {
                // Absent watermark means sequence 0; any tombstones are corrupt.
                crate::retention::verify_tombstone_chain(&transaction, 0, None)?;
            }
        }
        // A new validation session invalidates any previous session's clean claim
        // until this one finishes with zero findings (checkpoint write gate).
        self.shared.set_startup_validation_clean(false);
        let mut checkpoint = None;
        let mut checkpoint_verified = false;
        let mut checkpoint_ignored_reason = None;
        let mut sampled_window_rows_inspected = 0_u64;
        let mut sample_finding_seen = false;
        match crate::validated_prefix::load_active_checkpoint(
            &transaction,
            database_id,
            snapshot.retained_metadata.history_incarnation(),
            &full_structural_counts,
        ) {
            Ok(active) => {
                let c = active.counts;
                structural_counts[10] = full_structural_counts[10].saturating_sub(c.commits_count);
                structural_counts[12] = full_structural_counts[12].saturating_sub(c.events_count);
                structural_counts[14] = full_structural_counts[14].saturating_sub(c.outbox_count);
                structural_counts[15] =
                    full_structural_counts[15].saturating_sub(c.outbox_status_count);
                structural_counts[21] = full_structural_counts[21].saturating_sub(c.audit_count);
                // Execute existing inspect functions over deterministically sampled
                // prefix windows (ADR-0085 sample policy). Findings fail closed.
                let (inspected, sample_finding) = run_checkpoint_sample_windows(
                    &transaction,
                    &active.checkpoint_hash,
                    active.checkpoint_commit_sequence,
                    active.audit_sequence_bound,
                )?;
                sampled_window_rows_inspected = inspected;
                if sample_finding.is_some() {
                    // Drop the fast path; the full pass re-emits the finding.
                    // The finding still vetoes the checkpoint write below.
                    structural_counts = full_structural_counts;
                    structural_total = snapshot.structural_total;
                    checkpoint = None;
                    checkpoint_verified = false;
                    sampled_window_rows_inspected = 0;
                    sample_finding_seen = true;
                } else {
                    structural_total =
                        structural_counts.iter().try_fold(1u64, |total, count| {
                            total.checked_add(*count).ok_or_else(limit_exceeded)
                        })?;
                    checkpoint = Some(active);
                    checkpoint_verified = true;
                }
            }
            Err(reason) => {
                // Fail-closed → full validation; count and expose the reason so
                // lost fast paths (including migration-driven fingerprint
                // mismatches) are visible, never silent.
                self.shared.note_checkpoint_ignored(reason);
                checkpoint_ignored_reason = Some(reason);
            }
        }
        Ok(Self::Session {
            shared: Arc::clone(&self.shared),
            lease: Some(lease),
            durable_commit_epoch,
            database_id,
            open_session_id,
            retained_metadata: snapshot.retained_metadata,
            inputs,
            structural_counts,
            full_structural_counts,
            structural_total,
            next_structural: StructuralEvidenceCursor::start(database_id, open_session_id),
            next_historical: HistoricalEvidenceCursor::start(database_id, open_session_id),
            last_historical_key: None,
            structural_finished: false,
            historical_finished: false,
            authoritative_finding_seen: false,
            any_finding_seen: sample_finding_seen,
            saw_v1_index: false,
            structural_read: Some(transaction),
            structural_cursors: None,
            entity_chains: None,
            pending_entity_orphan_findings: VecDeque::new(),
            historical_plan: None,
            checkpoint,
            sampled_window_rows_inspected,
            checkpoint_verified,
            checkpoint_ignored_reason,
            walked_suffix_counts: [0; STRUCTURAL_TABLE_COUNT],
            walked_prefix_counts: [0; STRUCTURAL_TABLE_COUNT],
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
            // Orphan findings must already have been drained on the last
            // advancing page — never emit a non-advancing page here.
            if !self.pending_entity_orphan_findings.is_empty() {
                return Err(invariant());
            }
            self.verify_checkpoint_prefix_counts()?;
            self.structural_finished = true;
            self.structural_cursors = None;
            self.entity_chains = None;
            self.structural_read = None;
            return Ok(StructuralEvidencePage::ExactEnd(
                RedbStructuralEvidenceEnd { cursor },
            ));
        }

        let page_cap = riffdb_storage_api::MAX_INTEGRITY_FINDINGS;
        let finding_bound = u64::try_from(page_cap).map_err(|_| limit_exceeded())?;
        let remaining = self
            .structural_total
            .checked_sub(cursor.position())
            .ok_or_else(invariant)?;
        let mut inspected = remaining.min(u64::from(limit.get())).min(finding_bound);
        // Last advancing page must reserve room for the bounded orphan set so
        // findings never require a non-advancing terminal drain.
        // row_budget is provably > 0 via the compile-time guard above.
        if inspected == remaining {
            let row_budget = finding_bound.saturating_sub(ENTITY_CHAIN_ORPHAN_FINDING_CAP as u64);
            if inspected > row_budget {
                inspected = row_budget;
            }
        }
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
        let finishing = next.position() == self.structural_total;
        if finishing {
            // Drain every remaining orphan on the last advancing page.
            while let Some(finding) = self.pending_entity_orphan_findings.pop_front() {
                if findings.len() >= page_cap {
                    return Err(limit_exceeded());
                }
                if finding.scope() == StructuralFindingScope::Authoritative {
                    self.authoritative_finding_seen = true;
                }
                findings.push(finding);
            }
        } else {
            while findings.len() < page_cap
                && let Some(finding) = self.pending_entity_orphan_findings.pop_front()
            {
                if finding.scope() == StructuralFindingScope::Authoritative {
                    self.authoritative_finding_seen = true;
                }
                findings.push(finding);
            }
        }
        if !findings.is_empty() {
            self.any_finding_seen = true;
        }
        let page =
            StructuralEvidencePage::page(cursor, findings, next).map_err(value_error_as_storage)?;
        self.next_structural = next;
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
        let requested = usize::try_from(limit.get()).map_err(|_| limit_exceeded())?;
        {
            let plan = self.historical_plan.as_ref().ok_or_else(invariant)?;
            if plan.next_index >= plan.entries.len() {
                self.historical_finished = true;
                return Ok(HistoricalEvidencePage::ExactEnd(
                    RedbHistoricalEvidenceEnd { cursor },
                ));
            }
        }
        let transaction = self.open_snapshot_read()?;
        let mut evidence = Vec::new();
        let mut bytes = 0usize;
        let mut migration_rows = 0usize;
        let mut migration_evidence_bytes = 0usize;
        let mut migration_instruction_bytes = 0usize;
        let mut last_key = self.last_historical_key.clone();
        let plan = self.historical_plan.as_mut().ok_or_else(invariant)?;
        while evidence.len() < requested && plan.next_index < plan.entries.len() {
            let order_key = plan.entries[plan.next_index].0.clone();
            let item =
                materialize_historical_evidence(&transaction, &plan.entries[plan.next_index].1)?;
            let next_bytes = bytes
                .checked_add(historical_semantic_bytes(&item)?)
                .ok_or_else(limit_exceeded)?;
            if next_bytes > riffdb_storage_api::MAX_HISTORICAL_EVIDENCE_PAGE_BYTES {
                if evidence.is_empty() {
                    return Err(limit_exceeded());
                }
                break;
            }
            if let HistoricalSemanticEvidence::IndexMigrationRow(row) = &item {
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
            // ADR-0019 A1 write gate: the checkpoint is written ONLY when this
            // validation session produced ZERO findings of ANY scope. A finding
            // of any severity (authoritative, outbox, projection, sampled)
            // vetoes the write so the fast path can never silence it.
            if !self.any_finding_seen {
                self.shared.set_startup_validation_clean(true);
                let retained = self.retained_metadata.clone();
                if crate::validated_prefix::write_validated_prefix_checkpoint(
                    &self.shared,
                    &retained,
                )
                .is_err()
                {
                    // ADR-0019 A1: a failed checkpoint write after clean
                    // validation is non-fatal — it costs only the next open's
                    // fast path. Counted for operator visibility.
                    self.shared.note_checkpoint_write_failure();
                }
            }
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
        self.verify_structural_continuity(&transaction)?;
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
        self.verify_structural_continuity(transaction)
    }

    fn verify_structural_continuity(
        &self,
        transaction: &ReadTransaction,
    ) -> Result<(), StorageError> {
        self.verify_structural_counts(transaction)?;
        let metadata = read_retained_metadata(transaction)?;
        if metadata != self.retained_metadata {
            return Err(corrupt());
        }
        Ok(())
    }

    fn verify_structural_counts(&self, transaction: &ReadTransaction) -> Result<(), StorageError> {
        let meta = transaction.open_table(META).map_err(table_error)?;
        let mut counts = [0u64; STRUCTURAL_TABLE_COUNT];
        counts[0] = meta.len().map_err(precommit_storage_error)?;
        drop(meta);
        // Length-annotated so a table added to STRUCTURAL_TABLE_COUNT without a
        // row here is a compile error, not a silent zero (RT-A/RT-B lesson).
        let definitions: [TableDefinition<'static, &'static [u8], &'static [u8]>;
            STRUCTURAL_TABLE_COUNT - 1] = [
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
            EVENT_ROUTES,
            OUTBOX,
            OUTBOX_STATUS,
            PROJECTION_STATE,
            PROJECTION_FRONTIER,
            PROJECTION_APPLIED,
            CAPABILITIES,
            CAPABILITY_TOKENS,
            AUDIT,
            AUDIT_BY_REQUEST,
            CONTRACT_MIGRATION_JOURNAL,
            CONTRACT_MIGRATIONS,
            CONTRACT_WRITE_RETIREMENTS,
            RETIRED_ENTITIES,
            HISTORY_TOMBSTONES,
        ];
        for (index, definition) in definitions.into_iter().enumerate() {
            counts[index + 1] = table_len(transaction, definition)?;
        }
        if counts != self.full_structural_counts {
            return Err(corrupt());
        }
        if self.checkpoint.is_none() && counts != self.structural_counts {
            return Err(corrupt());
        }
        Ok(())
    }

    /// Verifies ALL EIGHT recorded below-S row counts at structural ExactEnd.
    ///
    /// Range-skipped phases (COMMITS/EVENTS/OUTBOX/OUTBOX_STATUS/AUDIT) check
    /// `full_len − walked_suffix == recorded_prefix`. The non-sequence-prefixed
    /// tables (EVENT_ROUTES/IDEMPOTENCY/AUDIT_BY_REQUEST) are fully walked with
    /// a counting skip; their classified prefix rows must equal the recorded
    /// counts exactly. Per ADR-0019 A1, a divergence after verified bindings is
    /// authoritative corruption: the recorded counts describe an immutable
    /// prefix, so the open REFUSES — exactly as full validation refuses on
    /// authoritative findings — instead of falling back.
    fn verify_checkpoint_prefix_counts(&self) -> Result<(), StorageError> {
        let Some(checkpoint) = self.checkpoint.as_ref() else {
            return Ok(());
        };
        let c = checkpoint.counts;
        let range_skipped = [
            (10, c.commits_count),
            (12, c.events_count),
            (14, c.outbox_count),
            (15, c.outbox_status_count),
            (21, c.audit_count),
        ];
        for (phase, recorded_prefix) in range_skipped {
            let full = self.full_structural_counts[phase];
            let walked = self.walked_suffix_counts[phase];
            if self.structural_counts[phase] != walked {
                return Err(corrupt());
            }
            if full.saturating_sub(walked) != recorded_prefix {
                return Err(corrupt());
            }
        }
        let counted_skip_walks = [
            (13, c.event_routes_count),
            (8, c.idempotency_count),
            (22, c.audit_by_request_count),
        ];
        for (phase, recorded_prefix) in counted_skip_walks {
            if self.walked_prefix_counts[phase] != recorded_prefix {
                return Err(corrupt());
            }
        }
        Ok(())
    }

    /// Test/benchmark observability: whether a verified checkpoint drove this session.
    #[doc(hidden)]
    #[must_use]
    pub fn checkpoint_verified(&self) -> bool {
        self.checkpoint_verified
    }

    /// Test observability: rows actually inspected by deterministic sample windows.
    #[doc(hidden)]
    #[must_use]
    pub fn sampled_window_rows_inspected(&self) -> u64 {
        self.sampled_window_rows_inspected
    }

    /// Test/operator observability: why a present checkpoint was ignored, if it was.
    #[doc(hidden)]
    #[must_use]
    pub fn checkpoint_ignored_reason(&self) -> Option<&'static str> {
        self.checkpoint_ignored_reason
            .map(crate::validated_prefix::CheckpointIgnoreReason::as_str)
    }

    /// Benchmark observability: COMMITS rows actually walked after the checkpoint
    /// bound S (0 on the full-validation path or an empty suffix).
    #[doc(hidden)]
    #[must_use]
    pub fn checkpoint_suffix_commits(&self) -> u64 {
        self.walked_suffix_counts[10]
    }
}

/// Runs existing inspect functions over checkpoint-derived sample windows below S.
///
/// Sequence-keyed windows (COMMITS + EVENTS) sample `[1, S]`; the audit class
/// samples `[1, audit_bound]` over AUDIT with the same self-hash derivation, so
/// audit-shaped histories (S = 0, bound > 0) still get nonzero below-bound
/// sampling. Sampled AUDIT service rows cross-check their AUDIT_BY_REQUEST peer
/// by key inside `inspect_audit_row`. Budget: `SAMPLE_WINDOW_COUNT` windows of
/// `SAMPLE_WINDOW_SIZE` rows per table class (≤ 1% of a 10M-row history).
///
/// Returns `(rows_inspected, optional_first_finding)`. Same checkpoint hash ⇒ same windows.
fn run_checkpoint_sample_windows(
    transaction: &ReadTransaction,
    checkpoint_hash: &[u8; 32],
    s: u64,
    audit_bound: u64,
) -> Result<(u64, Option<StructuralFinding>), StorageError> {
    use std::ops::Bound::{Included, Unbounded};

    let mut inspected = 0_u64;
    if s > 0 {
        let starts = crate::validated_prefix::sample_window_starts(checkpoint_hash, s);
        for start in starts {
            if start == 0 || start > s {
                continue;
            }
            let end_seq = start
                .saturating_add(crate::validated_prefix::SAMPLE_WINDOW_SIZE)
                .min(s.saturating_add(1));
            let Some(lower_seq) = CommitSequence::new(start) else {
                continue;
            };
            let lower_key = keys::encode_application_sequence_key(lower_seq);
            let commits = transaction.open_table(COMMITS).map_err(table_error)?;
            for entry in commits
                .range::<&[u8]>((Included(lower_key.as_slice()), Unbounded))
                .map_err(precommit_storage_error)?
            {
                let (key, value) = entry.map_err(precommit_storage_error)?;
                let Ok(seq) = keys::decode_application_sequence_key(key.value()) else {
                    continue;
                };
                if seq.get() >= end_seq || seq.get() > s {
                    break;
                }
                inspected = inspected.saturating_add(1);
                let index = seq.get().saturating_sub(1);
                if let Some(finding) =
                    inspect_commit_row(transaction, index, key.value(), value.value())?
                {
                    return Ok((inspected, Some(finding)));
                }
            }
            drop(commits);

            let lower_event = keys::encode_event_key(riffdb_types::EventId::new(lower_seq, 0));
            let events = transaction.open_table(EVENTS).map_err(table_error)?;
            for entry in events
                .range::<&[u8]>((Included(lower_event.as_slice()), Unbounded))
                .map_err(precommit_storage_error)?
            {
                let (key, value) = entry.map_err(precommit_storage_error)?;
                let Ok(id) = keys::decode_event_key(key.value()) else {
                    continue;
                };
                if id.commit_sequence().get() >= end_seq || id.commit_sequence().get() > s {
                    break;
                }
                inspected = inspected.saturating_add(1);
                if let Some(finding) = inspect_event_row(transaction, key.value(), value.value())? {
                    return Ok((inspected, Some(finding)));
                }
            }
        }
    }
    if audit_bound > 0 {
        let starts = crate::validated_prefix::sample_window_starts(checkpoint_hash, audit_bound);
        for start in starts {
            if start == 0 || start > audit_bound {
                continue;
            }
            let end_seq = start
                .saturating_add(crate::validated_prefix::SAMPLE_WINDOW_SIZE)
                .min(audit_bound.saturating_add(1));
            let Some(lower_seq) = riffdb_types::AdministrationSequence::new(start) else {
                continue;
            };
            let lower_key = keys::encode_audit_key(lower_seq);
            let audit = transaction.open_table(AUDIT).map_err(table_error)?;
            for entry in audit
                .range::<&[u8]>((Included(lower_key.as_slice()), Unbounded))
                .map_err(precommit_storage_error)?
            {
                let (key, value) = entry.map_err(precommit_storage_error)?;
                let Ok(seq) = keys::decode_audit_key(key.value()) else {
                    continue;
                };
                if seq.get() >= end_seq || seq.get() > audit_bound {
                    break;
                }
                inspected = inspected.saturating_add(1);
                let index = seq.get().saturating_sub(1);
                if let Some(finding) =
                    inspect_audit_row(transaction, index, key.value(), value.value())?
                {
                    return Ok((inspected, Some(finding)));
                }
            }
        }
    }
    Ok((inspected, None))
}

impl RedbStructuralEvidenceSession {
    fn open_structural_phase_range(
        &self,
        phase: usize,
        table: ReadOnlyTable<&'static [u8], &'static [u8]>,
    ) -> Result<Range<'static, &'static [u8], &'static [u8]>, StorageError> {
        let Some(checkpoint) = self.checkpoint.as_ref() else {
            return table.range::<&[u8]>(..).map_err(precommit_storage_error);
        };
        let s = checkpoint.checkpoint_commit_sequence;
        let audit_bound = checkpoint.audit_sequence_bound;
        match phase {
            10 if s > 0 => {
                let key =
                    keys::encode_application_sequence_key(CommitSequence::new(s).expect("s > 0"));
                table
                    .range::<&[u8]>((Excluded(key.as_slice()), Unbounded))
                    .map_err(precommit_storage_error)
            }
            12 | 14 | 15 if s > 0 => {
                if let Some(lower) = crate::validated_prefix::first_event_key_after(s) {
                    table
                        .range::<&[u8]>((Included(lower.as_slice()), Unbounded))
                        .map_err(precommit_storage_error)
                } else {
                    table
                        .range::<&[u8]>((Excluded(&[0xff_u8; 12][..]), Unbounded))
                        .map_err(precommit_storage_error)
                }
            }
            21 if audit_bound > 0 => {
                let key = keys::encode_audit_key(
                    riffdb_types::AdministrationSequence::new(audit_bound).expect("bound > 0"),
                );
                table
                    .range::<&[u8]>((Excluded(key.as_slice()), Unbounded))
                    .map_err(precommit_storage_error)
            }
            _ => table.range::<&[u8]>(..).map_err(precommit_storage_error),
        }
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
        if phase == 0 {
            let key = std::str::from_utf8(&key).map_err(|_| corrupt())?;
            return Ok(inspect_meta_row(key, &value, self.database_id));
        }
        if phase == 5 {
            return self.inspect_entity_row_with_chains(&key, &value);
        }
        if let Some(checkpoint) = self.checkpoint.as_ref()
            && crate::validated_prefix::skip_inspect_for_prefix_row(
                phase,
                &key,
                &value,
                checkpoint.checkpoint_commit_sequence,
                checkpoint.audit_sequence_bound,
            )
        {
            // Counting skip-walk: every classified prefix row is tallied and
            // verified against the recorded count at ExactEnd, so a vanished
            // below-S row in these full-walk tables still fails closed.
            self.walked_prefix_counts[phase] = self.walked_prefix_counts[phase].saturating_add(1);
            return Ok(None);
        }
        let inspect_index = match (phase, self.checkpoint.as_ref()) {
            (10, Some(cp)) => index.saturating_add(cp.counts.commits_count),
            (21, Some(cp)) => index.saturating_add(cp.counts.audit_count),
            _ => index,
        };
        if crate::validated_prefix::is_range_skipped_phase(phase) {
            self.walked_suffix_counts[phase] = self.walked_suffix_counts[phase].saturating_add(1);
        }
        let transaction = self.structural_read.as_ref().ok_or_else(invariant)?;
        inspect_table_row_from_bytes(
            transaction,
            &self.inputs,
            self.database_id,
            phase,
            inspect_index,
            &key,
            &value,
        )
    }

    fn ensure_entity_chains_built(&mut self) -> Result<(), StorageError> {
        if self.entity_chains.is_some() {
            return Ok(());
        }
        let entity_count = self.full_structural_counts.get(5).copied().unwrap_or(0);
        // Under a verified checkpoint the genesis COMMITS walk is replaced by
        // seeding from the fingerprint-verified (target, version) map at S and
        // advancing across the suffix only. The seed map is consumed exactly
        // once; a second build attempt under a checkpoint is an invariant error.
        let seed = match self.checkpoint.as_mut() {
            Some(checkpoint) => Some((
                checkpoint.checkpoint_commit_sequence,
                checkpoint.entities_at_s.take().ok_or_else(invariant)?,
            )),
            None => None,
        };
        let transaction = self.structural_read.as_ref().ok_or_else(invariant)?;
        self.entity_chains = Some(build_entity_chains(transaction, entity_count, seed)?);
        Ok(())
    }

    fn queue_entity_orphan_findings(&mut self) {
        let Some(state) = self.entity_chains.as_mut() else {
            return;
        };
        if state.orphans_queued {
            return;
        }
        state.orphans_queued = true;
        // Detail findings are bounded and carry no identity (StructuralFinding
        // has only scope+code). Emit at most CAP detail copies, plus one
        // aggregate when the detail list is truncated — either unconsumed+
        // reported-orphans exceed CAP, or the overflow list itself filled and
        // more map-capacity rejects were dropped (NEW-8: not on bare overflow
        // with a short detail list). Never mass-fail intact entity rows.
        let unconsumed = state.chains.values().filter(|c| !c.consumed).count();
        let detail_total = unconsumed.saturating_add(state.orphan_targets.len());
        let detail_emitted = detail_total.min(ENTITY_CHAIN_ORPHAN_REPORT_CAP);
        for _ in 0..detail_emitted {
            self.pending_entity_orphan_findings
                .push_back(authoritative(StructuralFindingCode::CrossLinkMismatch));
        }
        let detail_truncated = detail_total > ENTITY_CHAIN_ORPHAN_REPORT_CAP;
        let overflow_list_truncated =
            state.overflow && state.orphan_targets.len() >= ENTITY_CHAIN_ORPHAN_REPORT_CAP;
        if detail_truncated || overflow_list_truncated {
            self.pending_entity_orphan_findings
                .push_back(authoritative(StructuralFindingCode::CrossLinkMismatch));
        }
    }

    /// Marks the chain for one ENTITIES row as consumed (O(log N) map lookup).
    fn mark_entity_row_seen(&mut self, key: &riffdb_types::EntityKey) {
        let Some(state) = self.entity_chains.as_mut() else {
            return;
        };
        mark_entity_chain_consumed(&mut state.chains, key);
    }

    fn inspect_entity_row_with_chains(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<StructuralFinding>, StorageError> {
        self.ensure_entity_chains_built()?;
        let Ok(decoded_key) = keys::decode_entity_key(key) else {
            // Raw key undecodable: cannot reconstruct a target (ledgered).
            return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
        };
        // Mark before any semantic early-return (NEW-2), O(log N) (NEW-6).
        self.mark_entity_row_seen(&decoded_key);

        let record = match decoded(codec::decode_entity_record_v1(value)) {
            Ok(value) => value,
            Err(code) => {
                self.maybe_queue_orphans_after_entity_row();
                return Ok(Some(authoritative(code)));
            }
        };
        // Also mark by decoded target (handles key/target mismatch rows).
        if let Some(state) = self.entity_chains.as_mut()
            && let Some(chain) = state.chains.get_mut(record.target())
        {
            chain.consumed = true;
        }
        if record.target().key() != &decoded_key {
            self.maybe_queue_orphans_after_entity_row();
            return Ok(Some(authoritative(
                StructuralFindingCode::CrossLinkMismatch,
            )));
        }
        let transaction = self.structural_read.as_ref().ok_or_else(invariant)?;
        if !binding_bundle_exists(transaction, record.schema_binding())? {
            self.maybe_queue_orphans_after_entity_row();
            return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
        }
        // Per-entity verdict only — overflow/orphans do not mask intact rows.
        let history_ok = {
            let state = self.entity_chains.as_mut().ok_or_else(invariant)?;
            let migrations = &state.migrations;
            match state.chains.get_mut(record.target()) {
                Some(chain) => {
                    while chain.intact && chain.migration_cursor < migrations.len() {
                        match advance_entity_chain_through_migration(
                            transaction,
                            migrations,
                            record.target(),
                            chain,
                            None,
                        )? {
                            MigrationAdvance::Advanced | MigrationAdvance::Skipped => {}
                            MigrationAdvance::Invalid => chain.intact = false,
                            MigrationAdvance::Exhausted => break,
                        }
                    }
                    entity_row_matches_chain(chain, &record)
                }
                // No retained commit references this target. Under a pruned
                // floor its whole history is tombstone-covered: the row
                // validates by fingerprint-of-current shape (decode + binding
                // cross-links above), not by replay (ADR-0085 A2). Unpruned,
                // an unreferenced ENTITIES row remains corruption.
                None => state.pruned_floor > 0,
            }
        };
        self.maybe_queue_orphans_after_entity_row();
        Ok((!history_ok).then(|| authoritative(StructuralFindingCode::MissingCrossLink)))
    }

    fn maybe_queue_orphans_after_entity_row(&mut self) {
        let entity_count = self.structural_counts.get(5).copied().unwrap_or(0);
        let consumed_in_phase = self
            .structural_cursors
            .as_ref()
            .map(|c| c.consumed_in_phase)
            .unwrap_or(0);
        if entity_count > 0 && consumed_in_phase == entity_count {
            self.queue_entity_orphan_findings();
        }
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
                // ENTITIES phase entry: build chains even when the table is empty so
                // commit-referenced orphans are still validated.
                if phase == 5 {
                    self.ensure_entity_chains_built()?;
                }
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
                    13 => transaction.open_table(EVENT_ROUTES).map_err(table_error)?,
                    14 => transaction.open_table(OUTBOX).map_err(table_error)?,
                    15 => transaction.open_table(OUTBOX_STATUS).map_err(table_error)?,
                    16 => transaction
                        .open_table(PROJECTION_STATE)
                        .map_err(table_error)?,
                    17 => transaction
                        .open_table(PROJECTION_FRONTIER)
                        .map_err(table_error)?,
                    18 => transaction
                        .open_table(PROJECTION_APPLIED)
                        .map_err(table_error)?,
                    19 => transaction.open_table(CAPABILITIES).map_err(table_error)?,
                    20 => transaction
                        .open_table(CAPABILITY_TOKENS)
                        .map_err(table_error)?,
                    21 => transaction.open_table(AUDIT).map_err(table_error)?,
                    22 => transaction
                        .open_table(AUDIT_BY_REQUEST)
                        .map_err(table_error)?,
                    23 => transaction
                        .open_table(CONTRACT_MIGRATION_JOURNAL)
                        .map_err(table_error)?,
                    24 => transaction
                        .open_table(CONTRACT_MIGRATIONS)
                        .map_err(table_error)?,
                    25 => transaction
                        .open_table(CONTRACT_WRITE_RETIREMENTS)
                        .map_err(table_error)?,
                    26 => transaction
                        .open_table(RETIRED_ENTITIES)
                        .map_err(table_error)?,
                    27 => transaction
                        .open_table(HISTORY_TOMBSTONES)
                        .map_err(table_error)?,
                    _ => return Err(invariant()),
                };
                let range = self.open_structural_phase_range(phase, table)?;
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
            let leaving = cursors.phase;
            cursors.phase = cursors.phase.checked_add(1).ok_or_else(invariant)?;
            cursors.consumed_in_phase = 0;
            // Phase 5 is ENTITIES: queue unconsumed/orphan findings, then drop the map.
            if leaving == 5 {
                self.queue_entity_orphan_findings();
                self.entity_chains = None;
            }
        }
    }
}

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
        // Phase 5 (ENTITIES) is handled exclusively by
        // `inspect_entity_row_with_chains` in `inspect_structural_forward`.
        6 => inspect_index_row(transaction, key, value),
        7 => inspect_epoch_row(transaction, key, value),
        8 => inspect_terminal_row(transaction, inputs, database_id, key, value),
        9 => inspect_pending_row(transaction, inputs, database_id, key, value),
        10 => inspect_commit_row(transaction, index, key, value),
        11 => inspect_provenance_row(transaction, key, value),
        12 => inspect_event_row(transaction, key, value),
        13 => inspect_event_route_row(transaction, key, value),
        14 => inspect_outbox_row(transaction, key, value),
        15 => inspect_outbox_status_row(transaction, key, value),
        16 => inspect_projection_state_row(transaction, key, value),
        17 => inspect_projection_control_row(transaction, key, value),
        18 => inspect_projection_apply_row(transaction, key, value),
        19 => inspect_capability_row(transaction, inputs, database_id, key, value),
        20 => inspect_capability_lookup_row(transaction, key, value),
        21 => inspect_audit_row(transaction, index, key, value),
        22 => inspect_audit_by_request_row(transaction, key, value),
        23 => inspect_contract_migration_journal_row(key, value),
        24 => inspect_contract_migration_record_row(key, value),
        25 => inspect_contract_write_retirement_row(key, value),
        26 => inspect_retired_entity_row(transaction, key, value),
        27 => inspect_history_tombstone_row(key, value),
        _ => Err(invariant()),
    }
}

fn inspect_history_tombstone_row(
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let first = keys::decode_application_sequence_key(key).map_err(|_| corrupt())?;
    let decoded = riffdb_storage_api::proto_codec::decode_history_tombstone_v1(value)
        .map_err(crate::error::codec_error)?;
    let tombstone = decoded.value();
    if tombstone.first_sequence() != first.get() {
        return Err(corrupt());
    }
    Ok(None)
}

fn inspect_contract_migration_journal_row(
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let operation = keys::decode_contract_migration_operation_key(key).map_err(|_| corrupt())?;
    let decoded = riffdb_storage_api::proto_codec::decode_contract_migration_journal_v1(value)
        .map_err(crate::error::codec_error)?;
    if decoded.value().operation_id() != operation {
        return Err(corrupt());
    }
    Ok(None)
}

fn inspect_contract_migration_record_row(
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let operation = keys::decode_contract_migration_operation_key(key).map_err(|_| corrupt())?;
    let decoded = riffdb_storage_api::proto_codec::decode_contract_migration_record_v1(value)
        .map_err(crate::error::codec_error)?;
    if decoded.value().operation_id() != operation {
        return Err(corrupt());
    }
    Ok(None)
}

fn inspect_contract_write_retirement_row(
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let parent = keys::decode_contract_write_retirement_key(key).map_err(|_| corrupt())?;
    let decoded = riffdb_storage_api::proto_codec::decode_contract_write_retirement_v1(value)
        .map_err(crate::error::codec_error)?;
    if decoded.value().artifacts().parent() != parent {
        return Err(corrupt());
    }
    Ok(None)
}

fn inspect_retired_entity_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let (operation, target) = keys::decode_retired_entity_key(key).map_err(|_| corrupt())?;
    let decoded = riffdb_storage_api::proto_codec::decode_retired_entity_record_v1(value)
        .map_err(crate::error::codec_error)?;
    if decoded.value().operation_id() != operation || decoded.value().original_target() != &target {
        return Err(corrupt());
    }
    let migrations = transaction
        .open_table(CONTRACT_MIGRATIONS)
        .map_err(table_error)?;
    let operation_key = keys::encode_contract_migration_operation_key(operation);
    let Some(migration) = migrations
        .get(operation_key.as_slice())
        .map_err(precommit_storage_error)?
    else {
        return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
    };
    let migration =
        riffdb_storage_api::proto_codec::decode_contract_migration_record_v1(migration.value())
            .map_err(crate::error::codec_error)?;
    if migration.value().operation_id() != operation
        || migration.value().artifacts().migration() != decoded.value().migration()
    {
        return Ok(Some(authoritative(
            StructuralFindingCode::CrossLinkMismatch,
        )));
    }
    Ok(None)
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
        table_len(transaction, EVENT_ROUTES)?,
        table_len(transaction, OUTBOX)?,
        table_len(transaction, OUTBOX_STATUS)?,
        table_len(transaction, PROJECTION_STATE)?,
        table_len(transaction, PROJECTION_FRONTIER)?,
        table_len(transaction, PROJECTION_APPLIED)?,
        table_len(transaction, CAPABILITIES)?,
        table_len(transaction, CAPABILITY_TOKENS)?,
        table_len(transaction, AUDIT)?,
        table_len(transaction, AUDIT_BY_REQUEST)?,
        table_len(transaction, CONTRACT_MIGRATION_JOURNAL)?,
        table_len(transaction, CONTRACT_MIGRATIONS)?,
        table_len(transaction, CONTRACT_WRITE_RETIREMENTS)?,
        table_len(transaction, RETIRED_ENTITIES)?,
        table_len(transaction, HISTORY_TOMBSTONES)?,
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

pub(crate) fn read_retained_metadata_pub(
    transaction: &ReadTransaction,
) -> Result<RetainedMetadataV1, StorageError> {
    read_retained_metadata(transaction)
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
        // Optional checkpoint: invalid payloads must not produce structural findings.
        META_VALIDATED_PREFIX_CHECKPOINT => true,
        // Optional retention meta: full verify happens in the retention module.
        META_RETENTION_WATERMARK => true,
        META_RETENTION_HOLDS => true,
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
    // After offline prune, retained commits start at watermark+1. The table
    // ordinal `index` is relative to the retained prefix, not absolute history.
    let watermark = retention_watermark_sequence(transaction)?;
    let expected = watermark
        .checked_add(index)
        .and_then(|base| base.checked_add(1))
        .and_then(CommitSequence::new);
    if expected != Some(sequence) || record.commit_sequence() != sequence {
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
    let route_matches = if let Some(commit) = get_commit(transaction, id.commit_sequence())? {
        let route_key = keys::encode_event_route_key(commit.partition_hash(), id);
        matches!(
            get_decoded(
                transaction,
                EVENT_ROUTES,
                &route_key,
                codec::decode_event_route_v1,
            )?,
            Ok(Some(route))
                if route.event_id() == id
                    && route.event_type_id() == event.event_type_id()
                    && route.event_hash() == event.event_hash()
        )
    } else {
        false
    };
    Ok((event.event_id() != id
        || !event_graph_is_reciprocal(transaction, &event)?
        || !route_matches)
        .then(|| authoritative(StructuralFindingCode::CrossLinkMismatch)))
}

fn inspect_event_route_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok((partition_hash, id)) = keys::decode_event_route_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let route = match decoded(codec::decode_event_route_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    // Routes are retained under prune; event/commit bodies below the watermark
    // are intentionally absent and covered by the verified tombstone chain.
    let watermark = retention_watermark_sequence(transaction)?;
    if crate::retention::sequence_covered_by_watermark(id.commit_sequence().get(), watermark) {
        if route.event_id() != id {
            return Ok(Some(authoritative(
                StructuralFindingCode::CrossLinkMismatch,
            )));
        }
        return Ok(None);
    }
    let Some(event) = get_event(transaction, id)? else {
        return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
    };
    let Some(commit) = get_commit(transaction, id.commit_sequence())? else {
        return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
    };
    let ordinal = usize::try_from(id.event_ordinal()).map_err(|_| limit_exceeded())?;
    Ok((route.event_id() != id
        || route.event_type_id() != event.event_type_id()
        || route.event_hash() != event.event_hash()
        || commit.partition_hash() != partition_hash
        || commit.events().get(ordinal) != Some(&event))
    .then(|| authoritative(StructuralFindingCode::CrossLinkMismatch)))
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
            // Index existence is checked first so a Service row without a durable
            // AUDIT_BY_REQUEST peer reports MissingCrossLink (reciprocity), while
            // lifecycle validation uses the same index for O(k) per request
            // (k ≤ 2 under the fused-audit write gate) instead of a full AUDIT scan.
            if !service_audit_request_index_exists(transaction, record.request_id(), sequence)? {
                Some(authoritative(StructuralFindingCode::MissingCrossLink))
            } else if !service_lifecycle_is_reciprocal(transaction, record)? {
                Some(authoritative(StructuralFindingCode::CrossLinkMismatch))
            } else {
                None
            }
        }
        // Retention administration records (projection detach/reattach) have
        // no cross-linked peer row: the hold list is a live snapshot, not a
        // per-record reciprocal, so the codec's semantic decode is the check.
        riffdb_storage_api::StoredAdministrationAuditRecordV1::Retention(_) => None,
    };
    Ok(finding)
}

fn service_audit_request_index_exists(
    transaction: &ReadTransaction,
    request_id: riffdb_types::RequestId,
    sequence: riffdb_types::AdministrationSequence,
) -> Result<bool, StorageError> {
    let table = transaction
        .open_table(AUDIT_BY_REQUEST)
        .map_err(table_error)?;
    let key = keys::encode_audit_by_request_key(request_id, sequence);
    let Some(value) = table.get(key.as_slice()).map_err(precommit_storage_error)? else {
        return Ok(false);
    };
    let index = match decoded(codec::decode_service_audit_request_index_v1(value.value())) {
        Ok(value) => value,
        Err(_) => return Ok(false),
    };
    Ok(index.request_id() == request_id && index.administration_sequence() == sequence)
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
    let mut ordered: std::collections::BTreeMap<Vec<u8>, EvidenceLocator> =
        std::collections::BTreeMap::new();
    let mut index_bytes = 0usize;
    let mut insert = |order_key: Vec<u8>, locator: EvidenceLocator| -> Result<(), StorageError> {
        if ordered.contains_key(&order_key) {
            return Ok(());
        }
        let charge = order_key
            .len()
            .checked_add(locator.retained_bytes())
            .ok_or_else(limit_exceeded)?;
        index_bytes = index_bytes.checked_add(charge).ok_or_else(limit_exceeded)?;
        if index_bytes > MAX_STARTUP_EVIDENCE_INDEX_BYTES {
            return Err(limit_exceeded());
        }
        ordered.insert(order_key, locator);
        Ok(())
    };

    collect_bundle_locators(transaction, &mut insert)?;
    collect_contract_migration_edge_locators(transaction, &mut insert)?;
    collect_plan_locators(transaction, &mut insert)?;
    collect_active_catalog_locator(transaction, &mut insert)?;
    collect_persisted_key_locators(transaction, &mut insert)?;
    collect_capability_partition_locators(transaction, inputs, &mut insert)?;

    Ok(HistoricalEvidencePlan {
        entries: ordered.into_iter().collect(),
        next_index: 0,
    })
}

fn collect_contract_migration_edge_locators(
    transaction: &ReadTransaction,
    insert: &mut dyn FnMut(Vec<u8>, EvidenceLocator) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let retirements = transaction
        .open_table(CONTRACT_WRITE_RETIREMENTS)
        .map_err(table_error)?;
    for entry in retirements.iter().map_err(precommit_storage_error)? {
        let (key, _) = entry.map_err(precommit_storage_error)?;
        let predecessor =
            keys::decode_contract_write_retirement_key(key.value()).map_err(|_| corrupt())?;
        let edge = read_contract_migration_edge(transaction, predecessor)?.ok_or_else(corrupt)?;
        let evidence = HistoricalSemanticEvidence::ContractMigrationEdge(Box::new(edge));
        insert(
            historical_order_key(&evidence),
            EvidenceLocator::ContractMigrationEdge(predecessor),
        )?;
    }
    Ok(())
}

fn collect_bundle_locators(
    transaction: &ReadTransaction,
    insert: &mut dyn FnMut(Vec<u8>, EvidenceLocator) -> Result<(), StorageError>,
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
        insert(
            historical_order_key(&evidence),
            EvidenceLocator::Bundle(key.value().to_vec()),
        )?;
    }
    Ok(())
}

fn collect_plan_locators(
    transaction: &ReadTransaction,
    insert: &mut dyn FnMut(Vec<u8>, EvidenceLocator) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let mut push_plan = |plan: riffdb_storage_api::ExecutablePlanRef| -> Result<(), StorageError> {
        let evidence = HistoricalSemanticEvidence::PlanReference(plan.clone());
        insert(
            historical_order_key(&evidence),
            EvidenceLocator::PlanReference(plan),
        )
    };
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
        push_plan(plan.clone())?;
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
        push_plan(record.plan().clone())?;
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
        push_plan(record.plan().clone())?;
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
        push_plan(record.plan().clone())?;
    }
    Ok(())
}

fn collect_active_catalog_locator(
    transaction: &ReadTransaction,
    insert: &mut dyn FnMut(Vec<u8>, EvidenceLocator) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let active = read_active_pointer(transaction)?.map_err(|_| corrupt())?;
    let evidence = HistoricalSemanticEvidence::ActiveCatalog(active.map(|active| {
        HistoricalActiveCatalogEvidence::new(
            active.lineage().clone(),
            active.contract_version(),
            active.bundle_hash(),
        )
    }));
    insert(
        historical_order_key(&evidence),
        EvidenceLocator::ActiveCatalog,
    )
}

fn collect_persisted_key_locators(
    transaction: &ReadTransaction,
    insert: &mut dyn FnMut(Vec<u8>, EvidenceLocator) -> Result<(), StorageError>,
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
        let evidence = HistoricalSemanticEvidence::PersistedKey(
            HistoricalPersistedKeyEvidenceV1::from_entity(&record),
        );
        insert(
            historical_order_key(&evidence),
            EvidenceLocator::PersistedKeyEntity(key.value().to_vec()),
        )?;
    }
    let indexes = transaction
        .open_table(SECONDARY_INDEXES)
        .map_err(table_error)?;
    for entry in indexes.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let physical = keys::decode_index_entry_key(key.value()).map_err(|_| corrupt())?;
        let record = codec::decode_index_migration_row(&physical, value.value())?;
        let evidence = HistoricalSemanticEvidence::IndexMigrationRow(record);
        insert(
            historical_order_key(&evidence),
            EvidenceLocator::IndexMigration(key.value().to_vec()),
        )?;
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
            let evidence = HistoricalSemanticEvidence::PersistedKey(
                HistoricalPersistedKeyEvidenceV1::from_legacy_index_epoch(&record),
            );
            insert(
                historical_order_key(&evidence),
                EvidenceLocator::PersistedKeyEpoch(key.value().to_vec()),
            )?;
        } else {
            let physical = keys::decode_partition_index_key(key.value()).map_err(|_| corrupt())?;
            let record =
                decoded(codec::decode_index_epoch_v1(value.value())).map_err(|_| corrupt())?;
            if record.target() != &physical {
                return Err(corrupt());
            }
            let evidence = HistoricalSemanticEvidence::PersistedKey(
                HistoricalPersistedKeyEvidenceV1::from_index_epoch(&record),
            );
            insert(
                historical_order_key(&evidence),
                EvidenceLocator::PersistedKeyEpoch(key.value().to_vec()),
            )?;
        }
    }
    Ok(())
}

fn collect_capability_partition_locators(
    transaction: &ReadTransaction,
    inputs: &StartupValidationInputs,
    insert: &mut dyn FnMut(Vec<u8>, EvidenceLocator) -> Result<(), StorageError>,
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
            let order_key =
                historical_order_key(&HistoricalSemanticEvidence::CapabilityPartition(evidence));
            insert(
                order_key,
                EvidenceLocator::CapabilityPartition {
                    capability_id: physical,
                    entry_ordinal: ordinal,
                },
            )?;
        }
    }
    Ok(())
}

fn materialize_historical_evidence(
    transaction: &ReadTransaction,
    locator: &EvidenceLocator,
) -> Result<HistoricalSemanticEvidence, StorageError> {
    match locator {
        EvidenceLocator::Bundle(key) => {
            let table = transaction
                .open_table(CONTRACT_BUNDLES)
                .map_err(table_error)?;
            let value = table
                .get(key.as_slice())
                .map_err(precommit_storage_error)?
                .ok_or_else(corrupt)?;
            let (lineage, version) =
                keys::decode_contract_bundle_key(key).map_err(|_| corrupt())?;
            let bundle =
                decoded(codec::decode_contract_bundle_v1(value.value())).map_err(|_| corrupt())?;
            if bundle.lineage() != &lineage
                || bundle.contract_version() != version
                || hash_contract_bundle(bundle.canonical_bytes()) != bundle.bundle_hash()
            {
                return Err(corrupt());
            }
            Ok(HistoricalSemanticEvidence::Bundle(
                HistoricalBundleEvidence::new(
                    lineage,
                    version,
                    bundle.bundle_hash(),
                    HistoricalBundleBytes::new(bundle.canonical_bytes().to_vec())
                        .map_err(value_error_as_storage)?,
                ),
            ))
        }
        EvidenceLocator::ContractMigrationEdge(predecessor) => {
            read_contract_migration_edge(transaction, *predecessor)?
                .map(|edge| HistoricalSemanticEvidence::ContractMigrationEdge(Box::new(edge)))
                .ok_or_else(corrupt)
        }
        EvidenceLocator::PlanReference(plan) => {
            Ok(HistoricalSemanticEvidence::PlanReference(plan.clone()))
        }
        EvidenceLocator::ActiveCatalog => {
            let active = read_active_pointer(transaction)?.map_err(|_| corrupt())?;
            Ok(HistoricalSemanticEvidence::ActiveCatalog(active.map(
                |active| {
                    HistoricalActiveCatalogEvidence::new(
                        active.lineage().clone(),
                        active.contract_version(),
                        active.bundle_hash(),
                    )
                },
            )))
        }
        EvidenceLocator::PersistedKeyEntity(key) => {
            let table = transaction.open_table(ENTITIES).map_err(table_error)?;
            let value = table
                .get(key.as_slice())
                .map_err(precommit_storage_error)?
                .ok_or_else(corrupt)?;
            let physical = keys::decode_entity_key(key).map_err(|_| corrupt())?;
            let record =
                decoded(codec::decode_entity_record_v1(value.value())).map_err(|_| corrupt())?;
            if record.target().key() != &physical {
                return Err(corrupt());
            }
            Ok(HistoricalSemanticEvidence::PersistedKey(
                HistoricalPersistedKeyEvidenceV1::from_entity(&record),
            ))
        }
        EvidenceLocator::IndexMigration(key) => {
            let table = transaction
                .open_table(SECONDARY_INDEXES)
                .map_err(table_error)?;
            let value = table
                .get(key.as_slice())
                .map_err(precommit_storage_error)?
                .ok_or_else(corrupt)?;
            let physical = keys::decode_index_entry_key(key).map_err(|_| corrupt())?;
            let record = codec::decode_index_migration_row(&physical, value.value())?;
            Ok(HistoricalSemanticEvidence::IndexMigrationRow(record))
        }
        EvidenceLocator::PersistedKeyEpoch(key) => {
            let table = transaction.open_table(INDEX_EPOCHS).map_err(table_error)?;
            let value = table
                .get(key.as_slice())
                .map_err(precommit_storage_error)?
                .ok_or_else(corrupt)?;
            if let Ok(record) = decoded(codec::decode_legacy_index_epoch_v1(value.value())) {
                let physical = keys::decode_index_range_prefix_key(key).map_err(|_| corrupt())?;
                if record.target() != &physical {
                    return Err(corrupt());
                }
                Ok(HistoricalSemanticEvidence::PersistedKey(
                    HistoricalPersistedKeyEvidenceV1::from_legacy_index_epoch(&record),
                ))
            } else {
                let physical = keys::decode_partition_index_key(key).map_err(|_| corrupt())?;
                let record =
                    decoded(codec::decode_index_epoch_v1(value.value())).map_err(|_| corrupt())?;
                if record.target() != &physical {
                    return Err(corrupt());
                }
                Ok(HistoricalSemanticEvidence::PersistedKey(
                    HistoricalPersistedKeyEvidenceV1::from_index_epoch(&record),
                ))
            }
        }
        EvidenceLocator::CapabilityPartition {
            capability_id,
            entry_ordinal,
        } => {
            let table = transaction.open_table(CAPABILITIES).map_err(table_error)?;
            let key = keys::encode_capability_key(*capability_id);
            let value = table
                .get(key.as_slice())
                .map_err(precommit_storage_error)?
                .ok_or_else(corrupt)?;
            let capability = decoded(codec::decode_capability_record_v1(value.value()))
                .map_err(|_| corrupt())?;
            if capability.capability_id() != *capability_id {
                return Err(corrupt());
            }
            let evidence = HistoricalCapabilityPartitionEvidenceV1::from_capability_entry(
                &capability,
                *entry_ordinal,
            )
            .map_err(value_error_as_storage)?;
            Ok(HistoricalSemanticEvidence::CapabilityPartition(evidence))
        }
    }
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
        HistoricalSemanticEvidence::ContractMigrationEdge(edge) => {
            key.extend_from_slice(&[0x01, 0xff]);
            let artifacts = edge.retirement().artifacts();
            key.extend_from_slice(artifacts.parent().as_bytes());
            key.extend_from_slice(artifacts.candidate().as_bytes());
            key.extend_from_slice(edge.retirement().operation_id().as_bytes());
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
        HistoricalSemanticEvidence::ContractMigrationEdge(edge) => {
            let migration = riffdb_storage_api::proto_codec::encode_contract_migration_record_v1(
                edge.migration(),
            )
            .map_err(|_| corrupt())?;
            let retirement = riffdb_storage_api::proto_codec::encode_contract_write_retirement_v1(
                edge.retirement(),
            )
            .map_err(|_| corrupt())?;
            migration
                .as_bytes()
                .len()
                .checked_add(retirement.as_bytes().len())
        }
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

fn read_contract_migration_edge(
    transaction: &ReadTransaction,
    predecessor: ContractBundleHash,
) -> Result<Option<riffdb_storage_api::StoredContractMigrationEdgeV1>, StorageError> {
    let retirements = transaction
        .open_table(CONTRACT_WRITE_RETIREMENTS)
        .map_err(table_error)?;
    let retirement_key = keys::encode_contract_write_retirement_key(predecessor);
    let Some(retirement) = retirements
        .get(retirement_key.as_slice())
        .map_err(precommit_storage_error)?
    else {
        return Ok(None);
    };
    let (retirement, _) =
        riffdb_storage_api::proto_codec::decode_contract_write_retirement_v1(retirement.value())
            .map_err(|_| corrupt())?
            .into_parts();
    if retirement.artifacts().parent() != predecessor {
        return Err(corrupt());
    }
    let migrations = transaction
        .open_table(CONTRACT_MIGRATIONS)
        .map_err(table_error)?;
    let operation_key = keys::encode_contract_migration_operation_key(retirement.operation_id());
    let migration = migrations
        .get(operation_key.as_slice())
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    let (migration, _) =
        riffdb_storage_api::proto_codec::decode_contract_migration_record_v1(migration.value())
            .map_err(|_| corrupt())?
            .into_parts();
    riffdb_storage_api::StoredContractMigrationEdgeV1::new(retirement, migration)
        .map(Some)
        .map_err(|_| corrupt())
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
    drop(table);
    let migrations = transaction
        .open_table(CONTRACT_MIGRATIONS)
        .map_err(table_error)?;
    for entry in migrations.iter().map_err(precommit_storage_error)? {
        let (_, value) = entry.map_err(precommit_storage_error)?;
        let Ok(record) =
            riffdb_storage_api::proto_codec::decode_contract_migration_record_v1(value.value())
        else {
            return Ok(false);
        };
        if record.value().artifacts().candidate() == bundle.bundle_hash() {
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
    let mut last_catalog = None;
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
            last_catalog = Some((sequence, record.activated().clone()));
        }
    }
    drop(table);

    let migrations = transaction
        .open_table(CONTRACT_MIGRATIONS)
        .map_err(table_error)?;
    let mut last_migration = None;
    for entry in migrations.iter().map_err(precommit_storage_error)? {
        let (_, value) = entry.map_err(precommit_storage_error)?;
        let Ok(record) =
            riffdb_storage_api::proto_codec::decode_contract_migration_record_v1(value.value())
        else {
            return Ok(false);
        };
        let record = record.value();
        let sequence = record.administration_sequence();
        if last_migration
            .as_ref()
            .is_some_and(|(current, _)| *current == sequence)
        {
            return Ok(false);
        }
        if last_migration
            .as_ref()
            .is_none_or(|(current, _)| sequence > *current)
        {
            last_migration = Some((sequence, record.artifacts().candidate()));
        }
    }

    match (last_catalog, last_migration) {
        (None, None) => Ok(active.is_none()),
        (Some((_, pointer)), None) => Ok(active == Some(&pointer)),
        (None, Some(_)) => Ok(false),
        (Some((catalog_sequence, pointer)), Some((migration_sequence, candidate))) => {
            if catalog_sequence > migration_sequence {
                Ok(active == Some(&pointer))
            } else if migration_sequence > catalog_sequence {
                Ok(active.is_some_and(|pointer| pointer.bundle_hash() == candidate))
            } else {
                Ok(false)
            }
        }
    }
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
    // Below-watermark absence is covered by the verified tombstone chain
    // (ADR-0085 A2); projection source commits in the pruned prefix are not
    // corruption.
    let watermark = retention_watermark_sequence(transaction)?;
    if crate::retention::sequence_covered_by_watermark(sequence.get(), watermark) {
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

/// Live retention watermark sequence (0 when the meta key is absent).
fn retention_watermark_sequence(transaction: &ReadTransaction) -> Result<u64, StorageError> {
    Ok(crate::retention::load_watermark(transaction)?
        .map(|w| w.watermark_sequence())
        .unwrap_or(0))
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
        // Idempotency terminal records are retained across prune; the commit
        // body below the watermark is tombstone-covered absence (ADR-0085
        // A2). The retained provenance record must still reciprocate.
        let watermark = retention_watermark_sequence(transaction)?;
        if crate::retention::sequence_covered_by_watermark(
            outcome.commit_sequence().get(),
            watermark,
        ) {
            return Ok(
                get_provenance(transaction, outcome.provenance_id())?.is_some_and(|provenance| {
                    provenance.commit_sequence() == outcome.commit_sequence()
                        && provenance.identity() == outcome.identity()
                }),
            );
        }
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
    for reference in commit.entity_references() {
        if !current_entity_covers_reference(transaction, reference)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn current_entity_covers_reference(
    transaction: &ReadTransaction,
    reference: &riffdb_storage_api::CommittedEntityReferenceV2,
) -> Result<bool, StorageError> {
    let current = get_decoded(
        transaction,
        ENTITIES,
        keys::encode_entity_key(reference.target().key()),
        codec::decode_entity_record_v1,
    )?;
    Ok(matches!(current, Ok(Some(record))
        if record.target() == reference.target()
            && record.entity_version() >= reference.entity_version()))
}

/// Builds entity continuity chains from one forward COMMITS pass.
///
/// Without a seed the pass covers the complete history from genesis. With a
/// verified-checkpoint seed `(S, entities_at_S)` the chains start from the
/// fingerprint-verified `(target, version)` map at S — carrying no post-image
/// hash and no pending migrations (any migration recorded at write time is
/// already baked into the at-S versions; later migrations fail the fingerprint
/// and fall back to full validation) — and the pass advances across the
/// suffix `(S, head]` only.
fn build_entity_chains(
    transaction: &ReadTransaction,
    entity_count: u64,
    seed: Option<(
        u64,
        std::collections::BTreeMap<riffdb_storage_api::EntityTarget, riffdb_types::EntityVersion>,
    )>,
) -> Result<EntityChainState, StorageError> {
    let migrations = load_entity_migration_evidence(transaction)?;
    // Chains build from the RETAINED commit range only: below-watermark commit
    // bodies are tombstone-covered (verified before this walk began).
    let pruned_floor = retention_watermark_sequence(transaction)?;
    let table = transaction.open_table(COMMITS).map_err(table_error)?;
    let mut chains: std::collections::BTreeMap<riffdb_storage_api::EntityTarget, EntityChain> =
        std::collections::BTreeMap::new();
    let mut orphan_targets = Vec::new();
    let mut overflow = false;
    let entity_count_usize = usize::try_from(entity_count).unwrap_or(usize::MAX);
    let mut walk_lower_bound = None;
    if let Some((s, entities_at_s)) = seed {
        for (target, version) in entities_at_s {
            chains.insert(
                target,
                EntityChain {
                    version,
                    hash: None,
                    expected_bundle: None,
                    migration_cursor: migrations.len(),
                    intact: true,
                    consumed: false,
                    seeded: true,
                },
            );
        }
        if s > 0 {
            let sequence = CommitSequence::new(s).ok_or_else(corrupt)?;
            walk_lower_bound = Some(keys::encode_application_sequence_key(sequence));
        }
    }
    let range = match walk_lower_bound.as_ref() {
        Some(lower) => table
            .range::<&[u8]>((Excluded(lower.as_slice()), Unbounded))
            .map_err(precommit_storage_error)?,
        None => table.range::<&[u8]>(..).map_err(precommit_storage_error)?,
    };
    for entry in range {
        let (physical_key, value) = entry.map_err(precommit_storage_error)?;
        let Ok(commit_sequence) = keys::decode_application_sequence_key(physical_key.value())
        else {
            continue;
        };
        // Decode failures are covered by inspect_commit_row (MalformedRecord /
        // CrossLinkMismatch). Continue so sibling entities still validate.
        let Ok(references) = decoded(codec::decode_commit_entity_references(value.value())) else {
            continue;
        };
        let mut seen_in_commit = std::collections::BTreeSet::new();
        for reference in references {
            let target_key = reference.target().clone();
            if !seen_in_commit.insert(target_key.clone()) {
                // Duplicate target in one commit: mark chain broken if present.
                if let Some(chain) = chains.get_mut(&target_key) {
                    chain.intact = false;
                } else if chains.len() < entity_count_usize {
                    // INVARIANT: slot allocation assumes ENTITIES rows are never
                    // removed (true today). If a removal path appears, a dead
                    // target can steal a live entity's slot → false MissingCrossLink.
                    chains.insert(
                        target_key,
                        EntityChain {
                            version: reference.entity_version(),
                            hash: Some(reference.post_image_hash()),
                            expected_bundle: None,
                            migration_cursor: 0,
                            intact: false,
                            consumed: false,
                            seeded: false,
                        },
                    );
                } else {
                    record_orphan_target(&mut orphan_targets, &mut overflow, target_key);
                }
                continue;
            }
            if let Some(chain) = chains.get_mut(&target_key) {
                let mut expected_next = chain.version.checked_next();
                while chain.intact && expected_next != Some(reference.entity_version()) {
                    match advance_entity_chain_through_migration(
                        transaction,
                        &migrations,
                        &target_key,
                        chain,
                        Some(commit_sequence),
                    )? {
                        MigrationAdvance::Advanced | MigrationAdvance::Skipped => {
                            expected_next = chain.version.checked_next();
                        }
                        MigrationAdvance::Invalid => {
                            chain.intact = false;
                        }
                        MigrationAdvance::Exhausted => break,
                    }
                }
                if expected_next != Some(reference.entity_version()) {
                    chain.intact = false;
                }
                chain.version = reference.entity_version();
                chain.hash = Some(reference.post_image_hash());
                chain.expected_bundle = None;
                chain.seeded = false;
            } else if chains.len() < entity_count_usize {
                // INVARIANT: slot allocation assumes ENTITIES rows are never
                // removed (true today). If a removal path appears, a dead
                // target can steal a live entity's slot → false MissingCrossLink.
                // Under a pruned floor a chain may START at any version: its
                // earlier versions lived in tombstone-covered commits.
                let intact = pruned_floor > 0
                    || reference.entity_version() == riffdb_types::EntityVersion::first();
                chains.insert(
                    target_key,
                    EntityChain {
                        version: reference.entity_version(),
                        hash: Some(reference.post_image_hash()),
                        expected_bundle: None,
                        migration_cursor: 0,
                        intact,
                        consumed: false,
                        seeded: false,
                    },
                );
            } else {
                // At capacity: never grow the map; bound orphan reporting.
                record_orphan_target(&mut orphan_targets, &mut overflow, target_key);
            }
        }
    }
    Ok(EntityChainState {
        chains,
        migrations,
        orphan_targets,
        overflow,
        orphans_queued: false,
        pruned_floor,
    })
}

fn load_entity_migration_evidence(
    transaction: &ReadTransaction,
) -> Result<Vec<EntityMigrationEvidence>, StorageError> {
    let table = transaction
        .open_table(CONTRACT_MIGRATIONS)
        .map_err(table_error)?;
    let mut migrations = Vec::new();
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let Ok(operation_id) = keys::decode_contract_migration_operation_key(key.value()) else {
            continue;
        };
        let Ok(record) =
            riffdb_storage_api::proto_codec::decode_contract_migration_record_v1(value.value())
        else {
            continue;
        };
        if record.value().operation_id() != operation_id {
            continue;
        }
        migrations.push(EntityMigrationEvidence {
            operation_id,
            migration: record.value().artifacts().migration(),
            candidate: record.value().artifacts().candidate(),
            successor_frontier: record.value().successor_frontier(),
            administration_sequence: record.value().administration_sequence(),
        });
    }
    migrations.sort_unstable_by_key(|migration| migration.administration_sequence);
    if migrations
        .windows(2)
        .any(|pair| pair[0].administration_sequence == pair[1].administration_sequence)
    {
        migrations.clear();
    }
    Ok(migrations)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MigrationAdvance {
    Advanced,
    Skipped,
    Invalid,
    Exhausted,
}

fn advance_entity_chain_through_migration(
    transaction: &ReadTransaction,
    migrations: &[EntityMigrationEvidence],
    target: &riffdb_storage_api::EntityTarget,
    chain: &mut EntityChain,
    before_commit: Option<CommitSequence>,
) -> Result<MigrationAdvance, StorageError> {
    let Some(migration) = migrations.get(chain.migration_cursor).copied() else {
        return Ok(MigrationAdvance::Exhausted);
    };
    if before_commit.is_some_and(|commit| {
        migration
            .successor_frontier
            .is_some_and(|frontier| frontier >= commit)
    }) {
        return Ok(MigrationAdvance::Exhausted);
    }
    chain.migration_cursor = chain
        .migration_cursor
        .checked_add(1)
        .ok_or_else(limit_exceeded)?;

    let retained = transaction
        .open_table(RETIRED_ENTITIES)
        .map_err(table_error)?;
    let retained_key =
        keys::encode_retired_entity_key(migration.operation_id, target).map_err(|_| invariant())?;
    let Some(encoded) = retained
        .get(retained_key.as_slice())
        .map_err(precommit_storage_error)?
    else {
        return Ok(MigrationAdvance::Skipped);
    };
    let Ok(retired) =
        riffdb_storage_api::proto_codec::decode_retired_entity_record_v1(encoded.value())
    else {
        return Ok(MigrationAdvance::Invalid);
    };
    if retired.value().operation_id() != migration.operation_id
        || retired.value().migration() != migration.migration
        || retired.value().original_target() != target
    {
        return Ok(MigrationAdvance::Invalid);
    }
    let Ok(source) = codec::decode_entity_record_v1(retired.value().original_entity_envelope())
    else {
        return Ok(MigrationAdvance::Invalid);
    };
    if source.value().target() != target || source.value().entity_version() != chain.version {
        return Ok(MigrationAdvance::Invalid);
    }
    let predecessor_matches = match (chain.hash, chain.expected_bundle) {
        (Some(hash), _) => riffdb_storage_api::derive_entity_record_hash_v1(source.value())
            .is_ok_and(|actual| actual == hash),
        (None, Some(bundle)) => source.value().schema_binding().bundle_hash() == bundle,
        (None, None) => false,
    };
    if !predecessor_matches {
        return Ok(MigrationAdvance::Invalid);
    }
    let Some(next_version) = chain.version.checked_next() else {
        return Ok(MigrationAdvance::Invalid);
    };
    chain.version = next_version;
    chain.hash = None;
    chain.expected_bundle = Some(migration.candidate);
    Ok(MigrationAdvance::Advanced)
}

fn record_orphan_target(
    orphan_targets: &mut Vec<riffdb_storage_api::EntityTarget>,
    overflow: &mut bool,
    target: riffdb_storage_api::EntityTarget,
) {
    *overflow = true;
    if orphan_targets.len() < ENTITY_CHAIN_ORPHAN_REPORT_CAP
        && !orphan_targets.iter().any(|t| t == &target)
    {
        orphan_targets.push(target);
    }
}

/// Marks one chain consumed from a decoded ENTITIES key (O(log N)).
///
/// The entity-key envelope carries the type id, so the row target is
/// reconstructed without scanning the map (NEW-6).
fn mark_entity_chain_consumed(
    chains: &mut std::collections::BTreeMap<riffdb_storage_api::EntityTarget, EntityChain>,
    key: &riffdb_types::EntityKey,
) {
    let Ok(target) = riffdb_storage_api::EntityTarget::new(key.entity_type_id(), key.clone())
    else {
        return;
    };
    if let Some(chain) = chains.get_mut(&target) {
        chain.consumed = true;
    }
}

/// Production predicate: does the current ENTITIES row match the commit-built chain tip?
fn entity_row_matches_chain(
    chain: &EntityChain,
    current: &riffdb_storage_api::StoredEntityRecordV1,
) -> bool {
    chain.intact
        && chain.version == current.entity_version()
        && match (chain.hash, chain.expected_bundle) {
            (Some(expected), _) => riffdb_storage_api::derive_entity_record_hash_v1(current)
                .is_ok_and(|hash| hash == expected),
            (None, Some(expected)) => current.schema_binding().bundle_hash() == expected,
            // Prefix-only target under a verified checkpoint: the seeded chain
            // carries the fingerprint-verified version but no post-image hash
            // (ADR-0019 A1 assigns below-S content to proof + sample). Hash
            // checks resume for any chain a suffix reference touched.
            (None, None) => chain.seeded,
        }
}

#[cfg(test)]
fn entity_history_matches_chain(
    chains: &std::collections::BTreeMap<riffdb_storage_api::EntityTarget, EntityChain>,
    current: &riffdb_storage_api::StoredEntityRecordV1,
) -> bool {
    match chains.get(current.target()) {
        Some(chain) => entity_row_matches_chain(chain, current),
        None => false,
    }
}

fn provenance_graph_is_reciprocal(
    transaction: &ReadTransaction,
    provenance: &riffdb_storage_api::StoredProvenanceRecordV1,
) -> Result<bool, StorageError> {
    let Some(commit) = get_commit(transaction, provenance.commit_sequence())? else {
        // Provenance is retained across prune; commit bodies below the watermark
        // are intentionally absent (tombstone-covered). Terminal outcome remains.
        let watermark = retention_watermark_sequence(transaction)?;
        if crate::retention::sequence_covered_by_watermark(
            provenance.commit_sequence().get(),
            watermark,
        ) {
            return Ok(get_terminal(transaction, provenance.identity())?
                .is_some_and(|record| matches!(record, IdempotencyRecordV1::StoredOutcome(_))));
        }
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
        && provenance.affected_entities().len() == commit.entity_references().len()
        && provenance
            .affected_entities()
            .iter()
            .zip(commit.entity_references())
            .all(|(affected, reference)| {
                affected.target() == reference.target()
                    && affected.entity_version() == reference.entity_version()
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
    // Linear in the number of service-audit rows for this request_id (bounded by
    // the fused-audit write gate to ≤2), not in total AUDIT table size. Peers are
    // discovered via the durable AUDIT_BY_REQUEST prefix range, then point-loaded
    // from AUDIT. Lifecycle matching rules are unchanged.
    let index = transaction
        .open_table(AUDIT_BY_REQUEST)
        .map_err(table_error)?;
    let audit = transaction.open_table(AUDIT).map_err(table_error)?;
    let prefix = keys::encode_audit_by_request_prefix(target.request_id());
    let mut lifecycle = None;
    let mut target_seen = false;
    let scan = index
        .range::<&[u8]>((Included(prefix.as_slice()), Unbounded))
        .map_err(precommit_storage_error)?;
    for entry in scan {
        let (physical_key, index_value) = entry.map_err(precommit_storage_error)?;
        if !physical_key.value().starts_with(prefix.as_slice()) {
            break;
        }
        let Ok((request_id, sequence)) = keys::decode_audit_by_request_key(physical_key.value())
        else {
            return Ok(false);
        };
        if request_id != target.request_id() {
            return Ok(false);
        }
        let Ok(index_row) = decoded(codec::decode_service_audit_request_index_v1(
            index_value.value(),
        )) else {
            return Ok(false);
        };
        if index_row.request_id() != request_id || index_row.administration_sequence() != sequence {
            return Ok(false);
        }
        let audit_key = keys::encode_audit_key(sequence);
        let Some(audit_value) = audit
            .get(audit_key.as_slice())
            .map_err(precommit_storage_error)?
        else {
            return Ok(false);
        };
        let Ok(record) = decoded(codec::decode_administration_audit_record_v1(
            audit_value.value(),
        )) else {
            return Ok(false);
        };
        drop(audit_value);
        if record.administration_sequence() != sequence {
            return Ok(false);
        }
        let riffdb_storage_api::StoredAdministrationAuditRecordV1::Service(record) = record else {
            return Ok(false);
        };
        if record.request_id() != target.request_id() {
            return Ok(false);
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
            None if record.principal().is_some()
                && record.operation()
                    == riffdb_types::ServiceOperationV1::ApplyContractMigration
                && record.phase() == riffdb_types::ServiceAuditPhaseV1::Succeeded
                && matches!(
                    record.link(),
                    riffdb_types::ServiceAuditLinkV1::ControlPlane {
                        administration_sequence
                    } if administration_sequence == record.administration_sequence()
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
        } => {
            if record.phase() != riffdb_types::ServiceAuditPhaseV1::Succeeded
                || !matches!(
                    record.operation(),
                    riffdb_types::ServiceOperationV1::ExecuteCommand
                        | riffdb_types::ServiceOperationV1::ResolveCommandOutcome
                )
            {
                return Ok(false);
            }
            let commit_side = match get_commit(transaction, commit_sequence)? {
                Some(commit) => commit.provenance_id() == provenance_id,
                None => {
                    // Audit history is retained across prune; the linked commit
                    // body below the watermark is tombstone-covered absence
                    // (ADR-0085 A2). The retained provenance record still
                    // carries the binding and must reciprocate below.
                    let watermark = retention_watermark_sequence(transaction)?;
                    crate::retention::sequence_covered_by_watermark(
                        commit_sequence.get(),
                        watermark,
                    )
                }
            };
            Ok(commit_side
                && get_provenance(transaction, provenance_id)?
                    .is_some_and(|provenance| provenance.commit_sequence() == commit_sequence))
        }
        riffdb_types::ServiceAuditLinkV1::ControlPlane {
            administration_sequence,
        } => {
            if record.operation() == riffdb_types::ServiceOperationV1::ApplyContractMigration {
                return migration_service_link_is_valid(
                    transaction,
                    record,
                    administration_sequence,
                );
            }
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

fn migration_service_link_is_valid(
    transaction: &ReadTransaction,
    audit: &riffdb_storage_api::StoredServiceAuditRecordV1,
    administration_sequence: riffdb_types::AdministrationSequence,
) -> Result<bool, StorageError> {
    if audit.administration_sequence() != administration_sequence
        || audit.phase() != riffdb_types::ServiceAuditPhaseV1::Succeeded
    {
        return Ok(false);
    }
    let Some(principal) = audit.principal() else {
        return Ok(false);
    };
    let migrations = transaction
        .open_table(CONTRACT_MIGRATIONS)
        .map_err(table_error)?;
    let mut matching = 0_u8;
    for row in migrations.iter().map_err(precommit_storage_error)? {
        let (_, encoded) = row.map_err(precommit_storage_error)?;
        let Ok(record) =
            riffdb_storage_api::proto_codec::decode_contract_migration_record_v1(encoded.value())
        else {
            return Ok(false);
        };
        let record = record.value();
        if record.administration_sequence() == administration_sequence {
            matching = matching.saturating_add(1);
            if matching != 1
                || record.principal() != principal
                || record.approval_id() != audit.approval_id()
            {
                return Ok(false);
            }
        }
    }
    Ok(matching == 1)
}

fn application_allocator_matches(
    transaction: &ReadTransaction,
    allocator: ApplicationSequenceAllocator,
) -> Result<bool, StorageError> {
    let table = transaction.open_table(COMMITS).map_err(table_error)?;
    let events = transaction.open_table(EVENTS).map_err(table_error)?;
    let expected = match table.last().map_err(precommit_storage_error)? {
        None => {
            // Empty commits table: never-written, or fully pruned. Accept an
            // advanced allocator exactly when the verified watermark covers
            // the whole pruned prefix (allocator Next(n) ⇔ watermark == n-1;
            // Exhausted ⇔ watermark == u64::MAX) — same rule as the backup
            // facts check (ADR-0085 A2).
            if allocator != ApplicationSequenceAllocator::initial() {
                let watermark = retention_watermark_sequence(transaction)?;
                let covered = match allocator {
                    ApplicationSequenceAllocator::Next(next) => {
                        watermark > 0 && watermark == next.get().saturating_sub(1)
                    }
                    ApplicationSequenceAllocator::Exhausted => watermark == u64::MAX,
                };
                return Ok(covered);
            }
            ApplicationSequenceAllocator::initial()
        }
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
        ActiveCatalogPointerV1, AdministrationSequenceAllocator, AffectedEntityV1,
        ApplicationSequenceAllocator, AuditPrincipalV1, CapabilityAdministrationOperationV1,
        CapabilityBootstrapMarkerV1, CapabilityGrantV1, CapabilityPermissionKindV1,
        CapabilityPermissionV1, CapabilityPermissionsV1, CapabilityRequestedRecordV1,
        CatalogActivationIntentV1, CatalogAdministrationRepository, CommittedEntityReferenceV2,
        DatabaseInitializationPort, DeclaredOutcome, DurabilityMode, DurableKeySchemaBindingV1,
        EntityTarget, ExecutablePlanRef, ExpectedEntityState, HistoricalEvidencePage,
        IdempotencyIdentity, IdempotencyKeyDigest, PartitionScopeV1, ProjectionGenerationPosition,
        ProjectionLifecycleV1, PublishedApplyModeV1, ReadDependencies, ReadDependency,
        ReadableCapabilityDigestInventory, ReadableIdempotencyDigestInventory,
        RevocationReasonCodeV1, StoredAdministrationAuditRecordV1,
        StoredAdmittedProvenanceClaimsV1, StoredCapabilityAdministrationV1,
        StoredCapabilityRecordV1, StoredCatalogAdministrationV1, StoredCommitRecordV1,
        StoredContractBundleV1, StoredEntityRecordV1, StoredIndexEntryV1, StoredIndexEntryV2,
        StoredOutcomeV1, StoredProjectionApplyV1, StoredProjectionControlV1,
        StoredProvenanceRecordV1, StoredReadDependenciesV1, StoredServiceAuditRecordV1,
        StructuralEvidenceEnd, StructuralEvidencePage, derive_entity_record_hash_v1,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdministrationSequence, AdmittedActorContext, AggregateTypeId,
        Audience, CanonicalInputHash, CanonicalRecord, CanonicalValue, CapabilityId,
        CapabilityTokenDigest, CommandId, CommitSequence, DigestKeyId, EntityKeyBuilder,
        EntityTypeId, EntityVersion, Environment, FieldId, IndexEntryKeyBuilder, LogicalTime,
        OutcomeId, PartitionKeyBuilder, PlanHash, ProjectionApplyHash, ProjectionApplyKey,
        ProjectionGeneration, ProjectionId, ProjectionIdentity, ProjectionPlanHash, ProvenanceId,
        RequestId, ScopedPartitionV1, ServiceAuditLinkV1, ServiceAuditPhaseV1,
        ServiceAuditTargetsV1, ServiceIngressKindV1, ServiceOperationV1, TenantScope, Timestamp,
        hash_partition_key,
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

    /// Per-entity oracle resurrected from pre-Package-E `entity_history_matches`
    /// (6892c84), adapted to entity references (mutations are no longer stored).
    fn entity_history_matches_oracle(
        transaction: &ReadTransaction,
        current: &StoredEntityRecordV1,
    ) -> Result<bool, StorageError> {
        let table = transaction.open_table(COMMITS).map_err(table_error)?;
        let mut prior_version: Option<EntityVersion> = None;
        let mut saw_mutation = false;
        let mut terminal_hash = None;
        for entry in table.iter().map_err(precommit_storage_error)? {
            let (physical_key, value) = entry.map_err(precommit_storage_error)?;
            let Ok(sequence) = keys::decode_application_sequence_key(physical_key.value()) else {
                return Ok(false);
            };
            let Ok(references) = decoded(codec::decode_commit_entity_references(value.value()))
            else {
                return Ok(false);
            };
            let _ = sequence;
            let mut matching = references
                .iter()
                .filter(|reference| reference.target() == current.target());
            let Some(reference) = matching.next() else {
                continue;
            };
            if matching.next().is_some() {
                return Ok(false);
            }
            let expected =
                CommittedEntityReferenceV2::expected_from_version(reference.entity_version());
            let expected_matches = match (prior_version, expected) {
                (None, ExpectedEntityState::Absent) => {
                    reference.entity_version() == EntityVersion::first()
                }
                (Some(prior), ExpectedEntityState::Present(version)) => prior == version,
                (None, ExpectedEntityState::Present(_))
                | (Some(_), ExpectedEntityState::Absent) => false,
            };
            if !expected_matches {
                return Ok(false);
            }
            prior_version = Some(reference.entity_version());
            terminal_hash = Some(reference.post_image_hash());
            saw_mutation = true;
        }
        let current_hash = derive_entity_record_hash_v1(current).expect("hash current");
        Ok(saw_mutation
            && prior_version == Some(current.entity_version())
            && terminal_hash == Some(current_hash))
    }

    fn history_entity(
        seed: u64,
        version: EntityVersion,
        fields: &[u8],
        bundle: &StoredContractBundleV1,
    ) -> StoredEntityRecordV1 {
        let entity_type = EntityTypeId::new(7).expect("entity type");
        let mut key = EntityKeyBuilder::new(entity_type);
        key.push_u64(seed).expect("entity key component");
        let target =
            EntityTarget::new(entity_type, key.finish().expect("entity key")).expect("target");
        let binding = DurableKeySchemaBindingV1::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
        );
        let field_id = FieldId::new(1).expect("field");
        let record = CanonicalRecord::new(vec![(
            field_id,
            CanonicalValue::bytes(fields.to_vec()).expect("field bytes"),
        )])
        .expect("fields");
        StoredEntityRecordV1::new(target, version, bundle.contract_version(), binding, record)
            .expect("entity")
    }

    fn history_commit(
        sequence: CommitSequence,
        plan: ExecutablePlanRef,
        images: &[&StoredEntityRecordV1],
    ) -> StoredCommitRecordV1 {
        let sequence_byte = u8::try_from(sequence.get().min(250)).expect("sequence byte");
        let actor = AdmittedActorContext::new(
            ActorId::new("history-actor").expect("actor"),
            ActorKind::Human,
            TenantScope::Global,
            None,
        );
        let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
        partition.push_u64(1).expect("partition component");
        let partition_hash =
            hash_partition_key(partition.finish().expect("partition key").as_bytes());
        let references = images
            .iter()
            .map(|image| {
                CommittedEntityReferenceV2::from_post_image(image).expect("entity reference")
            })
            .collect::<Vec<_>>();
        let observations = images
            .iter()
            .map(|image| ReadDependency::EntityObservation {
                target: image.target().clone(),
                expected: CommittedEntityReferenceV2::expected_from_version(image.entity_version()),
            })
            .collect::<Vec<_>>();
        let read_dependencies = StoredReadDependenciesV1::from_live(
            &ReadDependencies::new(observations).expect("dependencies"),
        )
        .expect("stored dependencies");
        StoredCommitRecordV1::new(
            sequence,
            RequestId::from_bytes(uuid_bytes(sequence_byte.wrapping_add(0x20)))
                .expect("request ID"),
            plan,
            CanonicalInputHash::from_bytes([sequence_byte; 32]),
            actor,
            LogicalTime::new(Timestamp::new(i64::from(sequence_byte), 0).expect("timestamp")),
            partition_hash,
            Vec::new(),
            read_dependencies,
            references,
            Vec::new(),
            DeclaredOutcome::new(
                OutcomeId::first(),
                CanonicalRecord::new(Vec::new()).expect("outcome fields"),
            )
            .expect("outcome"),
            ProvenanceId::from_bytes(uuid_bytes(sequence_byte.wrapping_add(0x40)))
                .expect("provenance ID"),
            Vec::new(),
            DurabilityMode::Sync,
        )
        .expect("history commit")
    }

    fn write_entities_and_commits(
        store: &RedbStore,
        id: DatabaseId,
        bundle: &StoredContractBundleV1,
        entities: &[StoredEntityRecordV1],
        commits: &[StoredCommitRecordV1],
    ) {
        let write = store
            .shared
            .database
            .begin_write()
            .expect("write history fixture");
        {
            let mut table = write.open_table(CONTRACT_BUNDLES).expect("bundles");
            let key = keys::encode_contract_bundle_key(bundle.lineage(), bundle.contract_version())
                .expect("bundle key");
            let encoded = codec::encode_contract_bundle_v1(bundle).expect("encode bundle");
            table
                .insert(key.as_slice(), encoded.as_bytes())
                .expect("insert bundle");
        }
        // Activate the bundle so inspect_header / inspect_bundle_row produce
        // zero residuals on an intact control (C1 falsifiability).
        let pointer = ActiveCatalogPointerV1::from_bundle(bundle);
        let activation = StoredCatalogAdministrationV1::from_stored_parts(
            AdministrationSequence::first(),
            request_id(0xe1),
            Timestamp::new(1, 0).expect("timestamp"),
            audit_principal(0xe2),
            None,
            pointer.clone(),
            None,
        );
        {
            let encoded_active =
                codec::encode_active_catalog_pointer_v1(&pointer).expect("encode active");
            write
                .open_table(CATALOG_ACTIVE)
                .expect("active")
                .insert(CATALOG_ACTIVE_KEY.as_slice(), encoded_active.as_bytes())
                .expect("insert active");
        }
        {
            let encoded_audit = codec::encode_administration_audit_record_v1(
                &StoredAdministrationAuditRecordV1::Catalog(activation),
            )
            .expect("encode catalog activation");
            write
                .open_table(AUDIT)
                .expect("audit")
                .insert(
                    keys::encode_audit_key(AdministrationSequence::first()).as_slice(),
                    encoded_audit.as_bytes(),
                )
                .expect("insert activation");
        }
        {
            let mut table = write.open_table(ENTITIES).expect("entities");
            for entity in entities {
                let encoded = codec::encode_entity_record_v1(entity).expect("encode entity");
                table
                    .insert(entity.target().key().as_bytes(), encoded.as_bytes())
                    .expect("insert entity");
            }
        }
        {
            let mut commits_table = write.open_table(COMMITS).expect("commits");
            let mut outcomes = write.open_table(IDEMPOTENCY).expect("idempotency");
            let mut provenance_table = write.open_table(PROVENANCE).expect("provenance");
            for (ordinal, commit) in commits.iter().enumerate() {
                let key = keys::encode_application_sequence_key(commit.commit_sequence());
                let encoded = codec::encode_commit_record_v1(commit).expect("encode commit");
                commits_table
                    .insert(key.as_slice(), encoded.as_bytes())
                    .expect("insert commit");
                // Minimal reciprocal outcome/provenance so commit inspect noise
                // does not drown entity-history findings.
                let mut seed = [0x31u8; 32];
                seed[0] = u8::try_from(ordinal.wrapping_add(1)).unwrap_or(1);
                let identity = IdempotencyIdentity::new(
                    id,
                    Environment::new("test").expect("environment"),
                    TenantScope::Global,
                    commit.actor().principal_id().clone(),
                    commit.plan().contract_lineage().clone(),
                    commit.plan().command_id(),
                    IdempotencyKeyDigest::from_hmac_bytes(
                        DigestKeyId::new(1).expect("digest key"),
                        seed,
                    ),
                );
                let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
                partition.push_u64(1).expect("partition");
                let partition_key = partition.finish().expect("partition key");
                let outcome = StoredOutcomeV1::new(
                    identity.clone(),
                    commit.commit_sequence(),
                    commit.admission_request_id(),
                    commit.plan().clone(),
                    commit.canonical_input_hash(),
                    commit.actor().clone(),
                    commit.logical_time(),
                    partition_key,
                    commit.partition_hash(),
                    commit.conflict_hashes().to_vec(),
                    commit.declared_outcome().clone(),
                    StoredAdmittedProvenanceClaimsV1::default(),
                    commit.provenance_id(),
                    commit.durability_mode(),
                )
                .expect("outcome");
                let affected = commit
                    .entity_references()
                    .iter()
                    .map(|reference| {
                        AffectedEntityV1::from_stored_parts(
                            reference.target().clone(),
                            reference.entity_version(),
                        )
                    })
                    .collect();
                let provenance = StoredProvenanceRecordV1::new(
                    commit.provenance_id(),
                    commit.commit_sequence(),
                    identity,
                    commit.admission_request_id(),
                    commit.plan().clone(),
                    commit.canonical_input_hash(),
                    commit.actor().clone(),
                    commit.logical_time(),
                    commit.partition_hash(),
                    commit.conflict_hashes().to_vec(),
                    commit.declared_outcome().outcome_id(),
                    affected,
                    commit.outbox_event_ids().to_vec(),
                    StoredAdmittedProvenanceClaimsV1::default(),
                )
                .expect("provenance");
                let outcome_encoded =
                    codec::encode_stored_outcome_v1(&outcome).expect("encode outcome");
                let identity_key = outcome.identity().storage_key().expect("identity key");
                outcomes
                    .insert(identity_key.as_bytes(), outcome_encoded.as_bytes())
                    .expect("insert outcome");
                let provenance_encoded =
                    codec::encode_provenance_record_v1(&provenance).expect("encode provenance");
                let provenance_key = keys::encode_provenance_key(commit.provenance_id());
                provenance_table
                    .insert(provenance_key.as_slice(), provenance_encoded.as_bytes())
                    .expect("insert provenance");
            }
        }
        // Advance application sequence past the highest commit so sequence
        // continuity does not flag a discontinuity.
        if let Some(last) = commits.iter().map(|c| c.commit_sequence()).max()
            && let Some(next) = last.checked_next()
        {
            let encoded = codec::encode_application_sequence_allocator_v1(
                ApplicationSequenceAllocator::Next(next),
            )
            .expect("allocator");
            write
                .open_table(META)
                .expect("meta")
                .insert(META_APPLICATION_SEQUENCE, encoded.as_bytes())
                .expect("update allocator");
        }
        // Administration allocator must match the single catalog activation.
        {
            let next = AdministrationSequence::first()
                .checked_next()
                .expect("admin next");
            let encoded = codec::encode_administration_sequence_allocator_v1(
                AdministrationSequenceAllocator::Next(next),
            )
            .expect("admin allocator");
            write
                .open_table(META)
                .expect("meta")
                .insert(META_ADMINISTRATION_SEQUENCE, encoded.as_bytes())
                .expect("update admin allocator");
        }
        write.commit().expect("commit history fixture");
    }

    fn finding_codes(findings: &[StructuralFinding]) -> Vec<StructuralFindingCode> {
        findings.iter().map(|f| f.code()).collect()
    }

    fn count_code(findings: &[StructuralFinding], code: StructuralFindingCode) -> usize {
        findings.iter().filter(|f| f.code() == code).count()
    }

    /// Intact two-entity control used by corruption tests for a zero-finding baseline.
    fn intact_two_entity_fixture(
        label: &str,
        id_seed: u8,
    ) -> (
        TestDatabasePath,
        RedbStore,
        StoredContractBundleV1,
        StoredEntityRecordV1,
        StoredEntityRecordV1,
    ) {
        let path = TestDatabasePath::new(label);
        let id = database_id(id_seed);
        let store = initialized_store(&path, id);
        let bundle = stored_bundle("entity-history", 1, b"entity-history-bundle");
        let plan = ExecutablePlanRef::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            CommandId::first(),
            PlanHash::from_bytes([0x44; 32]),
        );
        let a = history_entity(1, EntityVersion::first(), b"a", &bundle);
        let b = history_entity(2, EntityVersion::first(), b"b", &bundle);
        let commit = history_commit(CommitSequence::first(), plan, &[&a, &b]);
        write_entities_and_commits(&store, id, &bundle, &[a.clone(), b.clone()], &[commit]);
        (path, store, bundle, a, b)
    }

    #[test]
    fn entity_history_intact_control_produces_zero_structural_findings() {
        let (_path, store, _bundle, _a, _b) = intact_two_entity_fixture("entity-control", 0x80);
        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin structural");
        let (_end, findings) = collect_structural(&mut session);
        assert_eq!(
            findings,
            Vec::new(),
            "intact activated fixture must be finding-clean: {findings:?}"
        );
    }

    #[test]
    fn entity_history_differential_oracle_agrees_on_populated_and_corrupted_histories() {
        let bundle = stored_bundle("entity-history", 1, b"entity-history-bundle");
        let plan = ExecutablePlanRef::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            CommandId::first(),
            PlanHash::from_bytes([0x44; 32]),
        );

        // Intact histories plus one corrupted history with a false verdict.
        #[allow(clippy::type_complexity)]
        let cases: Vec<(
            &str,
            Vec<StoredEntityRecordV1>,
            Vec<StoredCommitRecordV1>,
            Vec<bool>, // expected oracle/chain per entity
        )> = {
            let e1_v1 = history_entity(1, EntityVersion::first(), b"a1", &bundle);
            let e1_v2 = history_entity(
                1,
                EntityVersion::first().checked_next().expect("v2"),
                b"a2",
                &bundle,
            );
            let e2_v1 = history_entity(2, EntityVersion::first(), b"b1", &bundle);
            let e2_v2 = history_entity(
                2,
                EntityVersion::first().checked_next().expect("v2"),
                b"b2",
                &bundle,
            );
            let h1_commits = vec![
                history_commit(CommitSequence::first(), plan.clone(), &[&e1_v1, &e2_v1]),
                history_commit(
                    CommitSequence::new(2).expect("seq"),
                    plan.clone(),
                    &[&e1_v2],
                ),
                history_commit(
                    CommitSequence::new(3).expect("seq"),
                    plan.clone(),
                    &[&e2_v2],
                ),
            ];

            let e3 = history_entity(3, EntityVersion::first(), b"c1", &bundle);
            let e4 = history_entity(4, EntityVersion::first(), b"d1", &bundle);
            let h2_commits = vec![
                history_commit(CommitSequence::first(), plan.clone(), &[&e3]),
                history_commit(CommitSequence::new(2).expect("seq"), plan.clone(), &[&e4]),
            ];

            // Corrupted: good sibling + gapped entity (false verdict required).
            let good = history_entity(10, EntityVersion::first(), b"good", &bundle);
            let bad_v1 = history_entity(11, EntityVersion::first(), b"bad1", &bundle);
            let bad_v3 = history_entity(11, EntityVersion::new(3).expect("v3"), b"bad3", &bundle);
            let h_bad_commits = vec![
                history_commit(CommitSequence::first(), plan.clone(), &[&good, &bad_v1]),
                history_commit(CommitSequence::new(2).expect("seq"), plan, &[&bad_v3]),
            ];

            vec![
                (
                    "two-entity-versions",
                    vec![e1_v2, e2_v2],
                    h1_commits,
                    vec![true, true],
                ),
                (
                    "two-entity-creates",
                    vec![e3, e4],
                    h2_commits,
                    vec![true, true],
                ),
                (
                    "corrupted-gap",
                    vec![good, bad_v3],
                    h_bad_commits,
                    vec![true, false],
                ),
            ]
        };

        for (index, (label, entities, commits, expected)) in cases.into_iter().enumerate() {
            let path = TestDatabasePath::new(&format!("entity-oracle-{index}"));
            let store = initialized_store(&path, database_id(0x81 + index as u8));
            write_entities_and_commits(
                &store,
                database_id(0x81 + index as u8),
                &bundle,
                &entities,
                &commits,
            );
            let transaction = store
                .shared
                .database
                .begin_read()
                .expect("read transaction");
            let chains = build_entity_chains(
                &transaction,
                u64::try_from(entities.len()).expect("entity count"),
                None,
            )
            .expect("build chains");
            assert!(
                !chains.chains.is_empty(),
                "{label}: must produce non-empty chains"
            );
            for (entity, expect_ok) in entities.iter().zip(expected.iter().copied()) {
                let oracle =
                    entity_history_matches_oracle(&transaction, entity).expect("oracle check");
                // Production predicate (same function the structural session uses).
                let chain = entity_history_matches_chain(&chains.chains, entity);
                assert_eq!(
                    oracle,
                    chain,
                    "{label} entity {:?} oracle={oracle} chain={chain}",
                    entity.target().key().as_bytes()
                );
                assert_eq!(
                    oracle, expect_ok,
                    "{label} expected verdict {expect_ok}, got {oracle}"
                );
            }
            let mut session = store
                .begin_structural_evidence(inputs())
                .expect("begin structural");
            let (_end, findings) = collect_structural(&mut session);
            if expected.iter().all(|ok| *ok) {
                assert_eq!(
                    findings,
                    Vec::new(),
                    "{label}: intact history must be clean: {:?}",
                    finding_codes(&findings)
                );
            } else {
                assert_eq!(
                    count_code(&findings, StructuralFindingCode::MissingCrossLink),
                    1,
                    "{label}: exactly one MissingCrossLink for the broken entity: {:?}",
                    finding_codes(&findings)
                );
                assert_eq!(
                    count_code(&findings, StructuralFindingCode::CrossLinkMismatch),
                    0,
                    "{label}: no orphan mismatches on a consumed broken chain: {:?}",
                    finding_codes(&findings)
                );
            }
        }
    }

    #[test]
    fn entity_chain_broken_version_gap_via_structural_session() {
        let path = TestDatabasePath::new("entity-gap");
        let store = initialized_store(&path, database_id(0x91));
        let bundle = stored_bundle("entity-history", 1, b"entity-history-bundle");
        let plan = ExecutablePlanRef::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            CommandId::first(),
            PlanHash::from_bytes([0x45; 32]),
        );
        let good_v1 = history_entity(1, EntityVersion::first(), b"good1", &bundle);
        let good_v2 = history_entity(
            1,
            EntityVersion::first().checked_next().expect("v2"),
            b"good2",
            &bundle,
        );
        let bad_v1 = history_entity(2, EntityVersion::first(), b"bad1", &bundle);
        let bad_v3 = history_entity(2, EntityVersion::new(3).expect("v3"), b"bad3", &bundle);
        let commits = vec![
            history_commit(CommitSequence::first(), plan.clone(), &[&good_v1, &bad_v1]),
            history_commit(
                CommitSequence::new(2).expect("seq"),
                plan.clone(),
                &[&good_v2],
            ),
            history_commit(CommitSequence::new(3).expect("seq"), plan, &[&bad_v3]),
        ];
        write_entities_and_commits(
            &store,
            database_id(0x91),
            &bundle,
            &[good_v2.clone(), bad_v3.clone()],
            &commits,
        );

        let transaction = store.shared.database.begin_read().expect("read");
        let chains = build_entity_chains(&transaction, 2, None).expect("chains");
        assert!(entity_history_matches_chain(&chains.chains, &good_v2));
        assert!(!entity_history_matches_chain(&chains.chains, &bad_v3));

        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin structural");
        let (_end, findings) = collect_structural(&mut session);
        // Exact set: one MissingCrossLink for the gapped entity; sibling clean.
        assert_eq!(
            finding_codes(&findings),
            vec![StructuralFindingCode::MissingCrossLink],
            "exact finding set for version gap"
        );
    }

    #[test]
    fn entity_chain_fabricated_terminal_hash_via_structural_session() {
        let path = TestDatabasePath::new("entity-fab-hash");
        let store = initialized_store(&path, database_id(0x92));
        let bundle = stored_bundle("entity-history", 1, b"entity-history-bundle");
        let plan = ExecutablePlanRef::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            CommandId::first(),
            PlanHash::from_bytes([0x46; 32]),
        );
        let a = history_entity(1, EntityVersion::first(), b"a", &bundle);
        let b = history_entity(2, EntityVersion::first(), b"b", &bundle);
        let commit = history_commit(CommitSequence::first(), plan, &[&a, &b]);
        let a_tampered = history_entity(1, EntityVersion::first(), b"TAMPERED", &bundle);
        write_entities_and_commits(
            &store,
            database_id(0x92),
            &bundle,
            &[a_tampered.clone(), b.clone()],
            &[commit],
        );

        let transaction = store.shared.database.begin_read().expect("read");
        let chains = build_entity_chains(&transaction, 2, None).expect("chains");
        assert!(entity_history_matches_chain(&chains.chains, &b));
        assert!(!entity_history_matches_chain(&chains.chains, &a_tampered));

        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin structural");
        let (_end, findings) = collect_structural(&mut session);
        assert_eq!(
            finding_codes(&findings),
            vec![StructuralFindingCode::MissingCrossLink],
            "exact finding set for fabricated hash"
        );
    }

    #[test]
    fn entity_chain_duplicate_target_in_one_commit_via_structural_session() {
        let path = TestDatabasePath::new("entity-dup-target");
        let store = initialized_store(&path, database_id(0x93));
        let bundle = stored_bundle("entity-history", 1, b"entity-history-bundle");
        let plan = ExecutablePlanRef::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            CommandId::first(),
            PlanHash::from_bytes([0x47; 32]),
        );
        let a = history_entity(1, EntityVersion::first(), b"a", &bundle);
        let b = history_entity(2, EntityVersion::first(), b"b", &bundle);
        let commit = history_commit(CommitSequence::first(), plan, &[&a, &b]);
        // Write a clean graph first (outcomes/provenance/allocators), then corrupt
        // the commit wire with a duplicate entity reference.
        write_entities_and_commits(
            &store,
            database_id(0x93),
            &bundle,
            &[a.clone(), b.clone()],
            std::slice::from_ref(&commit),
        );
        let write = store.shared.database.begin_write().expect("write");
        {
            let encoded = codec::encode_commit_record_v1(&commit).expect("encode");
            let corrupted =
                riffdb_storage_api::inject_duplicate_entity_reference_v3(encoded.as_bytes())
                    .expect("inject duplicate");
            let key = keys::encode_application_sequence_key(CommitSequence::first());
            write
                .open_table(COMMITS)
                .expect("commits")
                .insert(key.as_slice(), corrupted.as_bytes())
                .expect("insert");
        }
        write.commit().expect("commit");

        let transaction = store.shared.database.begin_read().expect("read");
        let chains = build_entity_chains(&transaction, 2, None).expect("chains");
        // First entity in the commit is marked broken by the duplicate; B intact.
        assert!(
            !entity_history_matches_chain(&chains.chains, &a),
            "duplicated target A must fail chain"
        );
        assert!(
            entity_history_matches_chain(&chains.chains, &b),
            "intact sibling B must pass chain"
        );

        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin structural");
        let (_end, findings) = collect_structural(&mut session);
        // Chain finding for A + commit-row decode/reciprocal failure for the
        // duplicated wire (StoredCommitRecordV1 rejects duplicates on full decode).
        assert!(
            count_code(&findings, StructuralFindingCode::MissingCrossLink) >= 1,
            "duplicate must surface chain/commit findings: {:?}",
            finding_codes(&findings)
        );
        assert!(
            entity_history_matches_chain(&chains.chains, &b),
            "sibling must remain chain-intact"
        );
    }

    #[test]
    fn entity_chain_orphan_overflow_via_structural_session() {
        let path = TestDatabasePath::new("entity-orphan-overflow");
        let store = initialized_store(&path, database_id(0x94));
        let bundle = stored_bundle("entity-history", 1, b"entity-history-bundle");
        let plan = ExecutablePlanRef::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            CommandId::first(),
            PlanHash::from_bytes([0x48; 32]),
        );
        let real = history_entity(1, EntityVersion::first(), b"real", &bundle);
        let ghost = history_entity(99, EntityVersion::first(), b"ghost", &bundle);
        let commit = history_commit(CommitSequence::first(), plan, &[&real, &ghost]);
        write_entities_and_commits(
            &store,
            database_id(0x94),
            &bundle,
            std::slice::from_ref(&real),
            &[commit],
        );

        let transaction = store.shared.database.begin_read().expect("read");
        let chains = build_entity_chains(&transaction, 1, None).expect("chains");
        assert!(entity_history_matches_chain(&chains.chains, &real));
        assert!(chains.overflow || !chains.orphan_targets.is_empty());

        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin structural");
        let (_end, findings) = collect_structural(&mut session);
        // Ghost is not covered by ENTITIES → commit MissingCrossLink + orphan
        // CrossLinkMismatch. Real entity stays chain-clean (no entity MissingCrossLink
        // for real alone beyond commit graph).
        assert!(
            count_code(&findings, StructuralFindingCode::CrossLinkMismatch) >= 1,
            "orphan CrossLinkMismatch required: {:?}",
            finding_codes(&findings)
        );
        assert!(
            count_code(&findings, StructuralFindingCode::MissingCrossLink) >= 1,
            "commit reciprocal for ghost: {:?}",
            finding_codes(&findings)
        );
    }

    #[test]
    fn entity_chain_exact_count_masking_counterexample_reports_orphan() {
        // entities {E1,E2}, E2 never referenced, one commit references fabricated X
        // → len==2==count without the consumption/orphan fix would hide X.
        let path = TestDatabasePath::new("entity-masking");
        let store = initialized_store(&path, database_id(0x95));
        let bundle = stored_bundle("entity-history", 1, b"entity-history-bundle");
        let plan = ExecutablePlanRef::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            CommandId::first(),
            PlanHash::from_bytes([0x49; 32]),
        );
        let e1 = history_entity(1, EntityVersion::first(), b"e1", &bundle);
        let e2 = history_entity(2, EntityVersion::first(), b"e2", &bundle);
        let fabricated = history_entity(77, EntityVersion::first(), b"fab", &bundle);
        // Commit references E1 and fabricated X — not E2.
        let commit = history_commit(CommitSequence::first(), plan, &[&e1, &fabricated]);
        write_entities_and_commits(
            &store,
            database_id(0x95),
            &bundle,
            &[e1.clone(), e2.clone()],
            &[commit],
        );

        let transaction = store.shared.database.begin_read().expect("read");
        let chains = build_entity_chains(&transaction, 2, None).expect("chains");
        assert!(
            entity_history_matches_chain(&chains.chains, &e1),
            "E1 referenced must pass"
        );
        assert!(
            !entity_history_matches_chain(&chains.chains, &e2),
            "E2 never referenced must fail"
        );
        // fabricated occupies a chain slot and remains unconsumed.
        assert!(
            chains.chains.values().any(|c| !c.consumed)
                || !chains.orphan_targets.is_empty()
                || chains.chains.len() == 2,
            "fabricated target must be tracked"
        );
        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin structural");
        let (_end, findings) = collect_structural(&mut session);
        assert!(
            findings
                .iter()
                .any(|f| f.code() == StructuralFindingCode::MissingCrossLink),
            "E2 unreferenced: {findings:?}"
        );
        assert!(
            findings
                .iter()
                .any(|f| f.code() == StructuralFindingCode::CrossLinkMismatch),
            "fabricated orphan: {findings:?}"
        );
    }

    #[test]
    fn entity_chain_empty_entities_with_referencing_commit_reports_orphan() {
        let path = TestDatabasePath::new("entity-empty-ents");
        let store = initialized_store(&path, database_id(0x96));
        let bundle = stored_bundle("entity-history", 1, b"entity-history-bundle");
        let plan = ExecutablePlanRef::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            CommandId::first(),
            PlanHash::from_bytes([0x4a; 32]),
        );
        let ghost = history_entity(1, EntityVersion::first(), b"ghost", &bundle);
        let commit = history_commit(CommitSequence::first(), plan, &[&ghost]);
        // Zero ENTITIES rows; commit still references an entity.
        write_entities_and_commits(&store, database_id(0x96), &bundle, &[], &[commit]);

        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin structural");
        let (_end, findings) = collect_structural(&mut session);
        assert!(
            count_code(&findings, StructuralFindingCode::CrossLinkMismatch) >= 1,
            "empty ENTITIES must still report commit-referenced orphans: {:?}",
            finding_codes(&findings)
        );
    }

    #[test]
    fn entity_chain_missing_bundle_marks_row_not_orphan() {
        // NEW-2: commit-referenced entity with missing binding owns a chain;
        // row finding is MissingCrossLink and must NOT also orphan.
        let path = TestDatabasePath::new("entity-missing-bundle");
        let id = database_id(0x97);
        let store = initialized_store(&path, id);
        let bundle = stored_bundle("entity-history", 1, b"entity-history-bundle");
        let plan = ExecutablePlanRef::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            CommandId::first(),
            PlanHash::from_bytes([0x4b; 32]),
        );
        // Entity with a binding that does not exist in CONTRACT_BUNDLES, but is
        // commit-referenced so it owns a chain slot.
        let entity_type = EntityTypeId::new(7).expect("type");
        let mut key = EntityKeyBuilder::new(entity_type);
        key.push_u64(2).expect("key");
        let target = EntityTarget::new(entity_type, key.finish().expect("finish")).expect("target");
        let ghost_binding = DurableKeySchemaBindingV1::new(
            ContractLineage::new("missing-lineage-for-binding").expect("lineage"),
            ContractVersion::new(1).expect("version"),
            ContractBundleHash::from_bytes([0xee; 32]),
        );
        let field_id = FieldId::new(1).expect("field");
        let record = CanonicalRecord::new(vec![(
            field_id,
            CanonicalValue::bytes(b"orphan-check".to_vec()).expect("bytes"),
        )])
        .expect("fields");
        let missing_binding_entity = StoredEntityRecordV1::new(
            target,
            EntityVersion::first(),
            ContractVersion::new(1).expect("version"),
            ghost_binding,
            record,
        )
        .expect("entity");
        let commit = history_commit(CommitSequence::first(), plan, &[&missing_binding_entity]);
        write_entities_and_commits(
            &store,
            id,
            &bundle,
            std::slice::from_ref(&missing_binding_entity),
            &[commit],
        );

        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin structural");
        let (_end, findings) = collect_structural(&mut session);
        assert!(
            count_code(&findings, StructuralFindingCode::MissingCrossLink) >= 1,
            "missing binding must produce MissingCrossLink: {:?}",
            finding_codes(&findings)
        );
        assert_eq!(
            count_code(&findings, StructuralFindingCode::CrossLinkMismatch),
            0,
            "commit-referenced row with missing bundle must not orphan: {:?}",
            finding_codes(&findings)
        );
    }

    #[test]
    fn entity_orphan_drain_on_single_finishing_page() {
        // NEW-1(a): empty-ENTITIES orphan, limit=256, structural_total < 239 →
        // single finishing page executes the reserved orphan drain.
        let path = TestDatabasePath::new("entity-orphan-finish");
        let store = initialized_store(&path, database_id(0x98));
        let bundle = stored_bundle("entity-history", 1, b"entity-history-bundle");
        let plan = ExecutablePlanRef::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            CommandId::first(),
            PlanHash::from_bytes([0x4c; 32]),
        );
        let ghost = history_entity(1, EntityVersion::first(), b"ghost", &bundle);
        let commit = history_commit(CommitSequence::first(), plan, &[&ghost]);
        write_entities_and_commits(&store, database_id(0x98), &bundle, &[], &[commit]);

        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin structural");
        assert!(
            session.structural_total < 239,
            "fixture must be a single finishing page under limit=256 (total={})",
            session.structural_total
        );
        let large = EvidencePageLimit::new(256).expect("limit");
        let mut cursor =
            StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
        let mut collected = Vec::new();
        let mut advancing_pages = 0u32;
        while let StructuralEvidencePage::Page {
            start,
            findings,
            next,
        } = session
            .read_structural_evidence(cursor, large)
            .expect("finishing page must advance and drain orphans")
        {
            assert!(next.position() > start.position());
            advancing_pages += 1;
            collected.extend(findings);
            cursor = next;
        }
        assert_eq!(
            advancing_pages, 1,
            "expected exactly one finishing page for total < 239"
        );
        assert!(
            count_code(&collected, StructuralFindingCode::CrossLinkMismatch) >= 1,
            "orphan findings must drain on the finishing page: {:?}",
            finding_codes(&collected)
        );
    }

    #[test]
    fn entity_orphan_drain_after_reservation_clamp() {
        // NEW-1(b): structural_total in 240..=255 → reservation clamp splits the
        // would-be finishing page; follow-up page drains orphans.
        let path = TestDatabasePath::new("entity-orphan-clamp");
        let store = initialized_store(&path, database_id(0x99));
        let bundle = stored_bundle("entity-history", 1, b"entity-history-bundle");
        let plan = ExecutablePlanRef::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            CommandId::first(),
            PlanHash::from_bytes([0x4d; 32]),
        );
        let ghost = history_entity(1, EntityVersion::first(), b"ghost", &bundle);
        // Probe baseline size with one commit, then pad commits to land total
        // in 240..=255.
        write_entities_and_commits(
            &store,
            database_id(0x99),
            &bundle,
            &[],
            &[history_commit(
                CommitSequence::first(),
                plan.clone(),
                &[&ghost],
            )],
        );
        let baseline = {
            let session = store
                .begin_structural_evidence(inputs())
                .expect("baseline session");
            session.structural_total
        };
        // Re-open after the baseline session consumes the store handle.
        let store = RedbStore::open(&path.0).expect("reopen");
        // Each additional commit also adds outcome + provenance rows (+3 total).
        // Need structural_total in 240..=255.
        let target_total = 248u64;
        assert!(
            baseline < target_total,
            "baseline {baseline} already exceeds clamp window"
        );
        let extra_needed = target_total.saturating_sub(baseline);
        // Rough: each commit group adds ~3 rows; overshoot slightly then trim.
        let extra_commits = (extra_needed / 3) + 2;
        let mut commits = vec![history_commit(
            CommitSequence::first(),
            plan.clone(),
            &[&ghost],
        )];
        for seq in 2..=(1 + extra_commits) {
            commits.push(history_commit(
                CommitSequence::new(seq).expect("seq"),
                plan.clone(),
                &[&ghost],
            ));
        }
        // Rewrite fixture with padded commits.
        write_entities_and_commits(&store, database_id(0x99), &bundle, &[], &commits);
        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin structural");
        let total = session.structural_total;
        assert!(
            (240..=255).contains(&total),
            "need total in 240..=255 for clamp (got {total}; baseline was {baseline}, commits={})",
            commits.len()
        );

        let large = EvidencePageLimit::new(256).expect("limit");
        let mut cursor =
            StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
        let mut collected = Vec::new();
        let mut advancing_pages = 0u32;
        while let StructuralEvidencePage::Page {
            start,
            findings,
            next,
        } = session
            .read_structural_evidence(cursor, large)
            .expect("clamp + follow-up finishing page must complete")
        {
            assert!(next.position() > start.position());
            advancing_pages += 1;
            collected.extend(findings);
            cursor = next;
        }
        assert!(
            advancing_pages >= 2,
            "reservation clamp must force ≥2 advancing pages (got {advancing_pages}, total={total})"
        );
        assert!(
            count_code(&collected, StructuralFindingCode::CrossLinkMismatch) >= 1,
            "orphans must drain after clamp: {:?}",
            finding_codes(&collected)
        );
    }

    #[test]
    fn mark_entity_chain_consumed_is_logarithmic_not_quadratic() {
        // NEW-6: O(N log N) map lookup vs O(N²) full-map scan.
        // Reviewer measured quadratic mark alone at ~181ms @ 8k (release);
        // logarithmic marking of 8k entries is sub-millisecond even in debug.
        use std::time::Instant;
        let mut chains = std::collections::BTreeMap::new();
        let mut keys = Vec::new();
        const N: u64 = 8_000;
        for i in 1..=N {
            let entity_type = EntityTypeId::new(7).expect("type");
            let mut key = EntityKeyBuilder::new(entity_type);
            key.push_u64(i).expect("component");
            let entity_key = key.finish().expect("key");
            let target = EntityTarget::new(entity_type, entity_key.clone()).expect("target");
            chains.insert(
                target,
                EntityChain {
                    version: EntityVersion::first(),
                    hash: Some(riffdb_types::EntityRecordHash::from_bytes([0xab; 32])),
                    expected_bundle: None,
                    migration_cursor: 0,
                    intact: true,
                    consumed: false,
                    seeded: false,
                },
            );
            keys.push(entity_key);
        }
        let started = Instant::now();
        for key in &keys {
            mark_entity_chain_consumed(&mut chains, key);
        }
        let elapsed = started.elapsed();
        assert!(
            chains.values().all(|c| c.consumed),
            "every chain must be marked"
        );
        // Quadratic 8k was ~181ms release; allow 50ms debug headroom for log-time.
        assert!(
            elapsed.as_millis() < 50,
            "marking {N} chains took {elapsed:?} (expected O(N log N) ≪ 50ms)"
        );
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
            // Index rows present so lifecycle (not MissingCrossLink) is the defect.
            let mut index = write
                .open_table(AUDIT_BY_REQUEST)
                .expect("audit-by-request table");
            for sequence in [
                AdministrationSequence::first(),
                AdministrationSequence::new(2).expect("second sequence"),
            ] {
                let key = keys::encode_audit_by_request_key(request_id, sequence);
                let encoded = codec::encode_service_audit_request_index_v1(
                    riffdb_storage_api::StoredServiceAuditRequestIndexV1::new(request_id, sequence),
                )
                .expect("encode index");
                index
                    .insert(key.as_slice(), encoded.as_bytes())
                    .expect("insert index row");
            }
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

    /// Collects the linear historical plan as a full (order_key, evidence) sequence.
    /// Each order_key is checked against `historical_order_key` of the materialized
    /// evidence (self-oracle). Pre-deletion differential vs `select_next_historical`
    /// was equal on empty/mixed/plans fixtures before that oracle was removed.
    fn collect_historical_plan_sequence(
        path: &std::path::Path,
    ) -> Vec<(Vec<u8>, HistoricalSemanticEvidence)> {
        let store = RedbStore::open(path).expect("open");
        let transaction = store.shared.database.begin_read().expect("read");
        let validation = inputs_at(70);
        let plan = build_historical_evidence_plan(&transaction, &validation).expect("plan");
        plan.entries
            .into_iter()
            .map(|(key, locator)| {
                let evidence =
                    materialize_historical_evidence(&transaction, &locator).expect("materialize");
                assert_eq!(
                    key,
                    historical_order_key(&evidence),
                    "plan order_key must match materialized evidence"
                );
                (key, evidence)
            })
            .collect()
    }

    fn oracle_plan_ref(seed: u8) -> ExecutablePlanRef {
        ExecutablePlanRef::new(
            ContractLineage::new("oracle-app").expect("lineage"),
            ContractVersion::new(1).expect("version"),
            ContractBundleHash::from_bytes([seed; 32]),
            CommandId::new(u32::from(seed).max(1)).expect("command"),
            PlanHash::from_bytes([seed.wrapping_add(1); 32]),
        )
    }

    fn oracle_commit(sequence: CommitSequence, plan: ExecutablePlanRef) -> StoredCommitRecordV1 {
        let sequence_byte = u8::try_from(sequence.get()).expect("small sequence");
        let actor = AdmittedActorContext::new(
            ActorId::new("oracle-actor").expect("actor"),
            ActorKind::Human,
            TenantScope::Global,
            None,
        );
        let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
        partition.push_u64(1).expect("partition component");
        let partition_hash =
            hash_partition_key(partition.finish().expect("partition key").as_bytes());
        StoredCommitRecordV1::new(
            sequence,
            RequestId::from_bytes(uuid_bytes(sequence_byte.wrapping_add(0x20)))
                .expect("request ID"),
            plan,
            CanonicalInputHash::from_bytes([sequence_byte; 32]),
            actor,
            LogicalTime::new(Timestamp::new(i64::from(sequence_byte), 0).expect("timestamp")),
            partition_hash,
            Vec::new(),
            StoredReadDependenciesV1::from_live(
                &ReadDependencies::new(Vec::new()).expect("empty dependencies"),
            )
            .expect("stored dependencies"),
            Vec::new(),
            Vec::new(),
            DeclaredOutcome::new(
                OutcomeId::first(),
                CanonicalRecord::new(Vec::new()).expect("outcome fields"),
            )
            .expect("outcome"),
            ProvenanceId::from_bytes(uuid_bytes(sequence_byte.wrapping_add(0x40)))
                .expect("provenance ID"),
            Vec::new(),
            DurabilityMode::Sync,
        )
        .expect("stored commit")
    }

    fn populate_mixed_oracle_fixture(path: &TestDatabasePath, id: DatabaseId) {
        let store = initialized_store(path, id);
        let lineage = ContractLineage::new("oracle-app").expect("lineage");
        let bundles = (1..=4)
            .map(|version| {
                stored_bundle(
                    "oracle-app",
                    version,
                    format!("oracle-bundle-{version}").as_bytes(),
                )
            })
            .collect::<Vec<_>>();
        let active = ActiveCatalogPointerV1::from_bundle(&bundles[3]);
        let binding = DurableKeySchemaBindingV1::new(
            lineage.clone(),
            bundles[3].contract_version(),
            bundles[3].bundle_hash(),
        );
        let entities = (1..=5)
            .map(|value| {
                let entity_type = EntityTypeId::new(7).expect("entity type");
                let mut key = EntityKeyBuilder::new(entity_type);
                key.push_u64(value).expect("entity component");
                StoredEntityRecordV1::new(
                    EntityTarget::new(entity_type, key.finish().expect("entity key"))
                        .expect("target"),
                    EntityVersion::new(1).expect("entity version"),
                    bundles[3].contract_version(),
                    binding.clone(),
                    CanonicalRecord::new(Vec::new()).expect("fields"),
                )
                .expect("entity")
            })
            .collect::<Vec<_>>();
        let index_rows = (1..=6)
            .map(compiled_migration_legacy_row)
            .collect::<Vec<_>>();
        let capabilities = [
            active_capability(
                id,
                capability_id(0xa1),
                explicit_scope(&lineage, &[10, 11, 12]),
                20,
                0xb1,
            ),
            active_capability(
                id,
                capability_id(0xa2),
                explicit_scope(&lineage, &[20, 21]),
                20,
                0xb2,
            ),
        ];
        let commit = oracle_commit(CommitSequence::first(), oracle_plan_ref(0x31));
        let write = store
            .shared
            .database
            .begin_write()
            .expect("write oracle fixture");
        {
            let mut table = write.open_table(CONTRACT_BUNDLES).expect("bundles");
            for bundle in &bundles {
                let key =
                    keys::encode_contract_bundle_key(bundle.lineage(), bundle.contract_version())
                        .expect("bundle key");
                let encoded = codec::encode_contract_bundle_v1(bundle).expect("encode bundle");
                table
                    .insert(key.as_slice(), encoded.as_bytes())
                    .expect("insert bundle");
            }
        }
        {
            let mut table = write.open_table(CATALOG_ACTIVE).expect("active");
            let encoded = codec::encode_active_catalog_pointer_v1(&active).expect("encode active");
            table
                .insert(CATALOG_ACTIVE_KEY.as_slice(), encoded.as_bytes())
                .expect("insert active");
        }
        {
            let mut table = write.open_table(ENTITIES).expect("entities");
            for entity in &entities {
                let encoded = codec::encode_entity_record_v1(entity).expect("encode entity");
                table
                    .insert(entity.target().key().as_bytes(), encoded.as_bytes())
                    .expect("insert entity");
            }
        }
        {
            let mut table = write.open_table(SECONDARY_INDEXES).expect("indexes");
            for row in &index_rows {
                let encoded = riffdb_storage_api::encode_index_entry_v1_fixture(row)
                    .expect("encode V1 index")
                    .into_bytes();
                table
                    .insert(row.key().as_bytes(), encoded.as_slice())
                    .expect("insert index");
            }
        }
        {
            let mut table = write.open_table(CAPABILITIES).expect("capabilities");
            for capability in &capabilities {
                let key = keys::encode_capability_key(capability.capability_id());
                let encoded =
                    codec::encode_capability_record_v1(capability).expect("encode capability");
                table
                    .insert(key.as_slice(), encoded.as_bytes())
                    .expect("insert capability");
            }
        }
        {
            let mut table = write.open_table(COMMITS).expect("commits");
            let key = keys::encode_application_sequence_key(commit.commit_sequence());
            let encoded = codec::encode_commit_record_v1(&commit).expect("encode commit");
            table
                .insert(key.as_slice(), encoded.as_bytes())
                .expect("insert commit");
        }
        write.commit().expect("commit oracle fixture");
    }

    #[test]
    fn historical_plan_sequence_self_oracle_empty() {
        let path = TestDatabasePath::new("hist-oracle-empty");
        let mut store = RedbStore::open(&path.0).expect("open");
        store.initialize_database(database_id(0x81)).expect("init");
        drop(store);
        let sequence = collect_historical_plan_sequence(&path.0);
        assert_eq!(sequence.len(), 1, "empty DB emits only ActiveCatalog");
        assert!(matches!(
            sequence[0].1,
            HistoricalSemanticEvidence::ActiveCatalog(None)
        ));
    }

    #[test]
    fn historical_plan_sequence_self_oracle_mixed() {
        let path = TestDatabasePath::new("hist-oracle-mixed");
        populate_mixed_oracle_fixture(&path, database_id(0x82));
        let sequence = collect_historical_plan_sequence(&path.0);
        let bundles = sequence
            .iter()
            .filter(|(_, e)| matches!(e, HistoricalSemanticEvidence::Bundle(_)))
            .count();
        let entities = sequence
            .iter()
            .filter(|(_, e)| {
                matches!(
                    e,
                    HistoricalSemanticEvidence::PersistedKey(key)
                        if matches!(
                            key.key(),
                            riffdb_storage_api::IrOpaquePersistedKeyV1::Entity { .. }
                        )
                )
            })
            .count();
        let indexes = sequence
            .iter()
            .filter(|(_, e)| matches!(e, HistoricalSemanticEvidence::IndexMigrationRow(_)))
            .count();
        let partitions = sequence
            .iter()
            .filter(|(_, e)| matches!(e, HistoricalSemanticEvidence::CapabilityPartition(_)))
            .count();
        let plans = sequence
            .iter()
            .filter(|(_, e)| matches!(e, HistoricalSemanticEvidence::PlanReference(_)))
            .count();
        let active = sequence
            .iter()
            .filter(|(_, e)| matches!(e, HistoricalSemanticEvidence::ActiveCatalog(Some(_))))
            .count();
        assert!(bundles >= 4, "bundles={bundles}");
        assert!(entities >= 5, "entities={entities}");
        assert!(indexes >= 6, "indexes={indexes}");
        assert!(partitions >= 5, "capability partitions={partitions}");
        assert!(plans >= 1, "plans={plans}");
        assert_eq!(active, 1, "active catalog");
        assert!(
            sequence.len() > 4 + 5 + 6 + 5 + 1,
            "mixed sequence length {}",
            sequence.len()
        );
        // Strict total order on order_keys.
        assert!(
            sequence.windows(2).all(|pair| pair[0].0 < pair[1].0),
            "order_keys must be strictly increasing"
        );
    }

    #[test]
    fn historical_plan_sequence_self_oracle_commit_plans() {
        // Dedicated shape exercising collect_plan_locators via COMMITS rows.
        let path = TestDatabasePath::new("hist-oracle-plans");
        let id = database_id(0x83);
        let store = initialized_store(&path, id);
        let plans = [oracle_plan_ref(0x41), oracle_plan_ref(0x42)];
        let commits = [
            oracle_commit(CommitSequence::first(), plans[0].clone()),
            oracle_commit(CommitSequence::new(2).expect("seq"), plans[1].clone()),
        ];
        let write = store
            .shared
            .database
            .begin_write()
            .expect("write plan fixture");
        {
            let mut table = write.open_table(COMMITS).expect("commits");
            for commit in &commits {
                let key = keys::encode_application_sequence_key(commit.commit_sequence());
                let encoded = codec::encode_commit_record_v1(commit).expect("encode commit");
                table
                    .insert(key.as_slice(), encoded.as_bytes())
                    .expect("insert commit");
            }
        }
        write.commit().expect("commit plan fixture");
        drop(store);
        let sequence = collect_historical_plan_sequence(&path.0);
        let plan_count = sequence
            .iter()
            .filter(|(_, e)| matches!(e, HistoricalSemanticEvidence::PlanReference(_)))
            .count();
        assert_eq!(plan_count, 2, "two commit plan references");
    }

    #[test]
    fn service_audit_without_request_index_reports_missing_cross_link() {
        let path = TestDatabasePath::new("missing-audit-request-index");
        let store = initialized_store(&path, database_id(0x84));
        let record = StoredServiceAuditRecordV1::from_stored_parts(
            AdministrationSequence::first(),
            request_id(0x71),
            Timestamp::new(1, 0).expect("timestamp"),
            ServiceOperationV1::GetHealth,
            ServiceAuditPhaseV1::Denied,
            Some(audit_principal(0x72)),
            ServiceIngressKindV1::Grpc,
            ServiceAuditTargetsV1::empty(),
            None,
            ServiceAuditLinkV1::None,
        )
        .expect("standalone service audit");
        let encoded = codec::encode_administration_audit_record_v1(
            &StoredAdministrationAuditRecordV1::Service(record),
        )
        .expect("encode service audit");
        let allocator = codec::encode_administration_sequence_allocator_v1(
            AdministrationSequenceAllocator::next(
                AdministrationSequence::new(2).expect("allocator sequence"),
            ),
        )
        .expect("encode allocator");
        let write = store
            .shared
            .database
            .begin_write()
            .expect("write corrupt fixture");
        {
            let mut audit = write.open_table(AUDIT).expect("audit table");
            audit
                .insert(
                    keys::encode_audit_key(AdministrationSequence::first()).as_slice(),
                    encoded.as_bytes(),
                )
                .expect("insert service audit without index peer");
        }
        {
            let mut meta = write.open_table(META).expect("metadata table");
            meta.insert(META_ADMINISTRATION_SEQUENCE, allocator.as_bytes())
                .expect("advance allocator");
        }
        // Deliberately omit AUDIT_BY_REQUEST — reciprocity must fail closed.
        write.commit().expect("commit corrupt fixture");

        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin evidence");
        let (_, findings) = collect_structural(&mut session);
        assert!(
            findings.iter().any(|finding| {
                finding.scope() == StructuralFindingScope::Authoritative
                    && finding.code() == StructuralFindingCode::MissingCrossLink
            }),
            "expected MissingCrossLink for Service audit without AUDIT_BY_REQUEST peer; got {findings:?}"
        );
    }
}
