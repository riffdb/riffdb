//! Exclusive read-only startup evidence over one immutable redb snapshot.

mod index_migration_backend;
#[path = "startup_v3_activation.rs"]
mod v3_activation;
pub(crate) use v3_activation::PendingV3Activation;

pub use index_migration_backend::RedbStartupIndexMigrationPort;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use redb::{
    MultimapTableHandle, Range, ReadOnlyTable, ReadTransaction, ReadableDatabase, ReadableTable,
    ReadableTableMetadata, TableDefinition, TableHandle,
};
use riffdb_catalog::{CatalogHistoryOutcome, ValidatedContractBundle, validate_catalog_history};
use riffdb_storage_api::{
    ApplicationSequenceAllocator, CapabilityLifecycleV1, DormantPortBundle, EvidencePageLimit,
    HistoricalActiveCatalogEvidence, HistoricalBundleBytes, HistoricalBundleEvidence,
    HistoricalCapabilityPartitionEvidenceV1, HistoricalEvidenceCursor, HistoricalEvidenceEnd,
    HistoricalEvidencePage, HistoricalPersistedKeyEvidenceV1, HistoricalSemanticEvidence,
    IndexMigrationCursor, MAX_RETAINED_QUERY_MODULES, OpenSessionId,
    ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
    RetainedMetadataV1, StartupIndexMigrationPort, StartupValidationInputs, StorageError,
    StorageErrorKind, StorageValueError, StructuralEvidenceCursor, StructuralEvidenceEnd,
    StructuralEvidenceOpen, StructuralEvidencePage, StructuralEvidenceSession, StructuralFinding,
    StructuralFindingCode, StructuralFindingScope, StructuralOpenOutcome, StructurallyOpened,
    UniqueIndexTarget, UniqueOccupancyKind,
};
use riffdb_types::{
    CommitSequence, ContractBundleHash, ContractLineage, ContractMigrationOperationId,
    ContractVersion, DatabaseId, DigestKeyId, ENTITY_KEY_V1_PREFIX, EntityTypeId, FieldId,
    FrontierPosition, INDEX_ENTRY_KEY_V1_PREFIX, MigrationBundleHash, Timestamp,
    hash_contract_bundle, hash_query_module, hash_reactive_module, hash_reactive_source,
};

use crate::codec::{self, IdempotencyRecordV1};
use crate::command_authority::{
    command_authority_head, command_member_at, commit_at, commits_in_physical_row,
    physical_scan_start,
};
use crate::error::{precommit_storage_error, storage_error, table_error, transaction_error};
use crate::gate::ExclusiveLease;
use crate::keys;
use crate::layout::{
    APPLICATION_EXPORT_OPERATIONS, APPLICATION_INSTALLATION_CAMPAIGNS, AUDIT, AUDIT_BY_REQUEST,
    CAPABILITIES, CAPABILITY_TOKENS, CATALOG_ACTIVE, CATALOG_ACTIVE_KEY, COMMITS, CONTRACT_BUNDLES,
    CONTRACT_MIGRATION_JOURNAL, CONTRACT_MIGRATIONS, CONTRACT_WRITE_RETIREMENTS, ENTITIES,
    EVENT_CONSUMER_DELIVERIES, EVENT_CONSUMERS, EVENT_ROUTES, EVENTS, HISTORY_TOMBSTONES,
    IDEMPOTENCY, IDEMPOTENCY_PENDING, INDEX_EPOCHS, META, META_ADMINISTRATION_SEQUENCE,
    META_APPLICATION_SEQUENCE, META_CAPABILITY_BOOTSTRAP, META_CHANGELOG_V2_ROTATION_RECEIPT,
    META_CLEAN_CLOSE_LIFECYCLE, META_DATABASE_ID, META_FORMAT_VERSION, META_HISTORY_INCARNATION,
    META_INDEX_EPOCH_ROWS_REPAIRED, META_KEYS, META_RECORD_REGISTRY, META_RETENTION_HOLDS,
    META_RETENTION_WATERMARK, META_VALIDATED_PREFIX_CHECKPOINT, OUTBOX, OUTBOX_STATUS,
    PROJECTION_APPLIED, PROJECTION_FRONTIER, PROJECTION_STATE, PROVENANCE, QUERY_MODULE_ACTIVE,
    QUERY_MODULES, REACTIVE_MODULES, RETIRED_ENTITIES, SECONDARY_INDEXES, TABLE_NAMES,
    VECTOR_EVIDENCE, VECTOR_EVIDENCE_INDEX, VECTOR_OBSERVATIONS,
};
use crate::store::{
    PRE_APPLICATION_EXPORT_REGISTRY_DIGEST, PRE_APPLICATION_INSTALLATION_REGISTRY_DIGEST,
    PRE_AUDIT_REQUEST_INDEX_REGISTRY_DIGEST, PRE_CONTRACT_MIGRATION_REGISTRY_DIGEST,
    PRE_ENTITY_REFERENCE_REGISTRY_DIGEST, PRE_EVENT_ROUTE_REGISTRY_DIGEST,
    PRE_HISTORY_INCARNATION_REGISTRY_DIGEST, PRE_INDEX_GENERATION_REGISTRY_DIGEST,
    PRE_RETENTION_WATERMARK_REGISTRY_DIGEST, PRE_VALIDATED_PREFIX_CHECKPOINT_REGISTRY_DIGEST,
    PRE_VECTOR_EVIDENCE_REGISTRY_DIGEST, PRE_VECTOR_HEALTH_OBSERVATION_REGISTRY_DIGEST,
    PRE_VECTOR_OBSERVATION_REGISTRY_DIGEST, PRE_WP417_REACTIVE_CONSUMER_REGISTRY_DIGEST,
    RedbDormantPorts, RedbStore, SharedRedb,
};

static NEXT_OPEN_SESSION: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EvidenceOpenPurpose {
    Startup,
    OfflineIntegrityScrub,
}

/// One fixed-cardinality successful offline integrity-scrub receipt.
///
/// The receipt deliberately carries neither database identity nor population,
/// path, frontier, hash, table, key, or application-value observations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code)] // Sealed until the authorized WP-760 maintenance owner calls it.
pub(crate) struct OfflineIntegrityScrubReceiptV1 {
    _private: (),
}

#[allow(dead_code)]
impl OfflineIntegrityScrubReceiptV1 {
    /// Closed receipt version.
    #[must_use]
    pub(crate) const fn receipt_version(self) -> u32 {
        1
    }

    /// Confirms that the structural stream reached its same-session exact end.
    #[must_use]
    pub(crate) const fn structural_exact_end(self) -> bool {
        true
    }

    /// Confirms that catalog validation consumed the historical exact end.
    #[must_use]
    pub(crate) const fn catalog_exact_end(self) -> bool {
        true
    }

    /// A scrub is read-only with respect to application authority.
    #[must_use]
    pub(crate) const fn authoritative_mutations(self) -> u8 {
        0
    }

    /// A scrub neither consumes nor creates clean-close lifecycle state.
    #[must_use]
    pub(crate) const fn lifecycle_mutations(self) -> u8 {
        0
    }
}

/// Exclusive offline complete-integrity scrub over one database file.
///
/// Binding does no I/O. `run` requires the engine's exclusive file ownership,
/// ignores clean-certificate and validated-prefix shortcuts, consumes both
/// evidence streams through exact end, and publishes a receipt only on total
/// structural and catalog-semantic success.
pub(crate) struct RedbOfflineIntegrityScrub {
    path: PathBuf,
    inputs: StartupValidationInputs,
}

impl RedbOfflineIntegrityScrub {
    pub(crate) fn from_inputs(path: &Path, inputs: StartupValidationInputs) -> Self {
        Self {
            path: path.to_path_buf(),
            inputs,
        }
    }

    /// Binds an operator-selected closed database and the configured readable
    /// digest-key inventory used by ordinary production startup.
    #[allow(dead_code)] // WP-760 owns the explicit operator-facing caller.
    pub(crate) fn bind(
        path: &Path,
        authorization_time: Timestamp,
        capability_key_ids: Vec<DigestKeyId>,
        idempotency_key_ids: Vec<DigestKeyId>,
    ) -> Result<Self, StorageError> {
        let capability = ReadableCapabilityDigestInventory::new(
            capability_key_ids
                .into_iter()
                .map(ReadableDigestKey::v1)
                .collect(),
        )
        .map_err(value_error_as_storage)?;
        let idempotency = ReadableIdempotencyDigestInventory::new(
            idempotency_key_ids
                .into_iter()
                .map(ReadableDigestKey::v1)
                .collect(),
        )
        .map_err(value_error_as_storage)?;
        Ok(Self::from_inputs(
            path,
            StartupValidationInputs::new(authorization_time, capability, idempotency),
        ))
    }

    /// Runs the unchanged complete structural and catalog-semantic path.
    pub(crate) fn run(self) -> Result<OfflineIntegrityScrubReceiptV1, StorageError> {
        let store = RedbStore::open(&self.path)?;
        let mut session = store.begin_structural_evidence_for(
            self.inputs,
            EvidenceOpenPurpose::OfflineIntegrityScrub,
        )?;
        let database_id = session.database_id();
        let open_session_id = session.open_session_id();
        let limit = EvidencePageLimit::new(64).ok_or_else(limit_exceeded)?;
        let mut cursor = StructuralEvidenceCursor::start(database_id, open_session_id);
        let structural_end = loop {
            match session.read_structural_evidence(cursor, limit)? {
                StructuralEvidencePage::Page { findings, next, .. } => {
                    if !findings.is_empty() {
                        return Err(storage_error(StorageErrorKind::CorruptData));
                    }
                    cursor = next;
                }
                StructuralEvidencePage::ExactEnd(end) => break end,
            }
        };
        let validation = validate_catalog_history(&mut session).map_err(|error| {
            storage_error(
                error
                    .storage_kind()
                    .unwrap_or(StorageErrorKind::CorruptData),
            )
        })?;
        let (catalog, historical_end) = validation.into_parts();
        if !matches!(catalog, CatalogHistoryOutcome::Ready(_)) {
            return Err(storage_error(StorageErrorKind::IncompatibleFormat));
        }
        session.finish_offline_integrity_scrub(structural_end, historical_end)?;
        Ok(OfflineIntegrityScrubReceiptV1 { _private: () })
    }
}
// The V1 validated-prefix checkpoint permanently covers the original table set.
// Additive tables are validated separately and never inferred from that proof.
const STRUCTURAL_TABLE_COUNT: usize = 28;
const ADDITIVE_STRUCTURAL_TABLE_COUNT: usize = 8;
const STARTUP_TABLE_COUNT: usize = STRUCTURAL_TABLE_COUNT + ADDITIVE_STRUCTURAL_TABLE_COUNT;

fn startup_registry_is_supported(digest: riffdb_types::SchemaHash) -> bool {
    digest == riffdb_storage_api::proto_codec::current_record_registry_digest()
        || digest == crate::changelog_v3_activation::PRE_V3_REGISTRY
        || digest == riffdb_types::SchemaHash::from_bytes(PRE_VECTOR_EVIDENCE_REGISTRY_DIGEST)
        || digest == riffdb_types::SchemaHash::from_bytes(PRE_VECTOR_OBSERVATION_REGISTRY_DIGEST)
        || digest
            == riffdb_types::SchemaHash::from_bytes(PRE_VECTOR_HEALTH_OBSERVATION_REGISTRY_DIGEST)
        || digest == riffdb_types::SchemaHash::from_bytes(PRE_EVENT_ROUTE_REGISTRY_DIGEST)
        || digest == riffdb_types::SchemaHash::from_bytes(PRE_ENTITY_REFERENCE_REGISTRY_DIGEST)
        || digest == riffdb_types::SchemaHash::from_bytes(PRE_CONTRACT_MIGRATION_REGISTRY_DIGEST)
        || digest
            == riffdb_types::SchemaHash::from_bytes(PRE_VALIDATED_PREFIX_CHECKPOINT_REGISTRY_DIGEST)
        || digest == riffdb_types::SchemaHash::from_bytes(PRE_RETENTION_WATERMARK_REGISTRY_DIGEST)
        || digest == riffdb_types::SchemaHash::from_bytes(PRE_AUDIT_REQUEST_INDEX_REGISTRY_DIGEST)
        || digest == riffdb_types::SchemaHash::from_bytes(PRE_HISTORY_INCARNATION_REGISTRY_DIGEST)
        || digest == riffdb_types::SchemaHash::from_bytes(PRE_INDEX_GENERATION_REGISTRY_DIGEST)
        || digest
            == riffdb_types::SchemaHash::from_bytes(PRE_WP417_REACTIVE_CONSUMER_REGISTRY_DIGEST)
        || digest
            == riffdb_types::SchemaHash::from_bytes(PRE_APPLICATION_INSTALLATION_REGISTRY_DIGEST)
        || digest == riffdb_types::SchemaHash::from_bytes(PRE_APPLICATION_EXPORT_REGISTRY_DIGEST)
}

/// Resolves active vector-field SLOs for the predecessor observation
/// migration. Catalog interpretation remains confined to startup; the store
/// receives only checked stable identities and thresholds.
pub(crate) type ActiveVectorSpecsForMigration =
    Option<(ContractLineage, BTreeMap<(EntityTypeId, FieldId), u64>)>;

pub(crate) fn active_vector_specs_for_migration(
    shared: &SharedRedb,
) -> Result<ActiveVectorSpecsForMigration, StorageError> {
    let transaction = shared.database.begin_read().map_err(transaction_error)?;
    let active_table = transaction
        .open_table(CATALOG_ACTIVE)
        .map_err(table_error)?;
    let Some(active_bytes) = active_table
        .get(CATALOG_ACTIVE_KEY.as_slice())
        .map_err(precommit_storage_error)?
    else {
        return Ok(None);
    };
    let active = codec::decode_active_catalog_pointer_v1(active_bytes.value())?
        .into_parts()
        .0;
    let bundle_key = keys::encode_contract_bundle_key(active.lineage(), active.contract_version())
        .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
    let bundles = transaction
        .open_table(CONTRACT_BUNDLES)
        .map_err(table_error)?;
    let bundle_bytes = bundles
        .get(bundle_key.as_slice())
        .map_err(precommit_storage_error)?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    let stored = codec::decode_contract_bundle_v1(bundle_bytes.value())?
        .into_parts()
        .0;
    let validated = ValidatedContractBundle::from_stored(&stored)
        .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
    let specs = validated
        .bundle()
        .schema()
        .vector_field_specs()
        .iter()
        .map(|spec| {
            (
                (spec.entity(), spec.field()),
                spec.stale_entity_count_threshold(),
            )
        })
        .collect();
    Ok(Some((active.lineage().clone(), specs)))
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

/// Persistent forward-only structural table cursors (linear scan).
struct StructuralCursors {
    /// Table phase: 0 = META, 1.. = BYTE_TABLES[phase-1] / structural phase+1 for inspect.
    phase: usize,
    consumed_in_phase: u64,
    /// Next logical command expected while walking physical COMMITS rows.
    next_commit_sequence: Option<CommitSequence>,
    commit_sequence_initialized: bool,
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
    CapabilityPartition {
        capability_id: riffdb_types::CapabilityId,
        entry_ordinal: usize,
    },
}

impl EvidenceLocator {
    fn retained_bytes(&self) -> usize {
        match self {
            Self::Bundle(key) => key.len().saturating_add(8),
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

#[derive(Clone, Copy, Default)]
struct PersistedEvidenceGroupSources {
    entities: bool,
    index_rows: bool,
    legacy_epochs: bool,
    partition_epochs: bool,
}

impl PersistedEvidenceGroupSources {
    const fn retained_bytes(self) -> usize {
        4
    }
}

struct PersistedEvidenceGroup {
    /// Complete historical order-key prefix through the bounded raw-key length.
    /// Every row in this group therefore has table order equal to evidence order.
    order_prefix: Vec<u8>,
    sources: PersistedEvidenceGroupSources,
}

struct PendingHistoricalEvidence {
    order_key: Vec<u8>,
    evidence: HistoricalSemanticEvidence,
}

enum PersistedEvidenceGroupCursor {
    Entities {
        after: Option<Vec<u8>>,
        exhausted: bool,
        prior: Option<Vec<u8>>,
        buffered: VecDeque<PendingHistoricalEvidence>,
    },
    IndexKeys {
        after_index: Option<Vec<u8>>,
        index_exhausted: bool,
        after_epoch: Option<Vec<u8>>,
        epoch_exhausted: bool,
        pending_index: Option<Box<PendingHistoricalEvidence>>,
        pending_epoch: Option<Box<PendingHistoricalEvidence>>,
        prior_index: Option<Vec<u8>>,
        prior_epoch: Option<Vec<u8>>,
        buffered_index: VecDeque<PendingHistoricalEvidence>,
        buffered_epoch: VecDeque<PendingHistoricalEvidence>,
    },
    PartitionEpochs {
        after: Option<Vec<u8>>,
        exhausted: bool,
        prior: Option<Vec<u8>>,
        buffered: VecDeque<PendingHistoricalEvidence>,
    },
}

struct HistoricalEvidencePlan {
    /// Catalog-shaped entries preceding the 0x04 persisted-key domain.
    prefix_entries: Vec<(Vec<u8>, EvidenceLocator)>,
    next_prefix: usize,
    /// Bounded schema/kind/owner/key-length groups. Row locators are never kept.
    persisted_groups: Vec<PersistedEvidenceGroup>,
    next_group: usize,
    group_cursor: Option<PersistedEvidenceGroupCursor>,
    /// Capability-partition entries following the 0x04 domain.
    suffix_entries: Vec<(Vec<u8>, EvidenceLocator)>,
    next_suffix: usize,
    /// One page-deferred item; semantic page ceilings never lose a consumed row.
    pending: Option<PendingHistoricalEvidence>,
    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "startup-plan test observability")
    )]
    retained_bytes: usize,
}

#[derive(Clone)]
struct CachedCommandAudit {
    commit_sequence: CommitSequence,
    member: riffdb_storage_api::StoredCommandAuditMemberV1,
    record: riffdb_storage_api::StoredServiceAuditRecordV1,
    peer_sequence: riffdb_types::AdministrationSequence,
}

/// The small publication subset of the administration stream, decoded by one
/// exact forward pass and shared by every startup reciprocity check.
///
/// Command and ordinary service audits are deliberately not retained. Catalog,
/// query-module, and reactive-module publications are rare control-plane facts;
/// retaining only those facts avoids repeatedly decoding the entire mixed audit
/// stream while keeping the cache bounded by the startup evidence byte limit.
struct PublicationAuditCache {
    catalogs: Vec<riffdb_storage_api::StoredCatalogAdministrationV1>,
    query_modules: Vec<riffdb_storage_api::StoredQueryModuleAdministrationV1>,
    reactive_modules: BTreeSet<riffdb_types::ReactiveModuleHash>,
    decoded_rows: u64,
    valid: bool,
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
    /// Exact inactive predecessor; complete validation must precede migration.
    v3_activation_required: bool,
    /// True only for an ADR-0157 verified clean-close bounded startup.
    clean_close_fast: bool,
    /// Exact clean lifecycle consumed to DIRTY immediately before activation.
    verified_clean_lifecycle: Option<crate::clean_close::CleanCloseLifecycle>,
    /// Which precondition declined the ADR-0157 bounded path, if it declined.
    /// `None` exactly when `clean_close_fast` is true.
    clean_close_declined_reason: Option<crate::clean_close::CleanCloseDeclineReason>,
    /// Walk plan counts (full or suffix-adjusted under a verified checkpoint).
    structural_counts: [u64; STRUCTURAL_TABLE_COUNT],
    /// Full table lens from the immutable snapshot (for below-S count verification).
    full_structural_counts: [u64; STRUCTURAL_TABLE_COUNT],
    /// Additive table counts never covered by the V1 validated-prefix checkpoint.
    additive_structural_counts: [u64; ADDITIVE_STRUCTURAL_TABLE_COUNT],
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
    /// The one immutable snapshot pin owned by the validation session.
    ///
    /// Structural and historical validation share this snapshot while the
    /// exclusive mutation lease is retained. Historical materialization does
    /// not reopen and recheck every table for every key, and the session never
    /// owns overlapping read transactions.
    validation_read: Option<ReadTransaction>,
    historical_tables: Option<HistoricalMaterializationTables>,
    structural_cursors: Option<StructuralCursors>,
    /// Exact command-audit members derived once from validated segments. This
    /// avoids decoding an entire segment again for each audit locator.
    command_audits:
        std::collections::BTreeMap<riffdb_types::AdministrationSequence, CachedCommandAudit>,
    command_audit_bytes: usize,
    command_capsules:
        std::collections::BTreeMap<CommitSequence, riffdb_storage_api::StoredCommandCapsuleV1>,
    /// Commands whose complete authority graph was decoded from one physical
    /// segment/capsule row during the cache's exact forward `COMMITS` pass.
    embedded_command_authority: BTreeSet<CommitSequence>,
    command_cache_built: bool,
    /// Publication facts decoded once from AUDIT and shared by catalog, query
    /// module, reactive module, and suffix-audit validation.
    publication_audits: Option<PublicationAuditCache>,
    /// Exact valid retained bundles collected once from the pinned startup
    /// snapshot. Entity and index validation consult this bounded set instead
    /// of reopening and decoding `CONTRACT_BUNDLES` for every retained row.
    binding_bundles: BTreeSet<riffdb_storage_api::DurableKeySchemaBindingV1>,
    /// Built at ENTITIES phase entry; dropped after orphan findings are queued.
    entity_chains: Option<EntityChainState>,
    /// Non-command `AUDIT` rows decoded for publication proofs this session.
    /// One whole physical pass serves every catalog, query-module, and reactive
    /// module check; the counter excludes command locators skipped by dispatch.
    reactive_publication_audit_decodes: u64,
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
    /// Terminal `ExecutionFailed` rows seen in the IDEMPOTENCY phase — the whole
    /// table on both the full walk and the checkpoint fast path, which classifies
    /// every row of that table either way. Seeds the process census that lets a
    /// checkpoint's materialized-StoredOutcome-only `idempotency_count` be
    /// derived from redb's IDEMPOTENCY row count instead of a full pass.
    terminal_execution_failure_rows: u64,
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

impl StructuralEvidenceOpen for RedbStore {
    type Session = RedbStructuralEvidenceSession;

    fn begin_structural_evidence(
        self,
        inputs: StartupValidationInputs,
    ) -> Result<Self::Session, StorageError> {
        self.begin_structural_evidence_for(inputs, EvidenceOpenPurpose::Startup)
    }
}

impl RedbStore {
    fn begin_structural_evidence_for(
        self,
        inputs: StartupValidationInputs,
        purpose: EvidenceOpenPurpose,
    ) -> Result<RedbStructuralEvidenceSession, StorageError> {
        let lease = self.acquire_mutation_lease()?;
        let durable_commit_epoch = self.shared.durable_commit_epoch();
        let transaction = self
            .shared
            .database
            .begin_read()
            .map_err(transaction_error)?;
        let snapshot = collect_startup_snapshot(&transaction)?;
        let v3_active = crate::changelog_v3_roots::read_checkpoint_roots(&transaction)?.is_some();
        if self.shared.durable_commit_epoch() != durable_commit_epoch {
            return Err(corrupt());
        }
        let database_id = snapshot.retained_metadata.database_id();
        let open_session_id = allocate_open_session()?;
        let full_structural_counts = snapshot.structural_counts;
        let additive_structural_counts = snapshot.additive_structural_counts;
        let mut structural_counts = snapshot.structural_counts;
        let mut structural_total = snapshot.structural_total;
        let verified_clean_lifecycle = self.shared.verified_clean_close_lifecycle(
            &transaction,
            database_id,
            snapshot.retained_metadata.history_incarnation(),
        )?;
        let clean_close_declined_reason = verified_clean_lifecycle.declined();
        if purpose == EvidenceOpenPurpose::Startup
            && v3_active
            && let Some(lifecycle) = verified_clean_lifecycle.verified()
        {
            // The binding verifier already checked every fixed meta/catalog
            // root plus every active query-module pointer and body. The
            // remaining startup proof walks only the bounded contract catalog;
            // population rows are validated locally when read.
            self.shared.set_startup_validation_clean(false);
            structural_counts = [0; STRUCTURAL_TABLE_COUNT];
            structural_counts[0] = full_structural_counts[0];
            structural_counts[1] = full_structural_counts[1];
            structural_counts[2] = full_structural_counts[2];
            let additive_structural_counts = [0; ADDITIVE_STRUCTURAL_TABLE_COUNT];
            structural_total = structural_counts.iter().try_fold(1_u64, |total, count| {
                total.checked_add(*count).ok_or_else(limit_exceeded)
            })?;
            let binding_bundles = collect_bundle_bindings(&transaction)?;
            return Ok(RedbStructuralEvidenceSession {
                shared: Arc::clone(&self.shared),
                lease: Some(lease),
                durable_commit_epoch,
                database_id,
                open_session_id,
                retained_metadata: snapshot.retained_metadata,
                inputs,
                v3_activation_required: false,
                clean_close_fast: true,
                verified_clean_lifecycle: Some(lifecycle),
                clean_close_declined_reason: None,
                structural_counts,
                full_structural_counts,
                additive_structural_counts,
                structural_total,
                next_structural: StructuralEvidenceCursor::start(database_id, open_session_id),
                next_historical: HistoricalEvidenceCursor::start(database_id, open_session_id),
                last_historical_key: None,
                structural_finished: false,
                historical_finished: false,
                authoritative_finding_seen: false,
                any_finding_seen: false,
                saw_v1_index: false,
                validation_read: Some(transaction),
                historical_tables: None,
                structural_cursors: None,
                command_audits: BTreeMap::new(),
                command_audit_bytes: 0,
                command_capsules: BTreeMap::new(),
                embedded_command_authority: BTreeSet::new(),
                command_cache_built: false,
                publication_audits: None,
                binding_bundles,
                entity_chains: None,
                reactive_publication_audit_decodes: 0,
                pending_entity_orphan_findings: VecDeque::new(),
                historical_plan: None,
                checkpoint: None,
                sampled_window_rows_inspected: 0,
                checkpoint_verified: false,
                checkpoint_ignored_reason: None,
                walked_suffix_counts: [0; STRUCTURAL_TABLE_COUNT],
                walked_prefix_counts: [0; STRUCTURAL_TABLE_COUNT],
                terminal_execution_failure_rows: 0,
            });
        }
        // The CLEAN branch above checks bounded terminal roots only. A full
        // session instead validates every retained receipt from THIS pin before
        // any prefix checkpoint or DIRTY write can follow successful evidence.
        if v3_active {
            crate::changelog_v3_roots::validate_retained_history(&transaction)?
                .ok_or_else(corrupt)?;
        }
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
        if purpose == EvidenceOpenPurpose::Startup {
            self.shared.set_startup_validation_clean(false);
        }
        let binding_bundles = collect_bundle_bindings(&transaction)?;
        // The deterministic prefix sample and the full structural session both
        // need publication reciprocity. Build their one bounded control-plane
        // cache here so checkpoint sampling cannot add a second whole AUDIT
        // pass before the header consumes the same evidence.
        let publication_audits = build_publication_audit_cache(&transaction)?;
        let reactive_publication_audit_decodes = publication_audits.decoded_rows;
        let mut checkpoint = None;
        let mut checkpoint_verified = false;
        let mut checkpoint_ignored_reason = None;
        let mut sampled_window_rows_inspected = 0_u64;
        let mut sample_finding_seen = false;
        let active_checkpoint = if purpose == EvidenceOpenPurpose::Startup && v3_active {
            crate::validated_prefix::load_active_checkpoint(
                &transaction,
                database_id,
                snapshot.retained_metadata.history_incarnation(),
                &full_structural_counts,
            )
        } else {
            Err(crate::validated_prefix::CheckpointIgnoreReason::Absent)
        };
        match active_checkpoint {
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
                    &binding_bundles,
                    &active.checkpoint_hash,
                    active.checkpoint_commit_sequence,
                    active.audit_sequence_bound,
                    &publication_audits,
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
                    structural_total = structural_counts
                        .iter()
                        .chain(additive_structural_counts.iter())
                        .try_fold(1u64, |total, count| {
                            total.checked_add(*count).ok_or_else(limit_exceeded)
                        })?;
                    checkpoint = Some(active);
                    checkpoint_verified = true;
                }
            }
            Err(reason) if purpose == EvidenceOpenPurpose::Startup => {
                // Fail-closed → full validation; count and expose the reason so
                // lost fast paths (including migration-driven fingerprint
                // mismatches) are visible, never silent.
                self.shared.note_checkpoint_ignored(reason);
                checkpoint_ignored_reason = Some(reason);
            }
            Err(_) => {}
        }
        Ok(RedbStructuralEvidenceSession {
            shared: Arc::clone(&self.shared),
            lease: Some(lease),
            durable_commit_epoch,
            database_id,
            open_session_id,
            retained_metadata: snapshot.retained_metadata,
            inputs,
            v3_activation_required: !v3_active,
            clean_close_fast: false,
            verified_clean_lifecycle: None,
            clean_close_declined_reason,
            structural_counts,
            full_structural_counts,
            additive_structural_counts,
            structural_total,
            next_structural: StructuralEvidenceCursor::start(database_id, open_session_id),
            next_historical: HistoricalEvidenceCursor::start(database_id, open_session_id),
            last_historical_key: None,
            structural_finished: false,
            historical_finished: false,
            authoritative_finding_seen: false,
            any_finding_seen: sample_finding_seen,
            saw_v1_index: false,
            validation_read: Some(transaction),
            historical_tables: None,
            structural_cursors: None,
            command_audits: std::collections::BTreeMap::new(),
            command_audit_bytes: 0,
            command_capsules: std::collections::BTreeMap::new(),
            embedded_command_authority: BTreeSet::new(),
            command_cache_built: false,
            publication_audits: Some(publication_audits),
            binding_bundles,
            entity_chains: None,
            reactive_publication_audit_decodes,
            pending_entity_orphan_findings: VecDeque::new(),
            historical_plan: None,
            checkpoint,
            sampled_window_rows_inspected,
            checkpoint_verified,
            checkpoint_ignored_reason,
            walked_suffix_counts: [0; STRUCTURAL_TABLE_COUNT],
            walked_prefix_counts: [0; STRUCTURAL_TABLE_COUNT],
            terminal_execution_failure_rows: 0,
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
            self.publication_audits = None;
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
        // Structural pin is released once structural finishes; historical
        // installs its own immutable pin for the rest of the startup session.
        self.ensure_historical_read()?;
        if self.historical_plan.is_none() {
            let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
            let plan = if self.clean_close_fast {
                build_clean_close_historical_evidence_plan(transaction)?
            } else {
                build_historical_evidence_plan(transaction, &self.inputs)?
            };
            self.historical_plan = Some(plan);
        }
        let requested = usize::try_from(limit.get()).map_err(|_| limit_exceeded())?;
        let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
        let materialization_tables = self.historical_tables.as_ref().ok_or_else(invariant)?;
        let mut evidence = Vec::new();
        let mut bytes = 0usize;
        let mut migration_rows = 0usize;
        let mut migration_evidence_bytes = 0usize;
        let mut migration_instruction_bytes = 0usize;
        let mut last_key = self.last_historical_key.clone();
        let plan = self.historical_plan.as_mut().ok_or_else(invariant)?;
        let mut exhausted = false;
        while evidence.len() < requested {
            let Some(next_item) = plan.next(transaction, materialization_tables)? else {
                exhausted = true;
                break;
            };
            let PendingHistoricalEvidence {
                order_key,
                evidence: item,
            } = next_item;
            if let Some(prior) = last_key.as_ref() {
                if prior > &order_key {
                    return Err(invariant());
                }
                if prior == &order_key {
                    // Preserve the old BTreeMap's order-key deduplication.
                    continue;
                }
            }
            let next_bytes = bytes
                .checked_add(historical_semantic_bytes(&item)?)
                .ok_or_else(limit_exceeded)?;
            if next_bytes > riffdb_storage_api::MAX_HISTORICAL_EVIDENCE_PAGE_BYTES {
                if evidence.is_empty() {
                    return Err(limit_exceeded());
                }
                plan.pending = Some(PendingHistoricalEvidence {
                    order_key,
                    evidence: item,
                });
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
                    plan.pending = Some(PendingHistoricalEvidence {
                        order_key,
                        evidence: item,
                    });
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
        }
        if evidence.is_empty() {
            if !exhausted {
                return Err(invariant());
            }
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
        self.ensure_historical_read()?;
        read_historical_bundle_from_table(
            &self
                .historical_tables
                .as_ref()
                .ok_or_else(invariant)?
                .bundles,
            lineage,
            version,
            hash,
        )
    }

    fn read_integrity_entity(
        &mut self,
        target: &riffdb_storage_api::EntityTarget,
    ) -> Result<Option<riffdb_storage_api::StoredEntityRecordV1>, StorageError> {
        self.ensure_historical_read()?;
        let tables = self.historical_tables.as_ref().ok_or_else(invariant)?;
        crate::reads::read_entity_record(&tables.entities, target)
    }

    fn read_integrity_unique_occupancy(
        &mut self,
        target: &UniqueIndexTarget,
    ) -> Result<UniqueOccupancyKind, StorageError> {
        self.ensure_historical_read()?;
        let table = &self
            .historical_tables
            .as_ref()
            .ok_or_else(invariant)?
            .indexes;
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
        self.historical_tables = None;
        self.validation_read = None;
        drop(self.open_snapshot_read()?);
        let lease = self.lease.take().ok_or_else(invariant)?;
        if self.saw_v1_index {
            Ok(StructuralOpenOutcome::MigrationRequired(
                RedbStartupIndexMigrationPort::new(
                    Arc::clone(&self.shared),
                    lease,
                    self.database_id,
                    self.open_session_id,
                ),
            ))
        } else {
            if self.v3_activation_required {
                if self.clean_close_fast || self.checkpoint_verified || self.any_finding_seen {
                    return Err(corrupt());
                }
                let pending = PendingV3Activation::from_finished_session(
                    lease,
                    self.durable_commit_epoch,
                    self.retained_metadata.clone(),
                    self.terminal_execution_failure_rows,
                );
                return Ok(StructuralOpenOutcome::Clean(
                    StructurallyOpened::from_finished_session(
                        self.database_id,
                        self.open_session_id,
                        self.retained_metadata,
                        RedbDormantPorts {
                            shared: Arc::clone(&self.shared),
                            pending_v3_activation: Some(pending),
                        },
                        RedbCompletionAuthority { _private: () },
                    ),
                ));
            }
            // ADR-0019 A1 write gate: the checkpoint is written ONLY when this
            // validation session produced ZERO findings of ANY scope. A finding
            // of any severity (authoritative, outbox, projection, sampled)
            // vetoes the write so the fast path can never silence it.
            if !self.clean_close_fast && !self.any_finding_seen {
                // Seed the terminal execution-failure census from THIS walk
                // before the gate opens. The census and the write permission are
                // established by the same statement pair, so no checkpoint can
                // ever be built from an unseeded census; the IDEMPOTENCY phase
                // classified every row of the table, on the full walk and on the
                // fast path alike, so the seed is exact for this durable head.
                self.shared
                    .seed_terminal_execution_failure_rows(self.terminal_execution_failure_rows);
                self.shared.set_startup_validation_clean(true);
                let retained = self.retained_metadata.clone();
                if crate::validated_prefix::write_validated_prefix_checkpoint(
                    &self.shared,
                    &retained,
                    crate::validated_prefix::CheckpointPurpose::StartupValidation,
                )
                .is_err()
                {
                    // ADR-0019 A1: a failed checkpoint write after clean
                    // validation is non-fatal — it costs only the next open's
                    // fast path. Counted for operator visibility.
                    self.shared.note_checkpoint_write_failure();
                }
            }
            self.shared.advance_dirty_lifecycle_before_activation(
                self.database_id,
                self.retained_metadata.history_incarnation(),
                self.verified_clean_lifecycle.take(),
            )?;
            self.shared
                .bounded_clean_startup
                .store(self.clean_close_fast, Ordering::Release);
            drop(lease);
            Ok(StructuralOpenOutcome::Clean(
                StructurallyOpened::from_finished_session(
                    self.database_id,
                    self.open_session_id,
                    self.retained_metadata,
                    RedbDormantPorts {
                        shared: Arc::clone(&self.shared),
                        pending_v3_activation: None,
                    },
                    RedbCompletionAuthority { _private: () },
                ),
            ))
        }
    }
}

impl RedbStructuralEvidenceSession {
    fn finish_offline_integrity_scrub(
        mut self,
        structural_end: RedbStructuralEvidenceEnd,
        historical_end: RedbHistoricalEvidenceEnd,
    ) -> Result<(), StorageError> {
        if self.clean_close_fast
            || self.checkpoint_verified
            || !self.structural_finished
            || !self.historical_finished
            || structural_end.cursor != self.next_structural
            || historical_end.cursor != self.next_historical
            || self.authoritative_finding_seen
            || self.any_finding_seen
            || self.saw_v1_index
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        self.historical_tables = None;
        self.validation_read = None;
        drop(self.open_snapshot_read()?);
        let lease = self.lease.take().ok_or_else(invariant)?;
        drop(lease);
        Ok(())
    }

    fn ensure_historical_read(&mut self) -> Result<(), StorageError> {
        if self.validation_read.is_none() {
            self.validation_read = Some(self.open_snapshot_read()?);
        }
        if self.historical_tables.is_none() {
            let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
            self.historical_tables = Some(HistoricalMaterializationTables::open(transaction)?);
        }
        if self.historical_tables.is_none() {
            return Err(invariant());
        }
        Ok(())
    }

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
        let Some(transaction) = self.validation_read.as_ref() else {
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
        if self.clean_close_fast {
            // Redb table lengths are metadata reads. They prove the immutable
            // snapshot did not change without walking any population table.
            return Ok(());
        }
        if self.checkpoint.is_none() && counts != self.structural_counts {
            return Err(corrupt());
        }
        let additive_counts = [
            table_len(transaction, REACTIVE_MODULES)?,
            table_len(transaction, EVENT_CONSUMERS)?,
            table_len(transaction, EVENT_CONSUMER_DELIVERIES)?,
            table_len(transaction, APPLICATION_INSTALLATION_CAMPAIGNS)?,
            table_len(transaction, APPLICATION_EXPORT_OPERATIONS)?,
            table_len(transaction, VECTOR_EVIDENCE)?,
            table_len(transaction, VECTOR_OBSERVATIONS)?,
            table_len(transaction, VECTOR_EVIDENCE_INDEX)?,
        ];
        if additive_counts != self.additive_structural_counts {
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
    ///
    /// # Recovering a refusal
    ///
    /// A refusal here does not require restore-from-backup. `RetentionMaintenance::prune_to`
    /// (`crate::retention`, `prune_to`) opens the database file directly — no validated
    /// startup — and its FIRST exclusive transaction unconditionally deletes
    /// `META_VALIDATED_PREFIX_CHECKPOINT`. Running the offline retention prune
    /// therefore removes the disputed checkpoint, after which the next open finds
    /// none and runs full validation, which re-derives every count from its own
    /// walk. That is the operator lever for a count divergence, whatever produced
    /// it — a drifted process census, or rows added or lost below S while the
    /// database was closed.
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

    /// Test/benchmark observability: whether a verified clean-close lifecycle
    /// selected the bounded readiness path. This is observation only and cannot
    /// select startup behavior.
    #[doc(hidden)]
    #[must_use]
    pub fn clean_close_fast_path(&self) -> bool {
        self.clean_close_fast
    }

    /// Test/operator observability: which precondition declined the ADR-0157
    /// bounded path on this open, or `None` when the bounded path was admitted.
    ///
    /// Answers "why is this start slow?" from the session itself, before the
    /// complete validation pass runs. Nine preconditions previously shared one
    /// observable — a very slow start — which is how this cost was misdiagnosed
    /// twice. This is observation only and cannot select startup behavior.
    #[doc(hidden)]
    #[must_use]
    pub fn clean_close_declined_reason(&self) -> Option<&'static str> {
        self.clean_close_declined_reason
            .map(crate::clean_close::CleanCloseDeclineReason::as_str)
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

    /// Test/benchmark observability: AUDIT rows decoded to witness reactive
    /// publications. This is the non-command row count from the one physical
    /// `AUDIT` pass shared by all publication checks, never one pass per row.
    #[doc(hidden)]
    #[must_use]
    pub fn reactive_publication_audit_decodes(&self) -> u64 {
        self.reactive_publication_audit_decodes
    }

    /// Test observability: terminal `ExecutionFailed` rows this walk censused.
    #[doc(hidden)]
    #[must_use]
    pub fn terminal_execution_failure_rows(&self) -> u64 {
        self.terminal_execution_failure_rows
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
    binding_bundles: &BTreeSet<riffdb_storage_api::DurableKeySchemaBindingV1>,
    checkpoint_hash: &[u8; 32],
    s: u64,
    audit_bound: u64,
    publication_audits: &PublicationAuditCache,
) -> Result<(u64, Option<StructuralFinding>), StorageError> {
    use std::ops::Bound::{Included, Unbounded};

    let mut inspected = 0_u64;
    // The sample is strictly bounded. Resolve only the sampled command members
    // through their owning segments instead of decoding and retaining the
    // complete command history before inspecting a handful of windows.
    let sampled_command_capsules = std::collections::BTreeMap::new();
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
            let commits = transaction.open_table(COMMITS).map_err(table_error)?;
            let Some(lower_key) = physical_scan_start(&commits, lower_seq)? else {
                continue;
            };
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
                let command_capsules =
                    match riffdb_storage_api::decode_command_segment_v1(value.value()) {
                        Ok(segment) => segment
                            .value()
                            .commands()
                            .iter()
                            .map(|command| (command.commit_sequence(), command.base().clone()))
                            .collect(),
                        // The authoritative commit-row inspector below owns all
                        // malformed-record classification. A corrupt historical
                        // non-segment must not make this segment-only cache abort
                        // the evidence walk before that typed finding is emitted.
                        Err(_) => std::collections::BTreeMap::new(),
                    };
                let embedded_command_authority =
                    command_capsules.keys().copied().collect::<BTreeSet<_>>();
                let (finding, _) = inspect_commit_row(
                    transaction,
                    &command_capsules,
                    &embedded_command_authority,
                    binding_bundles,
                    seq,
                    key.value(),
                    value.value(),
                )?;
                if let Some(finding) = finding {
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
                if let Some(finding) = inspect_event_row(
                    transaction,
                    &sampled_command_capsules,
                    key.value(),
                    value.value(),
                )? {
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
                let finding = inspect_audit_row(
                    transaction,
                    publication_audits,
                    index,
                    key.value(),
                    value.value(),
                )?;
                if let Some(finding) = finding {
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
                let sequence = CommitSequence::new(s).ok_or_else(invariant)?;
                let key = keys::encode_application_sequence_key(sequence);
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
                    riffdb_types::AdministrationSequence::new(audit_bound).ok_or_else(invariant)?,
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
            if self.clean_close_fast {
                return Ok(None);
            }
            // Allocator continuity and the later command/audit phases need the
            // same exact command graph. Build it once (suffix-only under a
            // validated checkpoint) and share it instead of decoding every
            // command segment again in the header proof.
            self.ensure_command_cache()?;
            self.ensure_publication_audit_cache()?;
            let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
            return inspect_header(
                transaction,
                self.database_id,
                self.checkpoint.as_ref(),
                &self.command_audits,
                self.publication_audits.as_ref().ok_or_else(invariant)?,
            );
        }
        if self.structural_cursors.is_none() {
            self.structural_cursors = Some(StructuralCursors {
                phase: 0,
                consumed_in_phase: 0,
                next_commit_sequence: None,
                commit_sequence_initialized: false,
                meta: None,
                bytes: None,
            });
        }
        let (phase, index, key, value) = self.next_structural_row_raw()?;
        // Sanity: absolute position maps to this phase/index.
        let mut relative = position - 1;
        let mut expected_phase = None;
        for (phase_idx, count) in self
            .structural_counts
            .iter()
            .chain(self.additive_structural_counts.iter())
            .copied()
            .enumerate()
        {
            if relative < count {
                expected_phase = Some(phase_idx);
                break;
            }
            relative = relative.checked_sub(count).ok_or_else(invariant)?;
        }
        if Some(phase) != expected_phase || index != relative {
            return Err(invariant());
        }
        if phase == 0 {
            let key = std::str::from_utf8(&key).map_err(|_| corrupt())?;
            return Ok(inspect_meta_row(key, &value, self.database_id));
        }
        if self.clean_close_fast && phase == 1 {
            return inspect_clean_close_bundle_row(&key, &value);
        }
        if self.clean_close_fast && phase == 2 {
            return inspect_clean_close_active_catalog_row(
                self.validation_read.as_ref().ok_or_else(invariant)?,
                &key,
                &value,
            );
        }
        if phase == 5 {
            return self.inspect_entity_row_with_chains(&key, &value);
        }
        if phase == 28 {
            return self.inspect_reactive_module_row_with_witness(&key, &value);
        }
        if phase == 8 {
            return self.inspect_terminal_row_with_census(&key, &value);
        }
        if let Some(checkpoint) = self.checkpoint.as_ref()
            && crate::validated_prefix::skip_inspect_for_prefix_row(
                phase,
                &key,
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
        if phase == 10 {
            // Successful outcomes may be segment-owned with no physical
            // IDEMPOTENCY row, so phase 8 is not guaranteed to build this
            // reciprocal cache before the command-authority walk.
            self.ensure_command_cache()?;
            self.walked_suffix_counts[phase] = self.walked_suffix_counts[phase].saturating_add(1);
            let needs_initial = !self
                .structural_cursors
                .as_ref()
                .ok_or_else(invariant)?
                .commit_sequence_initialized;
            if needs_initial {
                let retained_floor = self.checkpoint.as_ref().map_or_else(
                    || {
                        self.validation_read
                            .as_ref()
                            .ok_or_else(invariant)
                            .and_then(retention_watermark_sequence)
                    },
                    |checkpoint| Ok(checkpoint.checkpoint_commit_sequence),
                )?;
                let expected = retained_floor.checked_add(1).and_then(CommitSequence::new);
                let cursors = self.structural_cursors.as_mut().ok_or_else(invariant)?;
                cursors.next_commit_sequence = expected;
                cursors.commit_sequence_initialized = true;
            }
            let expected = self
                .structural_cursors
                .as_ref()
                .ok_or_else(invariant)?
                .next_commit_sequence;
            let Some(expected) = expected else {
                return Ok(Some(authoritative(
                    StructuralFindingCode::SequenceDiscontinuity,
                )));
            };
            let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
            let (finding, last) = inspect_commit_row(
                transaction,
                &self.command_capsules,
                &self.embedded_command_authority,
                &self.binding_bundles,
                expected,
                &key,
                &value,
            )?;
            self.structural_cursors
                .as_mut()
                .ok_or_else(invariant)?
                .next_commit_sequence = last.and_then(CommitSequence::checked_next);
            return Ok(finding);
        }
        if phase == 11 {
            let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
            return inspect_provenance_row(transaction, &self.command_capsules, &key, &value);
        }
        if phase == 12 {
            let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
            return inspect_event_row(transaction, &self.command_capsules, &key, &value);
        }
        if phase == 13 {
            let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
            return inspect_event_route_row(transaction, &self.command_capsules, &key, &value);
        }
        if phase == 14 {
            let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
            return inspect_outbox_row(transaction, &self.command_capsules, &key, &value);
        }
        if phase == 21 {
            self.ensure_command_cache()?;
        }
        if phase == 21
            && let Some(finding) = inspect_cached_command_audit_row(
                self.validation_read.as_ref().ok_or_else(invariant)?,
                &self.command_audits,
                &self.embedded_command_authority,
                keys::decode_audit_key(&key)
                    .map(|sequence| sequence.get().saturating_sub(1))
                    .unwrap_or(index),
                &key,
                &value,
            )?
        {
            self.walked_suffix_counts[phase] = self.walked_suffix_counts[phase].saturating_add(1);
            return Ok(finding);
        }
        let inspect_index = match phase {
            21 => keys::decode_audit_key(&key)
                .map(|sequence| sequence.get().saturating_sub(1))
                .unwrap_or(index),
            _ => index,
        };
        if crate::validated_prefix::is_range_skipped_phase(phase) {
            self.walked_suffix_counts[phase] = self.walked_suffix_counts[phase].saturating_add(1);
        }
        let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
        let context = StructuralInspectContext {
            inputs: &self.inputs,
            binding_bundles: &self.binding_bundles,
            publication_audits: self.publication_audits.as_ref().ok_or_else(invariant)?,
            database_id: self.database_id,
        };
        let finding = inspect_table_row_from_bytes(
            transaction,
            &context,
            phase,
            inspect_index,
            &key,
            &value,
        )?;
        Ok(finding)
    }

    fn ensure_command_cache(&mut self) -> Result<(), StorageError> {
        if self.command_cache_built {
            return Ok(());
        }
        let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
        let commits = transaction.open_table(COMMITS).map_err(table_error)?;
        let events = transaction.open_table(EVENTS).map_err(table_error)?;
        let mut capsules = std::collections::BTreeMap::new();
        let mut audits = std::collections::BTreeMap::new();
        let mut embedded_authority = BTreeSet::new();
        let mut retained_bytes = 0usize;
        let checkpoint_sequence = self
            .checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.checkpoint_commit_sequence)
            .unwrap_or(0);
        let mut rows = if checkpoint_sequence == 0 {
            commits.iter().map_err(precommit_storage_error)?
        } else {
            let checkpoint_key = keys::encode_application_sequence_key(
                CommitSequence::new(checkpoint_sequence).ok_or_else(invariant)?,
            );
            commits
                .range::<&[u8]>((Excluded(checkpoint_key.as_slice()), Unbounded))
                .map_err(precommit_storage_error)?
        };
        for entry in &mut rows {
            let (key, value) = entry.map_err(precommit_storage_error)?;
            retained_bytes = retained_bytes
                .checked_add(value.value().len())
                .ok_or_else(limit_exceeded)?;
            if retained_bytes > MAX_STARTUP_EVIDENCE_INDEX_BYTES {
                return Err(limit_exceeded());
            }
            let physical =
                keys::decode_application_sequence_key(key.value()).map_err(|_| corrupt())?;
            let (commands, embedded) =
                match riffdb_storage_api::decode_command_segment_v1(value.value()) {
                    Ok(segment) => {
                        let segment = segment.into_parts().0;
                        if segment.first_commit_sequence() != physical {
                            return Err(corrupt());
                        }
                        (
                            segment
                                .commands()
                                .iter()
                                .map(|command| command.base().clone())
                                .collect::<Vec<_>>(),
                            true,
                        )
                    }
                    Err(error)
                        if error.kind()
                            == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
                    {
                        match command_member_at(&commits, &events, physical) {
                            Ok(Some(command)) => {
                                let embedded = matches!(
                                    &command,
                                    crate::command_authority::CommandAuthorityMember::CapsuleV2(_)
                                );
                                (vec![command.into_base()], embedded)
                            }
                            Ok(None) => (Vec::new(), false),
                            Err(error) if error.kind() == StorageErrorKind::CorruptData => {
                                (Vec::new(), false)
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    // The physical commit phase owns malformed-row reporting. Do
                    // not let this auxiliary cache turn that expected finding into
                    // a storage-level abort.
                    Err(_) => (Vec::new(), false),
                };
            for command in commands {
                let sequence = command.commit_sequence();
                if embedded && !embedded_authority.insert(sequence) {
                    return Err(corrupt());
                }
                let started = command.started_audit();
                let terminal = command.terminal_audit();
                for (member, record, peer_sequence) in [
                    (
                        riffdb_storage_api::StoredCommandAuditMemberV1::Started,
                        started,
                        terminal.administration_sequence(),
                    ),
                    (
                        riffdb_storage_api::StoredCommandAuditMemberV1::Terminal,
                        terminal,
                        started.administration_sequence(),
                    ),
                ] {
                    if audits
                        .insert(
                            record.administration_sequence(),
                            CachedCommandAudit {
                                commit_sequence: sequence,
                                member,
                                record: record.clone(),
                                peer_sequence,
                            },
                        )
                        .is_some()
                    {
                        return Err(corrupt());
                    }
                }
                if capsules.insert(sequence, command).is_some() {
                    return Err(corrupt());
                }
            }
        }
        self.command_audit_bytes = retained_bytes;
        self.command_audits = audits;
        self.command_capsules = capsules;
        self.embedded_command_authority = embedded_authority;
        self.command_cache_built = true;
        Ok(())
    }

    fn ensure_publication_audit_cache(&mut self) -> Result<(), StorageError> {
        if self.publication_audits.is_some() {
            return Ok(());
        }
        let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
        let cache = build_publication_audit_cache(transaction)?;
        self.reactive_publication_audit_decodes = cache.decoded_rows;
        self.publication_audits = Some(cache);
        Ok(())
    }

    fn ensure_entity_chains_built(&mut self) -> Result<(), StorageError> {
        if self.entity_chains.is_some() {
            return Ok(());
        }
        let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
        let live_entity_count = self.full_structural_counts.get(5).copied().unwrap_or(0);
        let chain_head_count = transaction
            .open_table(crate::layout::ENTITY_CHAIN_HEADS)
            .map_err(table_error)?
            .len()
            .map_err(precommit_storage_error)?;
        let entity_identity_count = live_entity_count.max(chain_head_count);
        // Under a verified checkpoint the genesis COMMITS walk is replaced by
        // seeding from the fingerprint-verified exact entity heads at S and
        // advancing across the suffix only. The seed map is consumed exactly
        // once; a second build attempt under a checkpoint is an invariant error.
        let seed = match self.checkpoint.as_mut() {
            Some(checkpoint) => Some((
                checkpoint.checkpoint_commit_sequence,
                checkpoint.entity_heads_at_s.take().ok_or_else(invariant)?,
            )),
            None => None,
        };
        self.entity_chains = Some(build_entity_chains(
            transaction,
            entity_identity_count,
            seed,
        )?);
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

    /// Judges one retained `REACTIVE_MODULES` row against the session's shared
    /// publication evidence, built once before header validation.
    fn inspect_reactive_module_row_with_witness(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<StructuralFinding>, StorageError> {
        let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
        inspect_reactive_module_row_cached(
            transaction,
            self.publication_audits.as_ref().ok_or_else(invariant)?,
            key,
            value,
        )
    }

    /// Inspects one IDEMPOTENCY row and censuses its terminal class.
    ///
    /// Dispatched here rather than from `inspect_table_row_from_bytes` because
    /// the checkpointed prefix skip and the terminal execution-failure census are
    /// two questions about one decode: classifying twice would add a second full
    /// protobuf pass over the largest-decode table at every open. The skip
    /// predicate is unchanged — `TerminalRowClass::is_prefix_outcome` is the same
    /// test `skip_inspect_for_prefix_row` spelled out for phase 8 — and the
    /// findings this phase produces are unchanged.
    ///
    /// The census counts EVERY `ExecutionFailed` row in the table on both paths:
    /// the skip admits only `StoredOutcome` rows at or below S, so no execution
    /// failure is ever skipped, on the fast path or the full walk.
    fn inspect_terminal_row_with_census(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<StructuralFinding>, StorageError> {
        let class = crate::validated_prefix::classify_terminal_row(value);
        if class == crate::validated_prefix::TerminalRowClass::Failed {
            self.terminal_execution_failure_rows =
                self.terminal_execution_failure_rows.saturating_add(1);
        }
        if let Some(checkpoint) = self.checkpoint.as_ref()
            && class.is_prefix_outcome(checkpoint.checkpoint_commit_sequence)
        {
            // Counting skip-walk: every classified prefix row is tallied and
            // verified against the recorded count at ExactEnd, so a vanished
            // below-S row in this full-walk table still fails closed.
            self.walked_prefix_counts[8] = self.walked_prefix_counts[8].saturating_add(1);
            return Ok(None);
        }
        // Only a suffix outcome or an execution-failure row needs the complete
        // command-authority graph. Building it before the checkpoint test makes
        // every clean restart decode and clone the entire retained command
        // history merely to skip already-proved prefix outcomes.
        self.ensure_command_cache()?;
        let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
        let finding = inspect_terminal_row(
            transaction,
            &self.command_capsules,
            &self.inputs,
            self.database_id,
            key,
            value,
        )?;
        Ok(finding)
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
        let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
        if !self.binding_bundles.contains(record.schema_binding()) {
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
            if phase >= STARTUP_TABLE_COUNT {
                return Err(invariant());
            }
            let configured_count = self
                .structural_counts
                .iter()
                .chain(self.additive_structural_counts.iter())
                .nth(phase)
                .copied()
                .ok_or_else(invariant)?;
            if self.clean_close_fast && configured_count == 0 {
                let cursors = self.structural_cursors.as_mut().ok_or_else(invariant)?;
                cursors.meta = None;
                cursors.bytes = None;
                cursors.phase = cursors.phase.checked_add(1).ok_or_else(invariant)?;
                cursors.consumed_in_phase = 0;
                continue;
            }
            if phase == 0 {
                let need_open = self
                    .structural_cursors
                    .as_ref()
                    .ok_or_else(invariant)?
                    .meta
                    .is_none();
                if need_open {
                    let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
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
                let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
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
                    28 => transaction
                        .open_table(REACTIVE_MODULES)
                        .map_err(table_error)?,
                    29 => transaction
                        .open_table(EVENT_CONSUMERS)
                        .map_err(table_error)?,
                    30 => transaction
                        .open_table(EVENT_CONSUMER_DELIVERIES)
                        .map_err(table_error)?,
                    31 => transaction
                        .open_table(APPLICATION_INSTALLATION_CAMPAIGNS)
                        .map_err(table_error)?,
                    32 => transaction
                        .open_table(APPLICATION_EXPORT_OPERATIONS)
                        .map_err(table_error)?,
                    33 => transaction
                        .open_table(VECTOR_EVIDENCE)
                        .map_err(table_error)?,
                    34 => transaction
                        .open_table(VECTOR_OBSERVATIONS)
                        .map_err(table_error)?,
                    35 => transaction
                        .open_table(VECTOR_EVIDENCE_INDEX)
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

struct StructuralInspectContext<'a> {
    inputs: &'a StartupValidationInputs,
    binding_bundles: &'a BTreeSet<riffdb_storage_api::DurableKeySchemaBindingV1>,
    publication_audits: &'a PublicationAuditCache,
    database_id: DatabaseId,
}

fn inspect_table_row_from_bytes(
    transaction: &ReadTransaction,
    context: &StructuralInspectContext<'_>,
    phase: usize,
    index: u64,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    match phase {
        1 => inspect_bundle_row(transaction, context.publication_audits, key, value),
        2 => inspect_active_row(transaction, context.publication_audits, key, value),
        3 => inspect_query_module_row(transaction, context.publication_audits, key, value),
        4 => inspect_active_query_module_row(transaction, context.publication_audits, key, value),
        // Phase 5 (ENTITIES) is handled exclusively by
        // `inspect_entity_row_with_chains` in `inspect_structural_forward`.
        6 => inspect_index_row(context.binding_bundles, key, value),
        7 => inspect_epoch_row(context.binding_bundles, key, value),
        // Phase 8 (IDEMPOTENCY) is handled exclusively by
        // `inspect_terminal_row_with_census` in `inspect_structural_forward`,
        // which owns the single terminal-class decode.
        9 => inspect_pending_row(transaction, context.inputs, context.database_id, key, value),
        10 => Err(invariant()),
        11 => Err(invariant()),
        12..=14 => Err(invariant()),
        15 => inspect_outbox_status_row(transaction, key, value),
        16 => inspect_projection_state_row(transaction, key, value),
        17 => inspect_projection_control_row(transaction, key, value),
        18 => inspect_projection_apply_row(transaction, key, value),
        19 => inspect_capability_row(transaction, context.inputs, context.database_id, key, value),
        20 => inspect_capability_lookup_row(transaction, key, value),
        21 => inspect_audit_row(transaction, context.publication_audits, index, key, value),
        22 => inspect_audit_by_request_row(transaction, key, value),
        23 => inspect_contract_migration_journal_row(key, value),
        24 => inspect_contract_migration_record_row(key, value),
        25 => inspect_contract_write_retirement_row(key, value),
        26 => inspect_retired_entity_row(transaction, key, value),
        27 => inspect_history_tombstone_row(key, value),
        // Phase 28 (REACTIVE_MODULES) is handled exclusively by
        // `inspect_reactive_module_row_with_witness` in
        // `inspect_structural_forward`, which owns the one-pass witness set.
        29 => inspect_event_consumer_row(transaction, context.database_id, key, value),
        30 => inspect_event_consumer_delivery_row(transaction, key, value),
        31 => inspect_application_installation_campaign_row(key, value),
        32 => inspect_application_export_operation_row(key, value),
        33 => inspect_vector_evidence_row(transaction, key, value),
        34 => inspect_vector_observation_row(transaction, key, value),
        35 => inspect_vector_evidence_index_row(transaction, key, value),
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

fn inspect_application_installation_campaign_row(
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let decoded =
        riffdb_storage_api::proto_codec::decode_application_installation_campaign_v1(value)
            .map_err(crate::error::codec_error)?;
    if key != decoded.value().campaign_id().as_bytes() {
        return Err(corrupt());
    }
    Ok(None)
}

fn inspect_application_export_operation_row(
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let decoded = riffdb_storage_api::decode_application_export_operation_v1(value)
        .map_err(crate::error::codec_error)?;
    if key != decoded.value().operation_id().as_bytes() {
        return Err(corrupt());
    }
    Ok(None)
}

fn inspect_vector_evidence_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let (entity_key, field) = keys::decode_vector_evidence_key(key).map_err(|_| corrupt())?;
    let decoded =
        riffdb_storage_api::decode_vector_evidence_v1(value).map_err(crate::error::codec_error)?;
    let evidence = decoded.value();
    if evidence.target().key() != &entity_key || evidence.vector_field() != field {
        return Err(corrupt());
    }
    let index_target = riffdb_storage_api::VectorObservationTargetV1::new(
        evidence.schema_binding().lineage().clone(),
        evidence.partition_key().clone(),
        evidence.target().entity_type_id(),
        evidence.vector_field(),
    );
    let index_key = keys::encode_vector_evidence_index_key(&index_target, &entity_key)
        .map_err(|_| corrupt())?;
    let index = transaction
        .open_table(VECTOR_EVIDENCE_INDEX)
        .map_err(table_error)?;
    let Some(index_bytes) = index
        .get(index_key.as_slice())
        .map_err(precommit_storage_error)?
    else {
        return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
    };
    let index_entry = riffdb_storage_api::decode_vector_evidence_index_v1(index_bytes.value())
        .map_err(crate::error::codec_error)?;
    if !index_entry.value().matches_evidence(evidence) {
        return Ok(Some(authoritative(
            StructuralFindingCode::CrossLinkMismatch,
        )));
    }
    let entities = transaction.open_table(ENTITIES).map_err(table_error)?;
    let Some(entity_bytes) = entities
        .get(entity_key.as_bytes())
        .map_err(precommit_storage_error)?
    else {
        return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
    };
    let entity = riffdb_storage_api::decode_entity_record_v1(entity_bytes.value())
        .map_err(crate::error::codec_error)?;
    let entity = entity.value();
    let vector_value = entity
        .fields()
        .fields()
        .binary_search_by_key(&field, |(field, _)| *field)
        .ok()
        .map(|position| &entity.fields().fields()[position].1);
    let embedding_matches = matches!(
        (vector_value, evidence.embedding_write()),
        (Some(riffdb_types::CanonicalValue::Vector(_)), Some(_))
            | (Some(riffdb_types::CanonicalValue::Null), None)
    );
    if entity.target() != evidence.target()
        || entity.entity_version() != evidence.entity_version()
        || !embedding_matches
    {
        return Ok(Some(authoritative(
            StructuralFindingCode::CrossLinkMismatch,
        )));
    }
    Ok(None)
}

fn inspect_vector_evidence_index_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let (target, entity_key) =
        keys::decode_vector_evidence_index_key(key).map_err(|_| corrupt())?;
    let decoded = riffdb_storage_api::decode_vector_evidence_index_v1(value)
        .map_err(crate::error::codec_error)?;
    let index = decoded.value();
    if index.target() != &target || index.entity_key() != &entity_key {
        return Err(corrupt());
    }
    let evidence_key = keys::encode_vector_evidence_key(&entity_key, target.vector_field())
        .map_err(|_| corrupt())?;
    let evidence_table = transaction
        .open_table(VECTOR_EVIDENCE)
        .map_err(table_error)?;
    let Some(evidence_bytes) = evidence_table
        .get(evidence_key.as_slice())
        .map_err(precommit_storage_error)?
    else {
        return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
    };
    let evidence = riffdb_storage_api::decode_vector_evidence_v1(evidence_bytes.value())
        .map_err(crate::error::codec_error)?;
    if !index.matches_evidence(evidence.value()) {
        return Ok(Some(authoritative(
            StructuralFindingCode::CrossLinkMismatch,
        )));
    }
    Ok(None)
}

fn inspect_vector_observation_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    if let Ok(lineage) = keys::decode_vector_health_observation_key(key) {
        let decoded = riffdb_storage_api::decode_vector_health_observation_v1(value)
            .map_err(crate::error::codec_error)?;
        let health = decoded.value();
        if health.lineage() != &lineage {
            return Err(corrupt());
        }
        let mut expected = health
            .fields()
            .map(|field| {
                (
                    (field.entity_type(), field.vector_field()),
                    (field.stale_entity_count_threshold(), 0_u64, 0_u64),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let table = transaction
            .open_table(VECTOR_OBSERVATIONS)
            .map_err(table_error)?;
        for row in table.iter().map_err(precommit_storage_error)? {
            let (row_key, row_value) = row.map_err(precommit_storage_error)?;
            if keys::decode_vector_health_observation_key(row_key.value()).is_ok() {
                continue;
            }
            let target =
                keys::decode_vector_observation_key(row_key.value()).map_err(|_| corrupt())?;
            if target.lineage() != &lineage {
                continue;
            }
            let observation = riffdb_storage_api::decode_vector_observation_v1(row_value.value())
                .map_err(crate::error::codec_error)?;
            let observation = observation.value();
            if observation.target() != &target || observation.total_entities() == 0 {
                return Ok(Some(authoritative(
                    StructuralFindingCode::CrossLinkMismatch,
                )));
            }
            let Some((threshold, partitions, breached)) =
                expected.get_mut(&(target.entity_type(), target.vector_field()))
            else {
                return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
            };
            *partitions = partitions.checked_add(1).ok_or_else(corrupt)?;
            if observation.source_stale_entities() > *threshold {
                *breached = breached.checked_add(1).ok_or_else(corrupt)?;
            }
        }
        let exact = health.fields().all(|field| {
            expected
                .get(&(field.entity_type(), field.vector_field()))
                .is_some_and(|(_, partitions, breached)| {
                    *partitions == field.partition_count()
                        && *breached == field.breached_partition_count()
                })
        });
        return Ok((!exact).then(|| authoritative(StructuralFindingCode::CrossLinkMismatch)));
    }
    let target = keys::decode_vector_observation_key(key).map_err(|_| corrupt())?;
    let decoded = riffdb_storage_api::decode_vector_observation_v1(value)
        .map_err(crate::error::codec_error)?;
    let observation = decoded.value();
    if observation.target() != &target || observation.total_entities() == 0 {
        return Err(corrupt());
    }
    let health_key =
        keys::encode_vector_health_observation_key(target.lineage()).map_err(|_| corrupt())?;
    let health_table = transaction
        .open_table(VECTOR_OBSERVATIONS)
        .map_err(table_error)?;
    let Some(health_bytes) = health_table
        .get(health_key.as_slice())
        .map_err(precommit_storage_error)?
    else {
        return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
    };
    let health = riffdb_storage_api::decode_vector_health_observation_v1(health_bytes.value())
        .map_err(crate::error::codec_error)?;
    if !health.value().fields().any(|field| {
        field.entity_type() == target.entity_type() && field.vector_field() == target.vector_field()
    }) {
        return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
    }

    let prefix = keys::encode_vector_evidence_index_prefix(&target).map_err(|_| corrupt())?;
    let upper = exclusive_prefix_end(&prefix).ok_or_else(corrupt)?;
    let index_table = transaction
        .open_table(VECTOR_EVIDENCE_INDEX)
        .map_err(table_error)?;
    let mut total = 0_u64;
    let mut stale = 0_u64;
    let mut models = BTreeMap::new();
    for row in index_table
        .range::<&[u8]>((Included(prefix.as_slice()), Excluded(upper.as_slice())))
        .map_err(precommit_storage_error)?
    {
        let (index_key, index_value) = row.map_err(precommit_storage_error)?;
        let (index_target, entity_key) =
            keys::decode_vector_evidence_index_key(index_key.value()).map_err(|_| corrupt())?;
        let entry = riffdb_storage_api::decode_vector_evidence_index_v1(index_value.value())
            .map_err(crate::error::codec_error)?;
        let entry = entry.value();
        if index_target != target || entry.target() != &target || entry.entity_key() != &entity_key
        {
            return Ok(Some(authoritative(
                StructuralFindingCode::CrossLinkMismatch,
            )));
        }
        total = total.checked_add(1).ok_or_else(corrupt)?;
        if entry.source_stale() {
            stale = stale.checked_add(1).ok_or_else(corrupt)?;
        }
        if let Some(embedding) = entry.embedding_write() {
            let count = models.entry(embedding.metadata().clone()).or_insert(0_u64);
            *count = count.checked_add(1).ok_or_else(corrupt)?;
        }
    }
    let expected = riffdb_storage_api::VectorObservationCountsV1::from_parts(
        target,
        total,
        stale,
        models.into_iter().collect(),
        observation.revision(),
    )
    .map_err(|_| corrupt())?;
    if &expected != observation {
        return Ok(Some(authoritative(
            StructuralFindingCode::CrossLinkMismatch,
        )));
    }
    Ok(None)
}

/// Validates one retained reactive-module row against everything it names.
fn inspect_reactive_module_row_cached(
    transaction: &ReadTransaction,
    publication_audits: &PublicationAuditCache,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let module_hash = keys::decode_reactive_module_key(key).map_err(|_| corrupt())?;
    let module = codec::decode_reactive_module_v1(value)?.into_parts().0;
    if module.module_hash() != module_hash
        || hash_reactive_source(module.canonical_source()) != module.source_hash()
        || hash_reactive_module(module.canonical_module()) != module_hash
    {
        return Ok(Some(authoritative(
            StructuralFindingCode::CrossLinkMismatch,
        )));
    }
    if !bundle_exists(
        transaction,
        module.contract_lineage(),
        module.contract_version(),
        module.contract_bundle_hash(),
    )? {
        return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
    }
    let query_modules = transaction.open_table(QUERY_MODULES).map_err(table_error)?;
    for dependency_hash in module.query_module_hashes() {
        let dependency_key = keys::encode_query_module_key(*dependency_hash);
        let Some(value) = query_modules
            .get(dependency_key.as_slice())
            .map_err(precommit_storage_error)?
        else {
            return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
        };
        let dependency = codec::decode_query_module_v1(value.value())?.into_parts().0;
        if dependency.module_hash() != *dependency_hash
            || dependency.contract_lineage() != module.contract_lineage()
            || dependency.contract_version() != module.contract_version()
            || dependency.contract_bundle_hash() != module.contract_bundle_hash()
        {
            return Ok(Some(authoritative(
                StructuralFindingCode::CrossLinkMismatch,
            )));
        }
    }
    drop(query_modules);
    if !publication_audits.valid || !publication_audits.reactive_modules.contains(&module_hash) {
        return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
    }
    Ok(None)
}

/// Collects the retained publication facts needed by every startup reciprocity
/// check in one exact forward `AUDIT` pass.
///
/// Command locators are skipped without decoding their capsules. All other
/// administration records are decoded and sequence-checked exactly once. Only
/// rare catalog/query publications and reactive hashes with a retained module
/// are kept, under the startup evidence byte cap. A malformed stream produces
/// an invalid cache rather than partial authority; every cached lookup then
/// fails closed and the structural walk emits an authoritative finding.
fn build_publication_audit_cache(
    transaction: &ReadTransaction,
) -> Result<PublicationAuditCache, StorageError> {
    let audit = transaction.open_table(AUDIT).map_err(table_error)?;
    let modules = transaction
        .open_table(REACTIVE_MODULES)
        .map_err(table_error)?;
    let mut cache = PublicationAuditCache {
        catalogs: Vec::new(),
        query_modules: Vec::new(),
        reactive_modules: BTreeSet::new(),
        decoded_rows: 0,
        valid: true,
    };
    let mut retained_bytes = 0usize;
    for entry in audit.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let Ok(sequence) = keys::decode_audit_key(key.value()) else {
            cache.valid = false;
            break;
        };
        let record = match decode_non_command_administration_audit(value.value()) {
            Ok(Some(record)) => record.into_parts().0,
            Ok(None) => continue,
            Err(_) => {
                cache.valid = false;
                break;
            }
        };
        cache.decoded_rows = cache.decoded_rows.saturating_add(1);
        if record.administration_sequence() != sequence {
            cache.valid = false;
            break;
        }
        match record {
            riffdb_storage_api::StoredAdministrationAuditRecordV1::Catalog(record) => {
                retained_bytes = retained_bytes
                    .checked_add(value.value().len())
                    .ok_or_else(limit_exceeded)?;
                cache.catalogs.push(record);
            }
            riffdb_storage_api::StoredAdministrationAuditRecordV1::QueryModule(record) => {
                retained_bytes = retained_bytes
                    .checked_add(value.value().len())
                    .ok_or_else(limit_exceeded)?;
                cache.query_modules.push(record);
            }
            riffdb_storage_api::StoredAdministrationAuditRecordV1::ReactiveModule(record) => {
                let module_key = keys::encode_reactive_module_key(record.module_hash());
                if modules
                    .get(module_key.as_slice())
                    .map_err(precommit_storage_error)?
                    .is_some()
                {
                    retained_bytes = retained_bytes
                        .checked_add(std::mem::size_of::<riffdb_types::ReactiveModuleHash>())
                        .ok_or_else(limit_exceeded)?;
                    cache.reactive_modules.insert(record.module_hash());
                }
            }
            riffdb_storage_api::StoredAdministrationAuditRecordV1::Capability(_)
            | riffdb_storage_api::StoredAdministrationAuditRecordV1::Service(_)
            | riffdb_storage_api::StoredAdministrationAuditRecordV1::Retention(_) => {}
        }
        if retained_bytes > MAX_STARTUP_EVIDENCE_INDEX_BYTES {
            return Err(limit_exceeded());
        }
    }
    Ok(cache)
}

fn inspect_event_consumer_row(
    transaction: &ReadTransaction,
    database_id: DatabaseId,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let identity = keys::decode_event_consumer_key(key).map_err(|_| corrupt())?;
    let consumer = codec::decode_event_consumer_v1(value)?.into_parts().0;
    if consumer.identity().identity_hash() != identity
        || consumer.identity().database_id() != database_id
    {
        return Ok(Some(authoritative(
            StructuralFindingCode::CrossLinkMismatch,
        )));
    }
    let modules = transaction
        .open_table(REACTIVE_MODULES)
        .map_err(table_error)?;
    let module_key = keys::encode_reactive_module_key(consumer.identity().reactive_module_hash());
    if modules
        .get(module_key.as_slice())
        .map_err(precommit_storage_error)?
        .is_none()
    {
        return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
    }
    drop(modules);
    let consumers = transaction
        .open_table(EVENT_CONSUMERS)
        .map_err(table_error)?;
    let deliveries = transaction
        .open_table(EVENT_CONSUMER_DELIVERIES)
        .map_err(table_error)?;
    match crate::consumer::read_snapshot_from_tables(
        &consumers,
        &deliveries,
        database_id,
        identity,
    )? {
        Some(snapshot) if snapshot.consumer() == &consumer => Ok(None),
        _ => Ok(Some(authoritative(
            StructuralFindingCode::CrossLinkMismatch,
        ))),
    }
}

fn inspect_event_consumer_delivery_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let (identity, event_id) =
        keys::decode_event_consumer_delivery_key(key).map_err(|_| corrupt())?;
    let delivery = codec::decode_event_consumer_delivery_v1(value)?
        .into_parts()
        .0;
    if delivery.consumer_identity_hash() != identity || delivery.event_id() != event_id {
        return Ok(Some(authoritative(
            StructuralFindingCode::CrossLinkMismatch,
        )));
    }
    let consumers = transaction
        .open_table(EVENT_CONSUMERS)
        .map_err(table_error)?;
    let consumer_key = keys::encode_event_consumer_key(identity);
    let Some(consumer) = consumers
        .get(consumer_key.as_slice())
        .map_err(precommit_storage_error)?
    else {
        return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
    };
    let consumer = codec::decode_event_consumer_v1(consumer.value())?
        .into_parts()
        .0;
    if consumer.history_incarnation() != delivery.history_incarnation() {
        return Ok(Some(authoritative(
            StructuralFindingCode::CrossLinkMismatch,
        )));
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
    additive_structural_counts: [u64; ADDITIVE_STRUCTURAL_TABLE_COUNT],
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
    let additive_counts = [
        table_len(transaction, REACTIVE_MODULES)?,
        table_len(transaction, EVENT_CONSUMERS)?,
        table_len(transaction, EVENT_CONSUMER_DELIVERIES)?,
        table_len(transaction, APPLICATION_INSTALLATION_CAMPAIGNS)?,
        table_len(transaction, APPLICATION_EXPORT_OPERATIONS)?,
        table_len(transaction, VECTOR_EVIDENCE)?,
        table_len(transaction, VECTOR_OBSERVATIONS)?,
        table_len(transaction, VECTOR_EVIDENCE_INDEX)?,
    ];
    let total = counts
        .iter()
        .chain(additive_counts.iter())
        .try_fold(1u64, |total, count| {
            total.checked_add(*count).ok_or_else(limit_exceeded)
        })?;
    Ok(StartupSnapshot {
        retained_metadata,
        structural_counts: counts,
        additive_structural_counts: additive_counts,
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
    if crate::changelog_v3_journal::has_recovery_roots(transaction)? {
        crate::store::v3_layout::exact_current_tables(&tables, &multimaps)?;
        crate::changelog_v3_roots::read_checkpoint_roots(transaction)?.ok_or_else(corrupt)?;
        return Ok(());
    }
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
    checkpoint: Option<&crate::validated_prefix::ActiveCheckpoint>,
    command_audits: &std::collections::BTreeMap<
        riffdb_types::AdministrationSequence,
        CachedCommandAudit,
    >,
    publication_audits: &PublicationAuditCache,
) -> Result<Option<StructuralFinding>, StorageError> {
    let meta = transaction.open_table(META).map_err(table_error)?;
    let mut seen = BTreeSet::new();
    for entry in meta.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let known = META_KEYS.contains(&key.value())
            || crate::changelog_v3_roots::validate_metadata_entry(key.value(), value.value())?;
        if !known || !seen.insert(key.value().to_owned()) {
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
        || !administration_allocator_matches(
            transaction,
            administration,
            checkpoint,
            command_audits,
        )?
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
    if !active_catalog_matches_last_activation_cached(
        transaction,
        publication_audits,
        active.as_ref(),
    )? {
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
        META_CHANGELOG_V2_ROTATION_RECEIPT => {
            riffdb_storage_api::decode_changelog_v2_rotation_receipt_v1(value).is_ok()
        }
        // Lifecycle evidence is optional to the complete validator. Malformed
        // bytes merely disqualify the fast path and are reset after complete
        // validation (ADR-0157 section 4).
        META_CLEAN_CLOSE_LIFECYCLE => true,
        _ => crate::changelog_v3_roots::validate_metadata_entry(key, value).unwrap_or(false),
    };
    (!valid).then(|| authoritative(StructuralFindingCode::MalformedRecord))
}

fn inspect_bundle_row(
    transaction: &ReadTransaction,
    publication_audits: &PublicationAuditCache,
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
    Ok(
        (!bundle_has_activation_cached(transaction, publication_audits, &bundle)?)
            .then(|| authoritative(StructuralFindingCode::MissingCrossLink)),
    )
}

fn inspect_clean_close_bundle_row(
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok((lineage, version)) = keys::decode_contract_bundle_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let bundle = match decoded(codec::decode_contract_bundle_v1(value)) {
        Ok(bundle) => bundle,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    Ok((bundle.lineage() != &lineage
        || bundle.contract_version() != version
        || hash_contract_bundle(bundle.canonical_bytes()) != bundle.bundle_hash())
    .then(|| authoritative(StructuralFindingCode::CrossLinkMismatch)))
}

fn inspect_clean_close_active_catalog_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    if key != CATALOG_ACTIVE_KEY {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    }
    let active = match decoded(codec::decode_active_catalog_pointer_v1(value)) {
        Ok(active) => active,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    Ok((!bundle_pointer_exists(transaction, &active)?)
        .then(|| authoritative(StructuralFindingCode::MissingCrossLink)))
}

fn inspect_active_row(
    transaction: &ReadTransaction,
    publication_audits: &PublicationAuditCache,
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
    Ok((!active_catalog_matches_last_activation_cached(
        transaction,
        publication_audits,
        Some(&active),
    )?)
    .then(|| authoritative(StructuralFindingCode::CrossLinkMismatch)))
}

fn inspect_query_module_row(
    transaction: &ReadTransaction,
    publication_audits: &PublicationAuditCache,
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
    Ok(
        (!query_module_has_activation_cached(publication_audits, &module))
            .then(|| authoritative(StructuralFindingCode::MissingCrossLink)),
    )
}

fn inspect_active_query_module_row(
    transaction: &ReadTransaction,
    publication_audits: &PublicationAuditCache,
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
    Ok(
        (!query_module_record_is_reciprocal_cached(transaction, publication_audits, &record)?)
            .then(|| authoritative(StructuralFindingCode::CrossLinkMismatch)),
    )
}

fn inspect_index_row(
    binding_bundles: &BTreeSet<riffdb_storage_api::DurableKeySchemaBindingV1>,
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
    Ok((!binding_bundles.contains(record.row().schema_binding()))
        .then(|| authoritative(StructuralFindingCode::MissingCrossLink)))
}

fn inspect_epoch_row(
    binding_bundles: &BTreeSet<riffdb_storage_api::DurableKeySchemaBindingV1>,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    if let Ok(record) = decoded(codec::decode_legacy_index_epoch_v1(value)) {
        let Ok(key) = keys::decode_index_range_prefix_key(key) else {
            return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
        };
        return Ok(
            (record.target() != &key || !binding_bundles.contains(record.schema_binding()))
                .then(|| authoritative(StructuralFindingCode::MissingCrossLink)),
        );
    }
    let Ok(key) = keys::decode_partition_index_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let record = match decoded(codec::decode_index_epoch_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    Ok(
        (record.target() != &key || !binding_bundles.contains(record.schema_binding()))
            .then(|| authoritative(StructuralFindingCode::MissingCrossLink)),
    )
}

fn inspect_terminal_row(
    transaction: &ReadTransaction,
    command_capsules: &std::collections::BTreeMap<
        CommitSequence,
        riffdb_storage_api::StoredCommandCapsuleV1,
    >,
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
    let mut command_capsule = None;
    let (identity, plan, outcome) = match &record {
        IdempotencyRecordV1::StoredOutcome(value) => (
            value.identity().clone(),
            value.plan().clone(),
            Some(value.clone()),
        ),
        IdempotencyRecordV1::ExecutionFailed(value) => (
            value.pending().identity().clone(),
            value.pending().plan().clone(),
            None,
        ),
        IdempotencyRecordV1::CommandLocator(locator) => {
            let Some(capsule) = command_capsules.get(&locator.commit_sequence()) else {
                return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
            };
            command_capsule = Some(capsule);
            (
                capsule.outcome().identity().clone(),
                capsule.outcome().plan().clone(),
                Some(capsule.outcome().clone()),
            )
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
    let terminal_plan_exists = plan_bundle_exists(transaction, &plan)?;
    let pending_exists = raw_exists(transaction, IDEMPOTENCY_PENDING, key)?;
    if !terminal_plan_exists || pending_exists {
        return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
    }
    if let Some(outcome) = outcome.as_ref() {
        let reciprocal = match command_capsule {
            Some(capsule) => {
                outcome == capsule.outcome()
                    && outcome_matches_commit(outcome, capsule.commit())
                    && provenance_matches(outcome, capsule.commit(), capsule.provenance())
                    && command_capsule_graph_is_reciprocal(transaction, capsule)?
            }
            None => outcome_graph_is_reciprocal(transaction, outcome)?,
        };
        if !reciprocal {
            return Ok(Some(authoritative(
                StructuralFindingCode::CrossLinkMismatch,
            )));
        }
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
    command_capsules: &std::collections::BTreeMap<
        CommitSequence,
        riffdb_storage_api::StoredCommandCapsuleV1,
    >,
    embedded_command_authority: &BTreeSet<CommitSequence>,
    binding_bundles: &BTreeSet<riffdb_storage_api::DurableKeySchemaBindingV1>,
    expected_first: CommitSequence,
    key: &[u8],
    value: &[u8],
) -> Result<(Option<StructuralFinding>, Option<CommitSequence>), StorageError> {
    let Ok(sequence) = keys::decode_application_sequence_key(key) else {
        return Ok((
            Some(authoritative(StructuralFindingCode::MalformedRecord)),
            None,
        ));
    };
    let events = transaction.open_table(EVENTS).map_err(table_error)?;
    let records = match commits_in_physical_row(value, &events, sequence) {
        Ok(records) => records,
        Err(error) => {
            let code = match error.kind() {
                StorageErrorKind::LimitExceeded => StructuralFindingCode::LimitExceeded,
                _ => StructuralFindingCode::MalformedRecord,
            };
            return Ok((Some(authoritative(code)), None));
        }
    };
    if sequence != expected_first {
        return Ok((
            Some(authoritative(StructuralFindingCode::SequenceDiscontinuity)),
            records
                .last()
                .map(|record| record.value().commit_sequence()),
        ));
    }
    for record in &records {
        let record = record.value();
        let plan_exists = binding_bundles.iter().any(|binding| {
            binding.lineage() == record.plan().contract_lineage()
                && binding.contract_version() == record.plan().contract_version()
                && binding.bundle_hash() == record.plan().contract_bundle_hash()
        });
        let reciprocal = match command_capsules.get(&record.commit_sequence()) {
            Some(capsule) if capsule.commit() == record => {
                embedded_command_authority.contains(&record.commit_sequence())
                    || command_capsule_graph_is_reciprocal(transaction, capsule)?
            }
            Some(_) => false,
            None => commit_graph_is_reciprocal(transaction, record)?,
        };
        if !plan_exists || !reciprocal {
            return Ok((
                Some(authoritative(StructuralFindingCode::MissingCrossLink)),
                records
                    .last()
                    .map(|record| record.value().commit_sequence()),
            ));
        }
    }
    Ok((
        None,
        records
            .last()
            .map(|record| record.value().commit_sequence()),
    ))
}

fn inspect_provenance_row(
    transaction: &ReadTransaction,
    command_capsules: &std::collections::BTreeMap<
        CommitSequence,
        riffdb_storage_api::StoredCommandCapsuleV1,
    >,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(id) = keys::decode_provenance_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let mut command_capsule = None;
    let record = match decoded(codec::decode_provenance_record_v1(value)) {
        Ok(value) => value,
        Err(_) => {
            let locator = match decoded(codec::decode_command_locator_v1(value)) {
                Ok(locator) => locator,
                Err(code) => return Ok(Some(authoritative(code))),
            };
            let Some(capsule) = command_capsules.get(&locator.commit_sequence()) else {
                return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
            };
            command_capsule = Some(capsule);
            capsule.provenance().clone()
        }
    };
    let reciprocal = match command_capsule {
        Some(capsule) => {
            &record == capsule.provenance()
                && provenance_matches(capsule.outcome(), capsule.commit(), &record)
        }
        None => provenance_graph_is_reciprocal(transaction, &record)?,
    };
    Ok((record.provenance_id() != id
        || !plan_bundle_exists(transaction, record.plan())?
        || !reciprocal)
        .then(|| authoritative(StructuralFindingCode::CrossLinkMismatch)))
}

fn inspect_event_row(
    transaction: &ReadTransaction,
    command_capsules: &std::collections::BTreeMap<
        CommitSequence,
        riffdb_storage_api::StoredCommandCapsuleV1,
    >,
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
    let route_matches = if let Some(commit) =
        get_commit_cached(transaction, command_capsules, id.commit_sequence())?
    {
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
        || !event_graph_is_reciprocal(transaction, command_capsules, &event)?
        || !route_matches)
        .then(|| authoritative(StructuralFindingCode::CrossLinkMismatch)))
}

fn inspect_event_route_row(
    transaction: &ReadTransaction,
    command_capsules: &std::collections::BTreeMap<
        CommitSequence,
        riffdb_storage_api::StoredCommandCapsuleV1,
    >,
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
    let Some(commit) = get_commit_cached(transaction, command_capsules, id.commit_sequence())?
    else {
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
    command_capsules: &std::collections::BTreeMap<
        CommitSequence,
        riffdb_storage_api::StoredCommandCapsuleV1,
    >,
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
    Ok((intent.event_id() != id
        || !event_graph_is_reciprocal(transaction, command_capsules, intent.event())?)
    .then(|| authoritative(StructuralFindingCode::CrossLinkMismatch)))
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
    let intent_exists =
        raw_exists(transaction, OUTBOX, key)? || command_segment_event_exists(transaction, id)?;
    Ok((status.event_id() != id || !intent_exists)
        .then(|| derived_outbox(StructuralFindingCode::OrphanedOutboxStatus)))
}

fn command_segment_event_exists(
    transaction: &ReadTransaction,
    event_id: riffdb_types::EventId,
) -> Result<bool, StorageError> {
    let commits = transaction.open_table(COMMITS).map_err(table_error)?;
    let events = transaction.open_table(EVENTS).map_err(table_error)?;
    Ok(matches!(
        command_member_at(&commits, &events, event_id.commit_sequence())?,
        Some(crate::command_authority::CommandAuthorityMember::CapsuleV2(command))
            if command.events().iter().any(|event| event.event_id() == event_id)
    ))
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
            let previous =
                CommitSequence::new(key.commit_sequence().get() - 1).ok_or_else(invariant)?;
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

fn inspect_cached_command_audit_row(
    transaction: &ReadTransaction,
    cached: &std::collections::BTreeMap<riffdb_types::AdministrationSequence, CachedCommandAudit>,
    embedded_command_authority: &BTreeSet<CommitSequence>,
    index: u64,
    key: &[u8],
    value: &[u8],
) -> Result<Option<Option<StructuralFinding>>, StorageError> {
    let Ok(locator) = codec::decode_command_audit_locator_v1(value) else {
        return Ok(None);
    };
    let locator = locator.into_parts().0;
    let Ok(sequence) = keys::decode_audit_key(key) else {
        return Ok(Some(Some(authoritative(
            StructuralFindingCode::MalformedRecord,
        ))));
    };
    let Some(command) = cached.get(&sequence) else {
        return Ok(Some(Some(authoritative(
            StructuralFindingCode::MissingCrossLink,
        ))));
    };
    let expected_sequence = index
        .checked_add(1)
        .and_then(riffdb_types::AdministrationSequence::new);
    if expected_sequence != Some(sequence)
        || locator.commit_sequence() != command.commit_sequence
        || locator.member() != command.member
        || command.record.administration_sequence() != sequence
        || !match command.member {
            riffdb_storage_api::StoredCommandAuditMemberV1::Started => {
                command.record.phase() == riffdb_types::ServiceAuditPhaseV1::Started
                    && command.record.link() == riffdb_types::ServiceAuditLinkV1::None
            }
            riffdb_storage_api::StoredCommandAuditMemberV1::Terminal => {
                command.record.phase() == riffdb_types::ServiceAuditPhaseV1::Succeeded
                    && matches!(
                        command.record.link(),
                        riffdb_types::ServiceAuditLinkV1::Command { commit_sequence, .. }
                            if commit_sequence == command.commit_sequence
                    )
            }
        }
    {
        return Ok(Some(Some(authoritative(
            StructuralFindingCode::CrossLinkMismatch,
        ))));
    }
    let Some(peer) = cached.get(&command.peer_sequence) else {
        return Ok(Some(Some(authoritative(
            StructuralFindingCode::MissingCrossLink,
        ))));
    };
    let request_index_exists = embedded_command_authority.contains(&command.commit_sequence)
        || service_audit_request_index_exists(transaction, command.record.request_id(), sequence)?;
    if peer.commit_sequence != command.commit_sequence
        || peer.record.request_id() != command.record.request_id()
        || peer.peer_sequence != sequence
        || peer.member == command.member
        || !request_index_exists
    {
        return Ok(Some(Some(authoritative(
            StructuralFindingCode::CrossLinkMismatch,
        ))));
    }
    Ok(Some(None))
}

fn inspect_audit_row(
    transaction: &ReadTransaction,
    publication_audits: &PublicationAuditCache,
    index: u64,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(sequence) = keys::decode_audit_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let record = match decoded(decode_administration_audit_in_read(transaction, value)) {
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
            } else if !catalog_record_is_reciprocal_cached(transaction, publication_audits, record)?
            {
                Some(authoritative(StructuralFindingCode::CrossLinkMismatch))
            } else {
                None
            }
        }
        riffdb_storage_api::StoredAdministrationAuditRecordV1::QueryModule(record) => {
            if !query_module_pointer_exists(transaction, record.activated())? {
                Some(authoritative(StructuralFindingCode::MissingCrossLink))
            } else if !query_module_record_is_reciprocal_cached(
                transaction,
                publication_audits,
                record,
            )? {
                Some(authoritative(StructuralFindingCode::CrossLinkMismatch))
            } else {
                None
            }
        }
        riffdb_storage_api::StoredAdministrationAuditRecordV1::ReactiveModule(record) => {
            let table = transaction
                .open_table(REACTIVE_MODULES)
                .map_err(table_error)?;
            let key = keys::encode_reactive_module_key(record.module_hash());
            match table.get(key.as_slice()).map_err(precommit_storage_error)? {
                Some(value)
                    if codec::decode_reactive_module_v1(value.value())
                        .map(|item| item.value().module_hash() == record.module_hash())
                        .unwrap_or(false) =>
                {
                    None
                }
                _ => Some(authoritative(StructuralFindingCode::MissingCrossLink)),
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
    let audit = audit_record_at(transaction, sequence)?;
    let record = match audit {
        Ok(Some(riffdb_storage_api::StoredAdministrationAuditRecordV1::Service(record)))
            if record.request_id() == request_id
                && record.administration_sequence() == sequence =>
        {
            record
        }
        Ok(Some(_)) => {
            return Ok(Some(authoritative(
                StructuralFindingCode::CrossLinkMismatch,
            )));
        }
        Ok(None) | Err(_) => {
            return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
        }
    };
    // The owning AUDIT phase proves lifecycle reciprocity (and does so only for
    // the suffix under a validated-prefix checkpoint). This full-walk reverse
    // index phase proves the row's own target and the target's authoritative
    // command/control-plane link without rebuilding a whole-history command
    // cache. Prefix lifecycle evidence is already bound by the checkpoint.
    Ok((!service_link_is_valid(transaction, &record)?)
        .then(|| authoritative(StructuralFindingCode::CrossLinkMismatch)))
}

const MAX_STARTUP_EVIDENCE_INDEX_BYTES: usize = 512 * 1024 * 1024;
const HISTORICAL_GROUP_PREFETCH_ITEMS: usize = 500;

fn build_historical_evidence_plan(
    transaction: &ReadTransaction,
    inputs: &StartupValidationInputs,
) -> Result<HistoricalEvidencePlan, StorageError> {
    let mut ordered: std::collections::BTreeMap<Vec<u8>, EvidenceLocator> =
        std::collections::BTreeMap::new();
    let mut index_bytes = 0usize;
    {
        let mut insert =
            |order_key: Vec<u8>, locator: EvidenceLocator| -> Result<(), StorageError> {
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
    }

    let mut groups: std::collections::BTreeMap<Vec<u8>, PersistedEvidenceGroupSources> =
        std::collections::BTreeMap::new();
    collect_persisted_key_groups(transaction, &mut |order_prefix, source| {
        if let Some(sources) = groups.get_mut(&order_prefix) {
            source.add_to(sources);
            return Ok(());
        }
        let charge = order_prefix
            .len()
            .checked_add(PersistedEvidenceGroupSources::default().retained_bytes())
            .ok_or_else(limit_exceeded)?;
        index_bytes = index_bytes.checked_add(charge).ok_or_else(limit_exceeded)?;
        if index_bytes > MAX_STARTUP_EVIDENCE_INDEX_BYTES {
            return Err(limit_exceeded());
        }
        let mut sources = PersistedEvidenceGroupSources::default();
        source.add_to(&mut sources);
        groups.insert(order_prefix, sources);
        Ok(())
    })?;

    {
        let mut insert =
            |order_key: Vec<u8>, locator: EvidenceLocator| -> Result<(), StorageError> {
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
        collect_capability_partition_locators(transaction, inputs, &mut insert)?;
    }

    let split = ordered
        .keys()
        .position(|key| key.first().is_some_and(|tag| *tag >= 0x05))
        .unwrap_or(ordered.len());
    let mut entries = ordered.into_iter().collect::<Vec<_>>();
    let suffix_entries = entries.split_off(split);
    let prefix_entries = entries;

    Ok(HistoricalEvidencePlan {
        prefix_entries,
        next_prefix: 0,
        persisted_groups: groups
            .into_iter()
            .map(|(order_prefix, sources)| PersistedEvidenceGroup {
                order_prefix,
                sources,
            })
            .collect(),
        next_group: 0,
        group_cursor: None,
        suffix_entries,
        next_suffix: 0,
        pending: None,
        retained_bytes: index_bytes,
    })
}

fn build_clean_close_historical_evidence_plan(
    transaction: &ReadTransaction,
) -> Result<HistoricalEvidencePlan, StorageError> {
    let mut ordered = BTreeMap::<Vec<u8>, EvidenceLocator>::new();
    let mut index_bytes = 0_usize;
    {
        let mut insert =
            |order_key: Vec<u8>, locator: EvidenceLocator| -> Result<(), StorageError> {
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
        collect_active_catalog_locator(transaction, &mut insert)?;
    }
    Ok(HistoricalEvidencePlan {
        prefix_entries: ordered.into_iter().collect(),
        next_prefix: 0,
        persisted_groups: Vec::new(),
        next_group: 0,
        group_cursor: None,
        suffix_entries: Vec::new(),
        next_suffix: 0,
        pending: None,
        retained_bytes: index_bytes,
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
            IdempotencyRecordV1::StoredOutcome(value) => {
                (value.identity().clone(), value.plan().clone())
            }
            IdempotencyRecordV1::ExecutionFailed(value) => (
                value.pending().identity().clone(),
                value.pending().plan().clone(),
            ),
            // Successful command plans are collected once from COMMITS below;
            // structural evidence already proved every locator reciprocal.
            IdempotencyRecordV1::CommandLocator(_) => continue,
        };
        if identity.storage_key().ok().as_ref() != Some(&physical) {
            return Err(corrupt());
        }
        push_plan(plan)?;
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
        let physical_sequence =
            keys::decode_application_sequence_key(key.value()).map_err(|_| corrupt())?;
        let records = match commits_in_physical_row(value.value(), &events, physical_sequence) {
            Ok(records) => records,
            // Historical semantic ordering is an auxiliary view. The
            // structural commit phase reports this malformed row; omitting it
            // here avoids converting a typed finding into an aborted scan.
            Err(error) if error.kind() == StorageErrorKind::CorruptData => continue,
            Err(error) => return Err(error),
        };
        for record in records {
            push_plan(record.value().plan().clone())?;
        }
    }
    let provenance = transaction.open_table(PROVENANCE).map_err(table_error)?;
    for entry in provenance.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let id = keys::decode_provenance_key(key.value()).map_err(|_| corrupt())?;
        let record = match decoded(codec::decode_provenance_record_v1(value.value())) {
            Ok(record) => record,
            Err(_) => {
                let _locator = decoded(codec::decode_command_locator_v1(value.value()))
                    .map_err(|_| corrupt())?;
                continue;
            }
        };
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

#[derive(Clone, Copy, Eq, PartialEq)]
enum PersistedEvidenceGroupSource {
    Entity,
    IndexRow,
    LegacyEpoch,
    PartitionEpoch,
}

impl PersistedEvidenceGroupSource {
    const fn add_to(self, sources: &mut PersistedEvidenceGroupSources) {
        match self {
            Self::Entity => sources.entities = true,
            Self::IndexRow => sources.index_rows = true,
            Self::LegacyEpoch => sources.legacy_epochs = true,
            Self::PartitionEpoch => sources.partition_epochs = true,
        }
    }
}

fn persisted_evidence_group(
    evidence: &HistoricalSemanticEvidence,
) -> Result<(Vec<u8>, PersistedEvidenceGroupSource), StorageError> {
    let (suffix_len, source) = match evidence {
        HistoricalSemanticEvidence::PersistedKey(persisted) => match persisted.key() {
            riffdb_storage_api::IrOpaquePersistedKeyV1::Entity { key, .. } => {
                (key.as_bytes().len(), PersistedEvidenceGroupSource::Entity)
            }
            riffdb_storage_api::IrOpaquePersistedKeyV1::IndexRangePrefix(prefix) => (
                prefix.as_bytes().len(),
                PersistedEvidenceGroupSource::LegacyEpoch,
            ),
            riffdb_storage_api::IrOpaquePersistedKeyV1::PartitionIndex(target) => (
                target.partition_key().as_bytes().len(),
                PersistedEvidenceGroupSource::PartitionEpoch,
            ),
        },
        HistoricalSemanticEvidence::IndexMigrationRow(row) => (
            row.physical_key().as_bytes().len(),
            PersistedEvidenceGroupSource::IndexRow,
        ),
        _ => return Err(invariant()),
    };
    let mut order_prefix = historical_order_key(evidence);
    let prefix_len = order_prefix
        .len()
        .checked_sub(suffix_len)
        .ok_or_else(invariant)?;
    order_prefix.truncate(prefix_len);
    Ok((order_prefix, source))
}

fn entity_historical_evidence(
    key: &[u8],
    value: &[u8],
) -> Result<HistoricalSemanticEvidence, StorageError> {
    let physical = keys::decode_entity_key(key).map_err(|_| corrupt())?;
    let record = decoded(codec::decode_entity_record_v1(value)).map_err(|_| corrupt())?;
    if record.target().key() != &physical {
        return Err(corrupt());
    }
    Ok(HistoricalSemanticEvidence::PersistedKey(
        HistoricalPersistedKeyEvidenceV1::from_entity(&record),
    ))
}

fn index_historical_evidence(
    key: &[u8],
    value: &[u8],
) -> Result<HistoricalSemanticEvidence, StorageError> {
    let physical = keys::decode_index_entry_key(key).map_err(|_| corrupt())?;
    codec::decode_index_migration_row(&physical, value)
        .map(HistoricalSemanticEvidence::IndexMigrationRow)
}

fn epoch_historical_evidence(
    key: &[u8],
    value: &[u8],
) -> Result<HistoricalSemanticEvidence, StorageError> {
    if let Ok(record) = decoded(codec::decode_legacy_index_epoch_v1(value)) {
        let physical = keys::decode_index_range_prefix_key(key).map_err(|_| corrupt())?;
        if record.target() != &physical {
            return Err(corrupt());
        }
        return Ok(HistoricalSemanticEvidence::PersistedKey(
            HistoricalPersistedKeyEvidenceV1::from_legacy_index_epoch(&record),
        ));
    }
    let physical = keys::decode_partition_index_key(key).map_err(|_| corrupt())?;
    let record = decoded(codec::decode_index_epoch_v1(value)).map_err(|_| corrupt())?;
    if record.target() != &physical {
        return Err(corrupt());
    }
    Ok(HistoricalSemanticEvidence::PersistedKey(
        HistoricalPersistedKeyEvidenceV1::from_index_epoch(&record),
    ))
}

fn collect_persisted_key_groups(
    transaction: &ReadTransaction,
    insert: &mut dyn FnMut(Vec<u8>, PersistedEvidenceGroupSource) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let entities = transaction.open_table(ENTITIES).map_err(table_error)?;
    for entry in entities.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let evidence = entity_historical_evidence(key.value(), value.value())?;
        let (order_prefix, source) = persisted_evidence_group(&evidence)?;
        insert(order_prefix, source)?;
    }
    let indexes = transaction
        .open_table(SECONDARY_INDEXES)
        .map_err(table_error)?;
    for entry in indexes.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let evidence = index_historical_evidence(key.value(), value.value())?;
        let (order_prefix, source) = persisted_evidence_group(&evidence)?;
        insert(order_prefix, source)?;
    }
    let epochs = transaction.open_table(INDEX_EPOCHS).map_err(table_error)?;
    for entry in epochs.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let evidence = epoch_historical_evidence(key.value(), value.value())?;
        let (order_prefix, source) = persisted_evidence_group(&evidence)?;
        insert(order_prefix, source)?;
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

struct HistoricalMaterializationTables {
    bundles: ReadOnlyTable<&'static [u8], &'static [u8]>,
    entities: ReadOnlyTable<&'static [u8], &'static [u8]>,
    indexes: ReadOnlyTable<&'static [u8], &'static [u8]>,
    epochs: ReadOnlyTable<&'static [u8], &'static [u8]>,
    capabilities: ReadOnlyTable<&'static [u8], &'static [u8]>,
}

impl HistoricalMaterializationTables {
    fn open(transaction: &ReadTransaction) -> Result<Self, StorageError> {
        Ok(Self {
            bundles: transaction
                .open_table(CONTRACT_BUNDLES)
                .map_err(table_error)?,
            entities: transaction.open_table(ENTITIES).map_err(table_error)?,
            indexes: transaction
                .open_table(SECONDARY_INDEXES)
                .map_err(table_error)?,
            epochs: transaction.open_table(INDEX_EPOCHS).map_err(table_error)?,
            capabilities: transaction.open_table(CAPABILITIES).map_err(table_error)?,
        })
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "private evidence iterator keeps each bounded cursor and decoder explicit"
)]
fn next_matching_group_row(
    table: &ReadOnlyTable<&'static [u8], &'static [u8]>,
    after: &mut Option<Vec<u8>>,
    exhausted: &mut bool,
    order_prefix: &[u8],
    expected_source: PersistedEvidenceGroupSource,
    prior: &mut Option<Vec<u8>>,
    buffered: &mut VecDeque<PendingHistoricalEvidence>,
    decode: fn(&[u8], &[u8]) -> Result<HistoricalSemanticEvidence, StorageError>,
) -> Result<Option<PendingHistoricalEvidence>, StorageError> {
    if let Some(item) = buffered.pop_front() {
        return Ok(Some(item));
    }
    if *exhausted {
        return Ok(None);
    }
    let physical_prefix = match expected_source {
        PersistedEvidenceGroupSource::Entity => Some(ENTITY_KEY_V1_PREFIX),
        PersistedEvidenceGroupSource::IndexRow | PersistedEvidenceGroupSource::LegacyEpoch => {
            Some(INDEX_ENTRY_KEY_V1_PREFIX)
        }
        PersistedEvidenceGroupSource::PartitionEpoch => None,
    }
    .map(|namespace| {
        let owner_start = order_prefix.len().checked_sub(8).ok_or_else(invariant)?;
        let owner_end = owner_start.checked_add(4).ok_or_else(invariant)?;
        let owner = order_prefix
            .get(owner_start..owner_end)
            .ok_or_else(invariant)?;
        let mut prefix = Vec::with_capacity(namespace.len() + owner.len());
        prefix.extend_from_slice(&namespace);
        prefix.extend_from_slice(owner);
        Ok::<_, StorageError>(prefix)
    })
    .transpose()?;
    let physical_end = physical_prefix.as_ref().map(|prefix| {
        let mut end = prefix.clone();
        for byte in end.iter_mut().rev() {
            if *byte != u8::MAX {
                *byte = byte.saturating_add(1);
                return Ok(end);
            }
            *byte = 0;
        }
        Err(invariant())
    });
    let physical_end = physical_end.transpose()?;
    let lower = after.as_ref().map_or_else(
        || physical_prefix.as_deref().map_or(Unbounded, Included),
        |key| Excluded(key.as_slice()),
    );
    let upper = physical_end.as_deref().map_or(Unbounded, Excluded);
    let mut rows = table
        .range::<&[u8]>((lower, upper))
        .map_err(precommit_storage_error)?;
    let mut stopped_early = false;
    for entry in &mut rows {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let evidence = decode(key.value(), value.value())?;
        let (actual_prefix, source) = persisted_evidence_group(&evidence)?;
        if source != expected_source || actual_prefix != order_prefix {
            *after = Some(key.value().to_vec());
            continue;
        }
        let order_key = historical_order_key(&evidence);
        // This is the load-bearing ordering proof: once schema, kind, owner,
        // and raw-key length are fixed, redb's bytewise table order must agree
        // with the historical key. Refuse an implementation drift rather than
        // silently emit a reordered startup stream.
        if prior.as_ref().is_some_and(|prior| prior >= &order_key) {
            return Err(invariant());
        }
        *after = Some(key.value().to_vec());
        *prior = Some(order_key.clone());
        buffered.push_back(PendingHistoricalEvidence {
            order_key,
            evidence,
        });
        if buffered.len() >= HISTORICAL_GROUP_PREFETCH_ITEMS {
            stopped_early = true;
            break;
        }
    }
    if !stopped_early {
        *exhausted = true;
    }
    Ok(buffered.pop_front())
}

impl PersistedEvidenceGroupCursor {
    fn next(
        &mut self,
        order_prefix: &[u8],
        tables: &HistoricalMaterializationTables,
    ) -> Result<Option<PendingHistoricalEvidence>, StorageError> {
        match self {
            Self::Entities {
                after,
                exhausted,
                prior,
                buffered,
            } => next_matching_group_row(
                &tables.entities,
                after,
                exhausted,
                order_prefix,
                PersistedEvidenceGroupSource::Entity,
                prior,
                buffered,
                entity_historical_evidence,
            ),
            Self::PartitionEpochs {
                after,
                exhausted,
                prior,
                buffered,
            } => next_matching_group_row(
                &tables.epochs,
                after,
                exhausted,
                order_prefix,
                PersistedEvidenceGroupSource::PartitionEpoch,
                prior,
                buffered,
                epoch_historical_evidence,
            ),
            Self::IndexKeys {
                after_index,
                index_exhausted,
                after_epoch,
                epoch_exhausted,
                pending_index,
                pending_epoch,
                prior_index,
                prior_epoch,
                buffered_index,
                buffered_epoch,
            } => {
                if pending_index.is_none() && (!*index_exhausted || !buffered_index.is_empty()) {
                    *pending_index = next_matching_group_row(
                        &tables.indexes,
                        after_index,
                        index_exhausted,
                        order_prefix,
                        PersistedEvidenceGroupSource::IndexRow,
                        prior_index,
                        buffered_index,
                        index_historical_evidence,
                    )?
                    .map(Box::new);
                }
                if pending_epoch.is_none() && (!*epoch_exhausted || !buffered_epoch.is_empty()) {
                    *pending_epoch = next_matching_group_row(
                        &tables.epochs,
                        after_epoch,
                        epoch_exhausted,
                        order_prefix,
                        PersistedEvidenceGroupSource::LegacyEpoch,
                        prior_epoch,
                        buffered_epoch,
                        epoch_historical_evidence,
                    )?
                    .map(Box::new);
                }
                match (
                    pending_index.as_ref().map(|item| item.order_key.as_slice()),
                    pending_epoch.as_ref().map(|item| item.order_key.as_slice()),
                ) {
                    (None, None) => Ok(None),
                    (Some(_), None) => Ok(pending_index.take().map(|item| *item)),
                    (None, Some(_)) => Ok(pending_epoch.take().map(|item| *item)),
                    (Some(index_key), Some(epoch_key)) if index_key <= epoch_key => {
                        if index_key == epoch_key {
                            // The old BTreeMap retained the SECONDARY_INDEXES
                            // locator inserted before INDEX_EPOCHS on a collision.
                            let _ = pending_epoch.take();
                        }
                        Ok(pending_index.take().map(|item| *item))
                    }
                    (Some(_), Some(_)) => Ok(pending_epoch.take().map(|item| *item)),
                }
            }
        }
    }
}

impl HistoricalEvidencePlan {
    fn open_group_cursor(
        group: &PersistedEvidenceGroup,
    ) -> Result<PersistedEvidenceGroupCursor, StorageError> {
        let sources = group.sources;
        if sources.entities
            && !sources.index_rows
            && !sources.legacy_epochs
            && !sources.partition_epochs
        {
            return Ok(PersistedEvidenceGroupCursor::Entities {
                after: None,
                exhausted: false,
                prior: None,
                buffered: VecDeque::new(),
            });
        }
        if sources.partition_epochs
            && !sources.entities
            && !sources.index_rows
            && !sources.legacy_epochs
        {
            return Ok(PersistedEvidenceGroupCursor::PartitionEpochs {
                after: None,
                exhausted: false,
                prior: None,
                buffered: VecDeque::new(),
            });
        }
        if !sources.entities && !sources.partition_epochs {
            return Ok(PersistedEvidenceGroupCursor::IndexKeys {
                after_index: None,
                index_exhausted: !sources.index_rows,
                after_epoch: None,
                epoch_exhausted: !sources.legacy_epochs,
                pending_index: None,
                pending_epoch: None,
                prior_index: None,
                prior_epoch: None,
                buffered_index: VecDeque::new(),
                buffered_epoch: VecDeque::new(),
            });
        }
        Err(invariant())
    }

    fn next(
        &mut self,
        transaction: &ReadTransaction,
        tables: &HistoricalMaterializationTables,
    ) -> Result<Option<PendingHistoricalEvidence>, StorageError> {
        if let Some(pending) = self.pending.take() {
            return Ok(Some(pending));
        }
        if let Some((order_key, locator)) = self.prefix_entries.get(self.next_prefix) {
            let evidence = materialize_historical_evidence(transaction, tables, locator)?;
            let item = PendingHistoricalEvidence {
                order_key: order_key.clone(),
                evidence,
            };
            self.next_prefix = self.next_prefix.checked_add(1).ok_or_else(limit_exceeded)?;
            return Ok(Some(item));
        }
        while let Some(group) = self.persisted_groups.get(self.next_group) {
            if self.group_cursor.is_none() {
                self.group_cursor = Some(Self::open_group_cursor(group)?);
            }
            let cursor = self.group_cursor.as_mut().ok_or_else(invariant)?;
            if let Some(item) = cursor.next(&group.order_prefix, tables)? {
                return Ok(Some(item));
            }
            self.group_cursor = None;
            self.next_group = self.next_group.checked_add(1).ok_or_else(limit_exceeded)?;
        }
        if let Some((order_key, locator)) = self.suffix_entries.get(self.next_suffix) {
            let evidence = materialize_historical_evidence(transaction, tables, locator)?;
            let item = PendingHistoricalEvidence {
                order_key: order_key.clone(),
                evidence,
            };
            self.next_suffix = self.next_suffix.checked_add(1).ok_or_else(limit_exceeded)?;
            return Ok(Some(item));
        }
        Ok(None)
    }
}

fn materialize_historical_evidence(
    transaction: &ReadTransaction,
    tables: &HistoricalMaterializationTables,
    locator: &EvidenceLocator,
) -> Result<HistoricalSemanticEvidence, StorageError> {
    match locator {
        EvidenceLocator::Bundle(key) => {
            let value = tables
                .bundles
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
        EvidenceLocator::CapabilityPartition {
            capability_id,
            entry_ordinal,
        } => {
            let key = keys::encode_capability_key(*capability_id);
            let value = tables
                .capabilities
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
    let table = transaction
        .open_table(CONTRACT_BUNDLES)
        .map_err(table_error)?;
    read_historical_bundle_from_table(&table, lineage, version, hash)
}

fn read_historical_bundle_from_table(
    table: &ReadOnlyTable<&'static [u8], &'static [u8]>,
    lineage: &ContractLineage,
    version: ContractVersion,
    hash: ContractBundleHash,
) -> Result<Option<HistoricalBundleEvidence>, StorageError> {
    let key = keys::encode_contract_bundle_key(lineage, version).map_err(|_| invariant())?;
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

fn decode_administration_audit_in_read(
    transaction: &ReadTransaction,
    encoded: &[u8],
) -> Result<
    riffdb_storage_api::EncodedPageItem<riffdb_storage_api::StoredAdministrationAuditRecordV1>,
    StorageError,
> {
    let commits = transaction.open_table(COMMITS).map_err(table_error)?;
    let events = transaction.open_table(EVENTS).map_err(table_error)?;
    codec::decode_administration_audit_with_command_tables(encoded, &commits, &events)
}

/// Decode an administration-owned audit row without resolving command-owned
/// audit locators through `COMMITS`.
///
/// Callers use this only while searching for catalog, module, capability, or
/// retention records. Command audit rows are validated by the command-cache
/// pass; resolving every locator here would decode the same bounded command
/// segment once per audit member and make startup quadratic in segment size.
fn decode_non_command_administration_audit(
    encoded: &[u8],
) -> Result<
    Option<
        riffdb_storage_api::EncodedPageItem<riffdb_storage_api::StoredAdministrationAuditRecordV1>,
    >,
    StorageError,
> {
    if codec::decode_command_audit_locator_v1(encoded).is_ok() {
        return Ok(None);
    }
    codec::decode_administration_audit_record_v1(encoded).map(Some)
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

#[cfg(test)]
fn bundle_has_activation(
    transaction: &ReadTransaction,
    bundle: &riffdb_storage_api::StoredContractBundleV1,
) -> Result<bool, StorageError> {
    let cache = build_publication_audit_cache(transaction)?;
    bundle_has_activation_cached(transaction, &cache, bundle)
}

fn bundle_has_activation_cached(
    transaction: &ReadTransaction,
    cache: &PublicationAuditCache,
    bundle: &riffdb_storage_api::StoredContractBundleV1,
) -> Result<bool, StorageError> {
    if !cache.valid {
        return Ok(false);
    }
    if cache
        .catalogs
        .iter()
        .any(|activation| activation.activated().matches_bundle(bundle))
    {
        return Ok(true);
    }
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

fn query_module_has_activation_cached(
    cache: &PublicationAuditCache,
    module: &riffdb_storage_api::StoredQueryModuleV1,
) -> bool {
    cache.valid
        && cache
            .query_modules
            .iter()
            .any(|activation| activation.activated().matches_module(module))
}

fn same_query_module_contract(
    left: &riffdb_storage_api::ActiveQueryModulePointerV1,
    right: &riffdb_storage_api::ActiveQueryModulePointerV1,
) -> bool {
    left.contract_lineage() == right.contract_lineage()
        && left.contract_version() == right.contract_version()
        && left.contract_bundle_hash() == right.contract_bundle_hash()
}

fn query_module_record_is_reciprocal_cached(
    transaction: &ReadTransaction,
    cache: &PublicationAuditCache,
    record: &riffdb_storage_api::StoredQueryModuleAdministrationV1,
) -> Result<bool, StorageError> {
    if !cache.valid {
        return Ok(false);
    }
    if !query_module_pointer_exists(transaction, record.activated())? {
        return Ok(false);
    }
    let mut previous = None;
    let mut last = None;
    let mut found = false;
    for candidate in &cache.query_modules {
        if same_query_module_contract(candidate.activated(), record.activated()) {
            if candidate.administration_sequence() < record.administration_sequence() {
                previous = Some(candidate.activated().clone());
            }
            if candidate.administration_sequence() == record.administration_sequence() {
                found = candidate == record;
            }
            last = Some(candidate.clone());
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

#[cfg(test)]
fn catalog_record_is_reciprocal(
    transaction: &ReadTransaction,
    record: &riffdb_storage_api::StoredCatalogAdministrationV1,
) -> Result<bool, StorageError> {
    let cache = build_publication_audit_cache(transaction)?;
    catalog_record_is_reciprocal_cached(transaction, &cache, record)
}

fn catalog_record_is_reciprocal_cached(
    transaction: &ReadTransaction,
    cache: &PublicationAuditCache,
    record: &riffdb_storage_api::StoredCatalogAdministrationV1,
) -> Result<bool, StorageError> {
    if !cache.valid {
        return Ok(false);
    }
    if !bundle_pointer_exists(transaction, record.activated())? {
        return Ok(false);
    }
    let mut previous = None;
    let mut found = false;
    for candidate in &cache.catalogs {
        if candidate.administration_sequence() < record.administration_sequence() {
            previous = Some(candidate.activated().clone());
            continue;
        }
        if candidate.administration_sequence() == record.administration_sequence() {
            found = candidate == record;
        }
        break;
    }
    Ok(found && record.previous_active() == previous.as_ref())
}

#[cfg(test)]
fn active_catalog_matches_last_activation(
    transaction: &ReadTransaction,
    active: Option<&riffdb_storage_api::ActiveCatalogPointerV1>,
) -> Result<bool, StorageError> {
    let cache = build_publication_audit_cache(transaction)?;
    active_catalog_matches_last_activation_cached(transaction, &cache, active)
}

fn active_catalog_matches_last_activation_cached(
    transaction: &ReadTransaction,
    cache: &PublicationAuditCache,
    active: Option<&riffdb_storage_api::ActiveCatalogPointerV1>,
) -> Result<bool, StorageError> {
    if !cache.valid {
        return Ok(false);
    }
    let last_catalog = cache
        .catalogs
        .last()
        .map(|record| (record.administration_sequence(), record.activated().clone()));

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

fn collect_bundle_bindings(
    transaction: &ReadTransaction,
) -> Result<BTreeSet<riffdb_storage_api::DurableKeySchemaBindingV1>, StorageError> {
    let table = transaction
        .open_table(CONTRACT_BUNDLES)
        .map_err(table_error)?;
    let mut bindings = BTreeSet::new();
    let mut retained_bytes = 0_usize;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        retained_bytes = retained_bytes
            .checked_add(key.value().len())
            .and_then(|bytes| bytes.checked_add(value.value().len()))
            .ok_or_else(limit_exceeded)?;
        if retained_bytes > MAX_STARTUP_EVIDENCE_INDEX_BYTES {
            return Err(limit_exceeded());
        }
        let Ok((lineage, version)) = keys::decode_contract_bundle_key(key.value()) else {
            continue;
        };
        let Ok(bundle) = decoded(codec::decode_contract_bundle_v1(value.value())) else {
            continue;
        };
        if bundle.lineage() == &lineage
            && bundle.contract_version() == version
            && hash_contract_bundle(bundle.canonical_bytes()) == bundle.bundle_hash()
        {
            bindings.insert(riffdb_storage_api::DurableKeySchemaBindingV1::new(
                lineage,
                version,
                bundle.bundle_hash(),
            ));
        }
    }
    Ok(bindings)
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
    let commits = transaction.open_table(COMMITS).map_err(table_error)?;
    let events = transaction.open_table(EVENTS).map_err(table_error)?;
    match commit_at(&commits, &events, sequence) {
        Ok(value) => Ok(value),
        Err(error) if error.kind() == StorageErrorKind::CorruptData => Ok(None),
        Err(error) => Err(error),
    }
}

fn get_commit_cached(
    transaction: &ReadTransaction,
    command_capsules: &std::collections::BTreeMap<
        CommitSequence,
        riffdb_storage_api::StoredCommandCapsuleV1,
    >,
    sequence: CommitSequence,
) -> Result<Option<riffdb_storage_api::StoredCommitRecordV1>, StorageError> {
    if let Some(capsule) = command_capsules.get(&sequence) {
        return Ok(Some(capsule.commit().clone()));
    }
    get_commit(transaction, sequence)
}

fn get_command_capsule(
    transaction: &ReadTransaction,
    sequence: CommitSequence,
) -> Result<Option<riffdb_storage_api::StoredCommandCapsuleV1>, StorageError> {
    let commits = transaction.open_table(COMMITS).map_err(table_error)?;
    let events = transaction.open_table(EVENTS).map_err(table_error)?;
    match command_member_at(&commits, &events, sequence) {
        Ok(member) => Ok(member.map(|value| value.into_base())),
        Err(error) if error.kind() == StorageErrorKind::CorruptData => Ok(None),
        Err(error) => Err(error),
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
        Err(_) => {
            let table = transaction.open_table(PROVENANCE).map_err(table_error)?;
            let Some(value) = table.get(key.as_slice()).map_err(precommit_storage_error)? else {
                return Ok(None);
            };
            let locator = match decoded(codec::decode_command_locator_v1(value.value())) {
                Ok(locator) => locator,
                Err(_) => return Ok(None),
            };
            Ok(get_command_capsule(transaction, locator.commit_sequence())?
                .map(|capsule| capsule.provenance().clone())
                .filter(|record| record.provenance_id() == id))
        }
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
        Ok(Some(IdempotencyRecordV1::CommandLocator(locator))) => {
            Ok(get_command_capsule(transaction, locator.commit_sequence())?
                .map(|capsule| IdempotencyRecordV1::StoredOutcome(capsule.outcome().clone()))
                .filter(|record| {
                    matches!(record, IdempotencyRecordV1::StoredOutcome(outcome)
                        if outcome.identity() == identity)
                }))
        }
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
    // Legacy rows have no transition chain and therefore retain the historical
    // current-row cross-link. Delete-capable format activation rewrites command
    // authority to V2 capsules before a checked delete can become executable.
    for reference in commit.entity_references() {
        if !current_entity_covers_legacy_reference(transaction, reference)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn command_capsule_graph_is_reciprocal(
    transaction: &ReadTransaction,
    capsule: &riffdb_storage_api::StoredCommandCapsuleV1,
) -> Result<bool, StorageError> {
    let sequence = capsule.commit_sequence();
    let commits = transaction.open_table(COMMITS).map_err(table_error)?;
    let events = transaction.open_table(EVENTS).map_err(table_error)?;
    if let Some(crate::command_authority::CommandAuthorityMember::CapsuleV2(command)) =
        command_member_at(&commits, &events, sequence)?
    {
        if command.base() != capsule {
            return Ok(false);
        }
        return Ok(true);
    }
    let identity_key = capsule
        .outcome()
        .identity()
        .storage_key()
        .map_err(|_| corrupt())?;
    let idempotency = transaction.open_table(IDEMPOTENCY).map_err(table_error)?;
    let Some(outcome_row) = idempotency
        .get(keys::encode_idempotency_key(&identity_key))
        .map_err(precommit_storage_error)?
    else {
        return Ok(false);
    };
    let outcome_locator = match codec::decode_command_locator_v1(outcome_row.value()) {
        Ok(locator) => locator.into_parts().0,
        Err(_) => return Ok(false),
    };
    if outcome_locator.commit_sequence() != sequence {
        return Ok(false);
    }
    drop(outcome_row);
    drop(idempotency);

    let provenance = transaction.open_table(PROVENANCE).map_err(table_error)?;
    let provenance_key = keys::encode_provenance_key(capsule.provenance().provenance_id());
    let Some(provenance_row) = provenance
        .get(provenance_key.as_slice())
        .map_err(precommit_storage_error)?
    else {
        return Ok(false);
    };
    let provenance_locator = match codec::decode_command_locator_v1(provenance_row.value()) {
        Ok(locator) => locator.into_parts().0,
        Err(_) => return Ok(false),
    };
    if provenance_locator.commit_sequence() != sequence {
        return Ok(false);
    }

    for event in capsule.commit().events() {
        if get_event(transaction, event.event_id())?.as_ref() != Some(event)
            || get_outbox(transaction, event.event_id())?
                .as_ref()
                .map(riffdb_storage_api::StoredOutboxIntentV1::event)
                != Some(event)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn current_entity_covers_legacy_reference(
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

enum EntityHistoryMembers {
    References(Vec<riffdb_storage_api::CommittedEntityReferenceV2>),
    Transitions(Vec<riffdb_storage_api::CommittedEntityTransitionV1>),
}

fn entity_history_members_in_physical_row<E>(
    encoded: &[u8],
    events: &E,
    physical_sequence: CommitSequence,
) -> Result<Vec<(CommitSequence, EntityHistoryMembers)>, StorageError>
where
    E: ReadableTable<&'static [u8], &'static [u8]>,
{
    match riffdb_storage_api::decode_command_segment_v1(encoded) {
        Ok(segment) => {
            let segment = segment.into_parts().0;
            if segment.first_commit_sequence() != physical_sequence {
                return Err(corrupt());
            }
            return Ok(segment
                .commands()
                .iter()
                .map(|command| {
                    let members = if command.entity_transitions().is_empty() {
                        EntityHistoryMembers::References(
                            command.base().commit().entity_references().to_vec(),
                        )
                    } else {
                        EntityHistoryMembers::Transitions(command.entity_transitions().to_vec())
                    };
                    (command.commit_sequence(), members)
                })
                .collect());
        }
        Err(error)
            if error.kind() == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType => {}
        Err(error) => return Err(crate::error::codec_error(error)),
    }
    match riffdb_storage_api::decode_command_capsule_v2(encoded) {
        Ok(capsule) => {
            let capsule = capsule.into_parts().0;
            if capsule.commit_sequence() != physical_sequence {
                return Err(corrupt());
            }
            let members = if capsule.entity_transitions().is_empty() {
                EntityHistoryMembers::References(
                    capsule.base().commit().entity_references().to_vec(),
                )
            } else {
                EntityHistoryMembers::Transitions(capsule.entity_transitions().to_vec())
            };
            return Ok(vec![(capsule.commit_sequence(), members)]);
        }
        Err(error)
            if error.kind() == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType => {}
        Err(error) => return Err(crate::error::codec_error(error)),
    }
    commits_in_physical_row(encoded, events, physical_sequence).map(|commits| {
        commits
            .into_iter()
            .map(|commit| {
                (
                    commit.value().commit_sequence(),
                    EntityHistoryMembers::References(commit.value().entity_references().to_vec()),
                )
            })
            .collect()
    })
}

/// Builds entity continuity chains from one forward COMMITS pass.
///
/// Without a seed the pass covers the complete history from genesis. With a
/// verified-checkpoint seed `(S, entity_heads_at_S)` the chains start from the
/// fingerprint-verified exact chain-head map at S, including post-image and
/// transition hashes. Any migration recorded at write time is already baked
/// into those heads; the pass advances across the suffix `(S, head]` only.
fn build_entity_chains(
    transaction: &ReadTransaction,
    entity_count: u64,
    seed: Option<(
        u64,
        std::collections::BTreeMap<
            riffdb_storage_api::EntityTarget,
            riffdb_storage_api::EntityChainHeadV1,
        >,
    )>,
) -> Result<EntityChainState, StorageError> {
    let has_exact_head_snapshot = seed.is_some();
    let migrations = load_entity_migration_evidence(transaction)?;
    // Chains build from the RETAINED commit range only: below-watermark commit
    // bodies are tombstone-covered (verified before this walk began).
    let pruned_floor = retention_watermark_sequence(transaction)?;
    let table = transaction.open_table(COMMITS).map_err(table_error)?;
    let mut chains: std::collections::BTreeMap<riffdb_storage_api::EntityTarget, EntityChain> =
        std::collections::BTreeMap::new();
    let mut transition_heads = std::collections::BTreeMap::<
        riffdb_storage_api::EntityTarget,
        riffdb_storage_api::EntityChainHeadV1,
    >::new();
    let mut orphan_targets = Vec::new();
    let mut overflow = false;
    let mut suffix_is_transition_complete = true;
    let entity_count_usize = usize::try_from(entity_count).unwrap_or(usize::MAX);
    let mut walk_lower_bound = None;
    let mut walk_after = 0_u64;
    if let Some((s, heads_at_s)) = seed {
        for (target, head) in heads_at_s {
            let (version, hash) = match head.state() {
                riffdb_storage_api::EntityChainStateV1::Live {
                    version,
                    value_hash,
                } => (version, Some(value_hash)),
                // Recreate restarts entity-local versioning at one. The exact
                // deleted predecessor remains in `transition_heads`; this
                // placeholder is never used to authorize a transition.
                riffdb_storage_api::EntityChainStateV1::Deleted => {
                    (riffdb_types::EntityVersion::first(), None)
                }
                riffdb_storage_api::EntityChainStateV1::NeverExisted => return Err(corrupt()),
            };
            chains.insert(
                target.clone(),
                EntityChain {
                    version,
                    hash,
                    expected_bundle: None,
                    // Entity-chain heads describe application-command
                    // transitions only. Offline contract migrations retain
                    // their own predecessor evidence and can advance an
                    // entity version without rewriting this head. Even an
                    // exact at-S head snapshot must therefore replay the
                    // bounded migration-evidence sequence before comparing a
                    // current entity row or a later command transition.
                    migration_cursor: 0,
                    intact: true,
                    consumed: false,
                },
            );
            transition_heads.insert(target, head);
        }
        if s > 0 {
            walk_after = s;
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
    let events = transaction.open_table(EVENTS).map_err(table_error)?;
    for entry in range {
        let (physical_key, value) = entry.map_err(precommit_storage_error)?;
        let Ok(physical_sequence) = keys::decode_application_sequence_key(physical_key.value())
        else {
            continue;
        };
        // Decode failures are covered by inspect_commit_row (MalformedRecord /
        // CrossLinkMismatch). Continue so sibling entities still validate.
        let histories =
            match entity_history_members_in_physical_row(value.value(), &events, physical_sequence)
            {
                Ok(histories) => histories,
                Err(_) => {
                    let Ok(references) =
                        decoded(codec::decode_commit_entity_references(value.value()))
                    else {
                        continue;
                    };
                    vec![(
                        physical_sequence,
                        EntityHistoryMembers::References(references),
                    )]
                }
            };
        for (commit_sequence, members) in histories {
            if commit_sequence.get() <= walk_after {
                continue;
            }
            let references = match members {
                EntityHistoryMembers::Transitions(transitions) => {
                    apply_entity_transitions_to_startup_chains(
                        &mut chains,
                        &mut transition_heads,
                        &mut orphan_targets,
                        &mut overflow,
                        entity_count_usize,
                        walk_after.max(pruned_floor),
                        pruned_floor > 0 && pruned_floor >= walk_after,
                        commit_sequence,
                        &transitions,
                    )?;
                    continue;
                }
                EntityHistoryMembers::References(references) => references,
            };
            // Frozen predecessor commit formats carry post-image references
            // but no transition heads. Their entities can legitimately have
            // migrated durable heads that this suffix walk cannot derive.
            suffix_is_transition_complete = false;
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
                        },
                    );
                } else {
                    // At capacity: never grow the map; bound orphan reporting.
                    record_orphan_target(&mut orphan_targets, &mut overflow, target_key);
                }
            }
        }
    }
    validate_transition_chain_heads(
        transaction,
        &mut chains,
        &transition_heads,
        has_exact_head_snapshot && suffix_is_transition_complete,
    )?;
    Ok(EntityChainState {
        chains,
        migrations,
        orphan_targets,
        overflow,
        orphans_queued: false,
        pruned_floor,
    })
}

#[allow(clippy::too_many_arguments)]
fn apply_entity_transitions_to_startup_chains(
    chains: &mut std::collections::BTreeMap<riffdb_storage_api::EntityTarget, EntityChain>,
    heads: &mut std::collections::BTreeMap<
        riffdb_storage_api::EntityTarget,
        riffdb_storage_api::EntityChainHeadV1,
    >,
    orphan_targets: &mut Vec<riffdb_storage_api::EntityTarget>,
    overflow: &mut bool,
    capacity: usize,
    retained_boundary_sequence: u64,
    retention_boundary_can_seed: bool,
    commit_sequence: CommitSequence,
    transitions: &[riffdb_storage_api::CommittedEntityTransitionV1],
) -> Result<(), StorageError> {
    let mut seen = std::collections::BTreeSet::new();
    for transition in transitions {
        let target = transition.target().clone();
        if transition.command_sequence() != commit_sequence || !seen.insert(target.clone()) {
            if let Some(chain) = chains.get_mut(&target) {
                chain.intact = false;
            } else {
                record_orphan_target(orphan_targets, overflow, target);
            }
            continue;
        }
        let next_head = if let Some(prior) = heads.get(&target) {
            prior.apply(transition)
        } else if transition.prior_state() == riffdb_storage_api::EntityChainStateV1::NeverExisted {
            riffdb_storage_api::EntityChainHeadV1::from_genesis(transition)
        } else {
            let prior_matches_chain = chains.get(&target).is_some_and(|chain| {
                matches!(
                    transition.prior_state(),
                    riffdb_storage_api::EntityChainStateV1::Live { version, value_hash }
                        if chain.version == version
                            && chain.hash.is_none_or(|hash| hash == value_hash)
                )
            });
            // ADR-0100 requires an explicit durable boundary to anchor a
            // predecessor omitted from retained history. A verified-prefix
            // checkpoint and the offline-retention watermark both provide
            // that boundary. Never guess it from the first successor sequence.
            let predecessor_sequence = CommitSequence::new(retained_boundary_sequence);
            match (
                prior_matches_chain || retention_boundary_can_seed,
                predecessor_sequence,
                transition.prior_transition_hash(),
            ) {
                (true, Some(predecessor_sequence), Some(prior_hash)) => {
                    riffdb_storage_api::EntityChainHeadV1::from_stored_parts(
                        target.clone(),
                        transition.prior_chain_revision(),
                        transition.prior_state(),
                        predecessor_sequence,
                        prior_hash,
                    )
                    .and_then(|prior| prior.apply(transition))
                }
                _ => Err(riffdb_storage_api::StorageValueError::IdentityMismatch),
            }
        };
        let Ok(next_head) = next_head else {
            if let Some(chain) = chains.get_mut(&target) {
                chain.intact = false;
            } else {
                record_orphan_target(orphan_targets, overflow, target);
            }
            continue;
        };
        let (version, hash) = match transition.next_state() {
            riffdb_storage_api::EntityChainStateV1::Live {
                version,
                value_hash,
            } => (version, Some(value_hash)),
            riffdb_storage_api::EntityChainStateV1::Deleted => match transition.prior_state() {
                riffdb_storage_api::EntityChainStateV1::Live { version, .. } => (version, None),
                riffdb_storage_api::EntityChainStateV1::Deleted
                | riffdb_storage_api::EntityChainStateV1::NeverExisted => {
                    if let Some(chain) = chains.get_mut(&target) {
                        chain.intact = false;
                    } else {
                        record_orphan_target(orphan_targets, overflow, target);
                    }
                    continue;
                }
            },
            riffdb_storage_api::EntityChainStateV1::NeverExisted => {
                if let Some(chain) = chains.get_mut(&target) {
                    chain.intact = false;
                } else {
                    record_orphan_target(orphan_targets, overflow, target);
                }
                continue;
            }
        };
        if let Some(chain) = chains.get_mut(&target) {
            chain.version = version;
            chain.hash = hash;
            chain.expected_bundle = None;
        } else if chains.len() < capacity {
            chains.insert(
                target.clone(),
                EntityChain {
                    version,
                    hash,
                    expected_bundle: None,
                    migration_cursor: 0,
                    intact: true,
                    consumed: false,
                },
            );
        } else {
            record_orphan_target(orphan_targets, overflow, target);
            continue;
        }
        heads.insert(target, next_head);
    }
    Ok(())
}

fn validate_transition_chain_heads(
    transaction: &ReadTransaction,
    chains: &mut std::collections::BTreeMap<riffdb_storage_api::EntityTarget, EntityChain>,
    expected_heads: &std::collections::BTreeMap<
        riffdb_storage_api::EntityTarget,
        riffdb_storage_api::EntityChainHeadV1,
    >,
    history_accounts_for_every_head: bool,
) -> Result<(), StorageError> {
    let stored_heads = transaction
        .open_table(crate::layout::ENTITY_CHAIN_HEADS)
        .map_err(table_error)?;
    let entities = transaction.open_table(ENTITIES).map_err(table_error)?;
    // An exact checkpoint snapshot followed only by transition-bearing
    // commits must account for every durable head. On a mixed-era fallback,
    // however, a current head may legitimately come from a frozen legacy
    // commit with no transition payload; the full entity/reference walk
    // remains authoritative for that identity until a later exact checkpoint.
    if history_accounts_for_every_head
        && stored_heads.len().map_err(precommit_storage_error)?
            != u64::try_from(expected_heads.len()).map_err(|_| limit_exceeded())?
    {
        return Err(corrupt());
    }
    for (target, expected) in expected_heads {
        let key = keys::encode_entity_key(target.key());
        let actual = stored_heads
            .get(key)
            .map_err(precommit_storage_error)?
            .map(|encoded| riffdb_storage_api::decode_entity_chain_head_v1(encoded.value()))
            .transpose();
        let exact = matches!(actual, Ok(Some(ref decoded)) if decoded.value() == expected);
        let current_presence_matches = match expected.state() {
            riffdb_storage_api::EntityChainStateV1::Live { .. } => true,
            riffdb_storage_api::EntityChainStateV1::Deleted => entities
                .get(key)
                .map_err(precommit_storage_error)?
                .is_none(),
            riffdb_storage_api::EntityChainStateV1::NeverExisted => false,
        };
        let Some(chain) = chains.get_mut(target) else {
            continue;
        };
        chain.intact &= exact && current_presence_matches;
        if exact
            && current_presence_matches
            && expected.state() == riffdb_storage_api::EntityChainStateV1::Deleted
        {
            // A checked deleted head is the exact current-state witness; no
            // ENTITIES row should consume this historical identity.
            chain.consumed = true;
        }
    }
    Ok(())
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
            (None, None) => false,
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
    command_capsules: &std::collections::BTreeMap<
        CommitSequence,
        riffdb_storage_api::StoredCommandCapsuleV1,
    >,
    event: &riffdb_storage_api::StoredDurableEventV1,
) -> Result<bool, StorageError> {
    let Some(commit) = get_commit_cached(
        transaction,
        command_capsules,
        event.event_id().commit_sequence(),
    )?
    else {
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
        && {
            let mut affected = provenance.affected_entities().iter();
            commit.entity_references().iter().all(|reference| {
                affected.any(|item| {
                    item.target() == reference.target()
                        && item.entity_version() == reference.entity_version()
                })
            })
        }
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
    let table = transaction.open_table(AUDIT).map_err(table_error)?;
    let Some(value) = table.get(key.as_slice()).map_err(precommit_storage_error)? else {
        return Ok(Ok(None));
    };
    Ok(decoded(decode_administration_audit_in_read(
        transaction,
        value.value(),
    ))
    .map(Some))
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
        let Ok(record) = decoded(decode_administration_audit_in_read(
            transaction,
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
        // Intentionally PERMISSIVE relative to the append side, and deliberately
        // one-sided. `riffdb-storage-api` also refuses a `DeployReactiveModule`
        // linkless success when a NEW record is appended, because every reactive
        // publication success is linked. This pass validates records that are
        // already durable, so it keeps the released allowance: a database written
        // under it must still open. Do not mirror the append tightening here --
        // that converts a stale audit shape into a refused daemon start, which is
        // strictly worse than tolerating it. Same shape as the
        // `ApplyContractMigration` asymmetry pinned in `riffdb-service`.
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
            let provenance_side = match get_provenance(transaction, provenance_id)? {
                Some(provenance) => provenance.commit_sequence() == commit_sequence,
                // ADR-0102: a segmented command owns no independent provenance
                // row -- provenance identity is a rebuildable index and the
                // authoritative record lives inside the canonical segment
                // member. Reciprocate against that member instead of reading a
                // row the segmented writer never emits.
                // `known_command_and_control_plane_replays_keep_the_exact_succeeded_link`
                // goes red if this arm is removed.
                None => get_command_capsule(transaction, commit_sequence)?.is_some_and(|capsule| {
                    capsule.provenance().provenance_id() == provenance_id
                        && capsule.provenance().commit_sequence() == commit_sequence
                }),
            };
            Ok(commit_side && provenance_side)
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
                    riffdb_storage_api::StoredAdministrationAuditRecordV1::ReactiveModule(_),
                    riffdb_types::ServiceOperationV1::DeployReactiveModule,
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
    let head = match command_authority_head(&table, &events) {
        Ok(head) => head,
        Err(error) if error.kind() == StorageErrorKind::CorruptData => return Ok(false),
        Err(error) => return Err(error),
    };
    let expected = match head {
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
        Some(sequence) => sequence.checked_next().map_or(
            ApplicationSequenceAllocator::Exhausted,
            ApplicationSequenceAllocator::next,
        ),
    };
    Ok(allocator == expected)
}

/// Proves the physical AUDIT row and the segment-derived record sharing one
/// administration sequence are the same record carried twice.
///
/// A two-phase admitted command writes its `Started` row physically when the
/// durable `Pending` admission is created, and the later command segment derives
/// that same already-durable record through its manifest. The two carriers are
/// then both present at one sequence. That is the ordinary equal-pair join every
/// other physical/derived audit reader already applies — see
/// `administration.rs::read_administration_record_readonly`,
/// `service_lifecycle_in_write`, and `validate_administration_tail`, which all
/// resolve `(Some(physical), Some(derived)) if physical == derived` to one
/// record and reject a disagreeing pair.
///
/// Equality is what makes the pair one record. Bytes that disagree, or that
/// cannot be decoded at all, prove nothing and leave the caller refusing; the
/// audit-row inspection phase classifies a malformed row on its own terms.
fn physical_audit_row_repeats_derived(
    transaction: &ReadTransaction,
    encoded: &[u8],
    derived: &riffdb_storage_api::StoredServiceAuditRecordV1,
) -> Result<bool, StorageError> {
    let commits = transaction.open_table(COMMITS).map_err(table_error)?;
    let events = transaction.open_table(EVENTS).map_err(table_error)?;
    let Ok(physical) =
        codec::decode_administration_audit_with_command_tables(encoded, &commits, &events)
    else {
        return Ok(false);
    };
    Ok(physical.into_parts().0
        == riffdb_storage_api::StoredAdministrationAuditRecordV1::Service(derived.clone()))
}

fn administration_allocator_matches(
    transaction: &ReadTransaction,
    allocator: riffdb_storage_api::AdministrationSequenceAllocator,
    checkpoint: Option<&crate::validated_prefix::ActiveCheckpoint>,
    derived: &std::collections::BTreeMap<riffdb_types::AdministrationSequence, CachedCommandAudit>,
) -> Result<bool, StorageError> {
    let table = transaction.open_table(AUDIT).map_err(table_error)?;
    let mut derived_sequences = derived.keys().copied().peekable();
    let mut expected = checkpoint.map_or_else(
        || Some(riffdb_types::AdministrationSequence::first()),
        |checkpoint| {
            (!checkpoint.retained.administration_sequence_exhausted)
                .then(|| {
                    riffdb_types::AdministrationSequence::new(
                        checkpoint.retained.next_administration_sequence,
                    )
                })
                .flatten()
        },
    );
    // `audit_sequence_bound` is the last *physical* AUDIT row. Segmented
    // commands carry later administration records inside COMMITS, so the
    // retained allocator can legitimately be more than one past that physical
    // bound. The checkpoint self-hash binds both facts; require only that its
    // next logical sequence is strictly above every bound physical row.
    if checkpoint.is_some_and(|checkpoint| {
        !checkpoint.retained.administration_sequence_exhausted
            && checkpoint.retained.next_administration_sequence <= checkpoint.audit_sequence_bound
    }) {
        return Ok(false);
    }
    // A two-phase command may be admitted before S and commit after S. Its
    // suffix command capsule repeats the Started audit that the checkpoint
    // already covered physically. Retain that cached member for reciprocal
    // command validation, but do not count it a second time when advancing the
    // allocator from the checkpoint's exact next logical sequence.
    if let (Some(checkpoint), Some(expected)) = (checkpoint, expected) {
        while derived_sequences
            .peek()
            .is_some_and(|sequence| *sequence < expected)
        {
            let Some(covered) = derived_sequences.next() else {
                return Ok(false);
            };
            if covered.get() > checkpoint.audit_sequence_bound {
                return Ok(false);
            }
        }
    }
    let mut accept = |sequence| {
        if expected != Some(sequence) {
            return false;
        }
        expected = sequence.checked_next();
        true
    };
    let mut physical_rows = if let Some(checkpoint) = checkpoint {
        if checkpoint.audit_sequence_bound == 0 {
            table.iter().map_err(precommit_storage_error)?
        } else {
            let lower = keys::encode_audit_key(
                riffdb_types::AdministrationSequence::new(checkpoint.audit_sequence_bound)
                    .ok_or_else(invariant)?,
            );
            table
                .range::<&[u8]>((Excluded(lower.as_slice()), Unbounded))
                .map_err(precommit_storage_error)?
        }
    } else {
        table.iter().map_err(precommit_storage_error)?
    };
    for entry in &mut physical_rows {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let Ok(physical) = keys::decode_audit_key(key.value()) else {
            return Ok(false);
        };
        while derived_sequences
            .peek()
            .is_some_and(|derived| *derived < physical)
        {
            let Some(derived) = derived_sequences.next() else {
                return Ok(false);
            };
            if !accept(derived) {
                return Ok(false);
            }
        }
        if derived_sequences
            .peek()
            .is_some_and(|derived| *derived == physical)
        {
            // One record with two carriers occupies one sequence, so consume the
            // derived entry and count the pair once. Physical and derived audit
            // sequences are not disjoint: two-phase admission makes the overlap
            // legitimate. Only a provably equal pair collapses — a disagreeing
            // pair falls through to the refusal below, exactly as a gap does.
            let Some(derived_record) = derived.get(&physical).map(|cached| &cached.record) else {
                return Ok(false);
            };
            if !physical_audit_row_repeats_derived(transaction, value.value(), derived_record)? {
                return Ok(false);
            }
            derived_sequences.next();
        }
        if !accept(physical) {
            return Ok(false);
        }
    }
    for sequence in derived_sequences {
        if !accept(sequence) {
            return Ok(false);
        }
    }
    let exact = expected.map_or(
        riffdb_storage_api::AdministrationSequenceAllocator::Exhausted,
        riffdb_storage_api::AdministrationSequenceAllocator::next,
    );
    Ok(allocator == exact)
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
    let length = u32::try_from(lineage.as_bytes().len()).unwrap_or_else(|_| std::process::abort());
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(lineage.as_bytes());
}

fn push_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    let length = u32::try_from(bytes.len()).unwrap_or_else(|_| std::process::abort());
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
mod tests;
