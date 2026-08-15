//! Bounded engine-mechanics evidence for the command-growth benchmark.

use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::num::NonZeroU64;
use std::path::Path;
use std::time::{Duration, Instant};

use redb::{
    Database, Durability, ReadOnlyDatabase, ReadableDatabase, ReadableTable, WriteTransaction,
};
use riffdb_storage_api::{
    AuditPrincipalV1, DatabaseInitializationPort, DatabaseInitializationResult,
    ServiceAuditAppendIntentV1, ServiceAuditAppendRepository, ServiceAuditAppendResult,
};
use riffdb_types::{
    ActorId, ActorKind, AdministrationSequence, CapabilityId, CommitSequence, DatabaseId,
    RequestId, ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetsV1,
    ServiceIngressKindV1, ServiceOperationV1, Timestamp,
};

use crate::journal::{
    EncodedJournalFrame, JournalFrame, JournalMutation, JournalMutationBuffer, JournalTable,
};
use crate::layout::{
    AUDIT, AUDIT_BY_REQUEST, COMMITS, ENTITIES, EVENT_ROUTES, EVENTS, IDEMPOTENCY,
    IDEMPOTENCY_PENDING, INDEX_EPOCHS, META, META_APPLICATION_SEQUENCE, OUTBOX, PROVENANCE,
    SECONDARY_INDEXES, create_all_tables,
};
use crate::store::{RedbDormantPorts, RedbOperationalPorts, RedbStore};

const MAX_WINDOW_COMMANDS: usize = 4_096;
pub const MAX_GROUP_COMMANDS: usize = riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS;
const PENDING_VALUE_BYTES: usize = 512;
const ENTITY_VALUE_BYTES: usize = 768;
const INDEX_VALUE_BYTES: usize = 256;
const EPOCH_VALUE_BYTES: usize = 128;
const PROVENANCE_VALUE_BYTES: usize = 768;
const EVENT_VALUE_BYTES: usize = 512;
const EVENT_ROUTE_VALUE_BYTES: usize = 96;
const OUTBOX_VALUE_BYTES: usize = 640;
const COMMIT_VALUE_BYTES: usize = 1_024;
const OUTCOME_VALUE_BYTES: usize = 768;
const AUDIT_VALUE_BYTES: usize = 192;
const AUDIT_REQUEST_VALUE_BYTES: usize = 96;

/// Redb-owned bounded statistics for one authoritative command-path table.
///
/// These values are diagnostic only. They neither enter authorization nor
/// alter the storage format, and table names come from a closed inventory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthoritativeTableInventoryV1 {
    name: &'static str,
    rows: u64,
    tree_height: u32,
    leaf_pages: u64,
    branch_pages: u64,
    stored_bytes: u64,
    metadata_bytes: u64,
    fragmented_bytes: u64,
}

impl AuthoritativeTableInventoryV1 {
    /// Closed physical table name.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Logical rows currently retained in the table.
    #[must_use]
    pub const fn rows(&self) -> u64 {
        self.rows
    }

    /// Maximum redb tree traversal depth.
    #[must_use]
    pub const fn tree_height(&self) -> u32 {
        self.tree_height
    }

    /// Leaf pages currently owned by the table.
    #[must_use]
    pub const fn leaf_pages(&self) -> u64 {
        self.leaf_pages
    }

    /// Branch pages currently owned by the table.
    #[must_use]
    pub const fn branch_pages(&self) -> u64 {
        self.branch_pages
    }

    /// Exact retained key and value bytes reported by redb.
    #[must_use]
    pub const fn stored_bytes(&self) -> u64 {
        self.stored_bytes
    }

    /// Internal branch-key and table metadata bytes reported by redb.
    #[must_use]
    pub const fn metadata_bytes(&self) -> u64 {
        self.metadata_bytes
    }

    /// Fragmented bytes attributed to the table by redb.
    #[must_use]
    pub const fn fragmented_bytes(&self) -> u64 {
        self.fragmented_bytes
    }
}

/// Reads the closed command-path table inventory from a cleanly stopped
/// database without opening a writer or performing recovery.
pub fn authoritative_table_inventory_v1(
    path: &Path,
) -> Result<Vec<AuthoritativeTableInventoryV1>, EngineBenchmarkError> {
    let database = ReadOnlyDatabase::open(path).map_err(|_| EngineBenchmarkError::Engine)?;
    let transaction = database
        .begin_read()
        .map_err(|_| EngineBenchmarkError::Engine)?;
    let mut inventory = Vec::with_capacity(13);
    let meta = transaction
        .open_table(META)
        .map_err(|_| EngineBenchmarkError::Engine)?;
    inventory.push(table_inventory("meta", &meta)?);
    drop(meta);
    for (name, definition) in [
        ("entities", ENTITIES),
        ("secondary_indexes", SECONDARY_INDEXES),
        ("index_epochs", INDEX_EPOCHS),
        ("idempotency", IDEMPOTENCY),
        ("idempotency_pending", IDEMPOTENCY_PENDING),
        ("commits", COMMITS),
        ("provenance", PROVENANCE),
        ("events", EVENTS),
        ("event_routes", EVENT_ROUTES),
        ("outbox", OUTBOX),
        ("audit", AUDIT),
        ("audit_by_request", AUDIT_BY_REQUEST),
    ] {
        let table = transaction
            .open_table(definition)
            .map_err(|_| EngineBenchmarkError::Engine)?;
        inventory.push(table_inventory(name, &table)?);
    }
    Ok(inventory)
}

fn table_inventory<K, V>(
    name: &'static str,
    table: &impl ReadableTable<K, V>,
) -> Result<AuthoritativeTableInventoryV1, EngineBenchmarkError>
where
    K: redb::Key + 'static,
    V: redb::Value + 'static,
{
    let stats = table.stats().map_err(|_| EngineBenchmarkError::Engine)?;
    Ok(AuthoritativeTableInventoryV1 {
        name,
        rows: table.len().map_err(|_| EngineBenchmarkError::Engine)?,
        tree_height: stats.tree_height(),
        leaf_pages: stats.leaf_pages(),
        branch_pages: stats.branch_pages(),
        stored_bytes: stats.stored_bytes(),
        metadata_bytes: stats.metadata_bytes(),
        fragmented_bytes: stats.fragmented_bytes(),
    })
}

/// Experimental engine durability used only by the benchmark harness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EngineDurability {
    /// No durability; isolates page/table work from durable flush work.
    None,
    /// One-phase immediate durability.
    ImmediateOnePhase,
    /// Hardened RiffDB recovery-oracle mechanics: two-phase immediate durability.
    ImmediateTwoPhase,
}

/// Experimental durable-record staging order used only by the benchmark harness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EngineStagingOrder {
    /// Applies one command's complete table graph before the next command.
    CommandMajor,
    /// Opens each table once and applies the complete command group to that table.
    TableMajor,
    /// Models the current fused capsule layout without a durable Pending transition.
    FusedCurrentCapsule,
    /// Models one segmented capsule authority plus only independently mutable rows.
    /// This is benchmark-only evidence for a proposed durable-layout ADR.
    FusedSegmentedCapsule,
    /// Models one state-bearing segment containing entity and index transitions.
    /// This is benchmark-only evidence and is not a production storage mode.
    FusedStateBearingSegment,
}

impl EngineStagingOrder {
    /// Stable report label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::CommandMajor => "command_major",
            Self::TableMajor => "table_major",
            Self::FusedCurrentCapsule => "fused_current_capsule",
            Self::FusedSegmentedCapsule => "fused_segmented_capsule",
            Self::FusedStateBearingSegment => "fused_state_bearing_segment",
        }
    }
}

impl EngineDurability {
    /// Stable report label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ImmediateOnePhase => "immediate_one_phase",
            Self::ImmediateTwoPhase => "immediate_two_phase",
        }
    }
}

/// Checked mechanics profile for one bounded window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineMechanicsProfile {
    durability: EngineDurability,
    group_commands: usize,
    staging_order: EngineStagingOrder,
}

impl EngineMechanicsProfile {
    /// Creates a bounded profile. Grouping is experimental and never changes production.
    pub fn new(
        durability: EngineDurability,
        group_commands: usize,
    ) -> Result<Self, EngineBenchmarkError> {
        if group_commands == 0 || group_commands > MAX_GROUP_COMMANDS {
            return Err(EngineBenchmarkError::InvalidConfiguration);
        }
        Ok(Self {
            durability,
            group_commands,
            staging_order: EngineStagingOrder::CommandMajor,
        })
    }

    /// Selects the benchmark-only record staging order.
    #[must_use]
    pub const fn with_staging_order(mut self, staging_order: EngineStagingOrder) -> Self {
        self.staging_order = staging_order;
        self
    }

    /// Returns the selected durability.
    #[must_use]
    pub const fn durability(self) -> EngineDurability {
        self.durability
    }

    /// Returns the maximum commands staged per experimental transaction.
    #[must_use]
    pub const fn group_commands(self) -> usize {
        self.group_commands
    }

    /// Returns the selected durable-record staging order.
    #[must_use]
    pub const fn staging_order(self) -> EngineStagingOrder {
        self.staging_order
    }
}

/// Timing decomposition for one growing-database window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineMechanicsSample {
    commands: usize,
    admission_work: Duration,
    admission_commit: Duration,
    terminal_work: Duration,
    terminal_commit: Duration,
    elapsed: Duration,
    file_bytes: u64,
}

impl EngineMechanicsSample {
    /// Commands completed in the window.
    #[must_use]
    pub const fn commands(self) -> usize {
        self.commands
    }

    /// Time spent staging pending admissions.
    #[must_use]
    pub const fn admission_work(self) -> Duration {
        self.admission_work
    }

    /// Time spent in pending-admission commit calls.
    #[must_use]
    pub const fn admission_commit(self) -> Duration {
        self.admission_commit
    }

    /// Time spent opening tables and staging terminal record graphs.
    #[must_use]
    pub const fn terminal_work(self) -> Duration {
        self.terminal_work
    }

    /// Time spent in terminal commit calls.
    #[must_use]
    pub const fn terminal_commit(self) -> Duration {
        self.terminal_commit
    }

    /// Complete measured window duration.
    #[must_use]
    pub const fn elapsed(self) -> Duration {
        self.elapsed
    }

    /// Database file size after the window.
    #[must_use]
    pub const fn file_bytes(self) -> u64 {
        self.file_bytes
    }
}

/// Closed physical projection compared by the WP-485 mechanics probe.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateSegmentProjection {
    /// ADR-0102 production shape: entity and secondary-index state remain rows.
    CurrentSegmentedAuthority,
    /// Proposed one-segment state authority. Never selected by production code.
    BenchmarkStateBearingSegment,
}

impl StateSegmentProjection {
    /// Stable evidence label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::CurrentSegmentedAuthority => "current_segmented_authority",
            Self::BenchmarkStateBearingSegment => "benchmark_state_bearing_segment",
        }
    }
}

/// Closed current-state mutation mix used by the WP-485 mechanics probe.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateSegmentWorkload {
    /// Every command creates a previously absent entity and index member.
    DistinctCreates,
    /// Every command overwrites a member in the retained current-state set.
    RetainedUpdates,
    /// Three creates followed by one retained update, repeated in FIFO order.
    MixedCreatesAndUpdates,
}

impl StateSegmentWorkload {
    /// Stable evidence label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::DistinctCreates => "distinct_creates",
            Self::RetainedUpdates => "retained_updates",
            Self::MixedCreatesAndUpdates => "mixed_75_create_25_update",
        }
    }
}

/// One benchmark-only state-segment projection measurement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateSegmentProjectionSample {
    projection: StateSegmentProjection,
    workload: StateSegmentWorkload,
    commands: usize,
    group_commands: usize,
    retained_entities: usize,
    table_work: Duration,
    final_durable_barrier: Duration,
    elapsed: Duration,
    file_bytes_before: u64,
    file_bytes_after: u64,
    inventory: Vec<AuthoritativeTableInventoryV1>,
}

impl StateSegmentProjectionSample {
    /// Physical projection measured.
    #[must_use]
    pub const fn projection(&self) -> StateSegmentProjection {
        self.projection
    }

    /// Mutation mix measured.
    #[must_use]
    pub const fn workload(&self) -> StateSegmentWorkload {
        self.workload
    }

    /// Commands completed.
    #[must_use]
    pub const fn commands(&self) -> usize {
        self.commands
    }

    /// Maximum commands per physical group.
    #[must_use]
    pub const fn group_commands(&self) -> usize {
        self.group_commands
    }

    /// Retained current-state population present before measurement.
    #[must_use]
    pub const fn retained_entities(&self) -> usize {
        self.retained_entities
    }

    /// Redb begin/stage/non-durable-commit work, excluding the final durability fence.
    #[must_use]
    pub const fn table_work(&self) -> Duration {
        self.table_work
    }

    /// Final common durability barrier used only to make inventory inspectable.
    #[must_use]
    pub const fn final_durable_barrier(&self) -> Duration {
        self.final_durable_barrier
    }

    /// Complete measured window including the common final durability barrier.
    #[must_use]
    pub const fn elapsed(&self) -> Duration {
        self.elapsed
    }

    /// Redb file bytes after retained-state setup and before the measured window.
    #[must_use]
    pub const fn file_bytes_before(&self) -> u64 {
        self.file_bytes_before
    }

    /// Redb file bytes after the measured window and final durability barrier.
    #[must_use]
    pub const fn file_bytes_after(&self) -> u64 {
        self.file_bytes_after
    }

    /// Closed per-table inventory after the measured window.
    #[must_use]
    pub fn inventory(&self) -> &[AuthoritativeTableInventoryV1] {
        &self.inventory
    }
}

/// One benchmark-only comparison of synchronous redb apply and a journal-backed overlay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalOverlayMechanicsSample {
    workload: StateSegmentWorkload,
    commands: usize,
    group_commands: usize,
    retained_entities: usize,
    current_frame_build: Duration,
    current_redb_apply: Duration,
    overlay_frame_build: Duration,
    overlay_apply: Duration,
    control_point_reads: Duration,
    overlay_point_reads: Duration,
    control_page_reads: Duration,
    overlay_page_reads: Duration,
    checkpoint: Duration,
    current_process_write_bytes: u64,
    overlay_process_write_bytes: u64,
    checkpoint_process_write_bytes: u64,
    encoded_journal_bytes: u64,
    mutation_census: Vec<JournalMutationCensusV1>,
    overlay_bytes: u64,
    overlay_transition_count: usize,
    checkpointed_transition_count: usize,
    overlay_rows_after_checkpoint: usize,
    point_read_checksum: u64,
    control_point_read_checksum: u64,
    page_read_checksum: u64,
    control_page_read_checksum: u64,
}

impl JournalOverlayMechanicsSample {
    /// Mutation mix measured.
    #[must_use]
    pub const fn workload(&self) -> StateSegmentWorkload {
        self.workload
    }

    /// Commands represented by the journal suffix.
    #[must_use]
    pub const fn commands(&self) -> usize {
        self.commands
    }

    /// Commands represented by one physical frame.
    #[must_use]
    pub const fn group_commands(&self) -> usize {
        self.group_commands
    }

    /// Current-state rows in the checkpoint before the measured suffix.
    #[must_use]
    pub const fn retained_entities(&self) -> usize {
        self.retained_entities
    }

    /// Exact journal mutation and frame construction for the current control.
    #[must_use]
    pub const fn current_frame_build(&self) -> Duration {
        self.current_frame_build
    }

    /// Redb begin, stage, and non-durable commit work before the control fence.
    #[must_use]
    pub const fn current_redb_apply(&self) -> Duration {
        self.current_redb_apply
    }

    /// Exact journal mutation and frame construction for the overlay candidate.
    #[must_use]
    pub const fn overlay_frame_build(&self) -> Duration {
        self.overlay_frame_build
    }

    /// Immutable overlay insertion and publication-construction work.
    #[must_use]
    pub const fn overlay_apply(&self) -> Duration {
        self.overlay_apply
    }

    /// Checkpoint-only point-read control time.
    #[must_use]
    pub const fn control_point_reads(&self) -> Duration {
        self.control_point_reads
    }

    /// Checkpoint-plus-overlay point-read time.
    #[must_use]
    pub const fn overlay_point_reads(&self) -> Duration {
        self.overlay_point_reads
    }

    /// Checkpoint-only bounded index-page control time.
    #[must_use]
    pub const fn control_page_reads(&self) -> Duration {
        self.control_page_reads
    }

    /// Checkpoint-plus-overlay bounded merge time.
    #[must_use]
    pub const fn overlay_page_reads(&self) -> Duration {
        self.overlay_page_reads
    }

    /// One immediate checkpoint applying the complete suffix to redb.
    #[must_use]
    pub const fn checkpoint(&self) -> Duration {
        self.checkpoint
    }

    /// Process write bytes caused by the current redb apply window.
    #[must_use]
    pub const fn current_process_write_bytes(&self) -> u64 {
        self.current_process_write_bytes
    }

    /// Process write bytes caused by journal construction plus overlay apply.
    #[must_use]
    pub const fn overlay_process_write_bytes(&self) -> u64 {
        self.overlay_process_write_bytes
    }

    /// Process write bytes caused by the bounded redb checkpoint.
    #[must_use]
    pub const fn checkpoint_process_write_bytes(&self) -> u64 {
        self.checkpoint_process_write_bytes
    }

    /// Canonical encoded journal-frame bytes built by the candidate.
    #[must_use]
    pub const fn encoded_journal_bytes(&self) -> u64 {
        self.encoded_journal_bytes
    }

    /// Closed per-table logical mutation-byte census before frame encoding.
    #[must_use]
    pub fn mutation_census(&self) -> &[JournalMutationCensusV1] {
        &self.mutation_census
    }

    /// Conservative keys, values, tombstones, and map-node charge.
    #[must_use]
    pub const fn overlay_bytes(&self) -> u64 {
        self.overlay_bytes
    }

    /// Logical command transitions represented by the overlay.
    #[must_use]
    pub const fn overlay_transition_count(&self) -> usize {
        self.overlay_transition_count
    }

    /// Logical transitions applied by the bounded checkpoint.
    #[must_use]
    pub const fn checkpointed_transition_count(&self) -> usize {
        self.checkpointed_transition_count
    }

    /// Overlay rows retained after the checkpoint control clears the suffix.
    #[must_use]
    pub const fn overlay_rows_after_checkpoint(&self) -> usize {
        self.overlay_rows_after_checkpoint
    }

    /// Candidate point-read semantic checksum.
    #[must_use]
    pub const fn point_read_checksum(&self) -> u64 {
        self.point_read_checksum
    }

    /// Checkpoint-only point-read semantic checksum.
    #[must_use]
    pub const fn control_point_read_checksum(&self) -> u64 {
        self.control_point_read_checksum
    }

    /// Candidate bounded-page semantic checksum.
    #[must_use]
    pub const fn page_read_checksum(&self) -> u64 {
        self.page_read_checksum
    }

    /// Checkpoint-only bounded-page semantic checksum.
    #[must_use]
    pub const fn control_page_read_checksum(&self) -> u64 {
        self.control_page_read_checksum
    }
}

/// Fixed-cardinality benchmark-only logical mutation census.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalMutationCensusV1 {
    table: &'static str,
    mutations: u64,
    key_bytes: u64,
    value_bytes: u64,
    expected_hash_bytes: u64,
    v1_fixed_header_bytes: u64,
}

impl JournalMutationCensusV1 {
    /// Closed journal table label.
    #[must_use]
    pub const fn table(&self) -> &'static str {
        self.table
    }

    /// Logical mutation count.
    #[must_use]
    pub const fn mutations(&self) -> u64 {
        self.mutations
    }

    /// Canonical key bytes carried by V1.
    #[must_use]
    pub const fn key_bytes(&self) -> u64 {
        self.key_bytes
    }

    /// Canonical value bytes carried by V1.
    #[must_use]
    pub const fn value_bytes(&self) -> u64 {
        self.value_bytes
    }

    /// Semantically required expected-prior hash bytes.
    #[must_use]
    pub const fn expected_hash_bytes(&self) -> u64 {
        self.expected_hash_bytes
    }

    /// Current V1 fixed mutation-header bytes, including absent hash slots.
    #[must_use]
    pub const fn v1_fixed_header_bytes(&self) -> u64 {
        self.v1_fixed_header_bytes
    }
}

fn empty_journal_mutation_census() -> Vec<JournalMutationCensusV1> {
    JournalTable::ALL
        .into_iter()
        .map(|table| JournalMutationCensusV1 {
            table: table.label(),
            mutations: 0,
            key_bytes: 0,
            value_bytes: 0,
            expected_hash_bytes: 0,
            v1_fixed_header_bytes: 0,
        })
        .collect()
}

fn observe_journal_mutations(
    census: &mut [JournalMutationCensusV1],
    mutations: &[JournalMutation],
) -> Result<(), EngineBenchmarkError> {
    const V1_FIXED_MUTATION_HEADER_BYTES: u64 = 44;
    for mutation in mutations {
        let (table, key, value_bytes, has_expected_hash) = match mutation {
            JournalMutation::Put {
                table,
                key,
                expected_hash,
                value,
            } => (*table, key.len(), value.len(), expected_hash.is_some()),
            JournalMutation::Delete { table, key, .. } => (*table, key.len(), 0, true),
        };
        let entry = census
            .get_mut(table as usize - 1)
            .filter(|entry| entry.table == table.label())
            .ok_or(EngineBenchmarkError::Engine)?;
        entry.mutations = entry.mutations.saturating_add(1);
        entry.key_bytes = entry
            .key_bytes
            .saturating_add(u64::try_from(key).unwrap_or(u64::MAX));
        entry.value_bytes = entry
            .value_bytes
            .saturating_add(u64::try_from(value_bytes).unwrap_or(u64::MAX));
        if has_expected_hash {
            entry.expected_hash_bytes = entry.expected_hash_bytes.saturating_add(32);
        }
        entry.v1_fixed_header_bytes = entry
            .v1_fixed_header_bytes
            .saturating_add(V1_FIXED_MUTATION_HEADER_BYTES);
    }
    Ok(())
}

#[derive(Default)]
struct BenchmarkJournalOverlay {
    tables: [BTreeMap<Vec<u8>, Option<Vec<u8>>>; 14],
    charged_bytes: u64,
}

impl BenchmarkJournalOverlay {
    fn apply(&mut self, mutation: &JournalMutation) -> Result<(), EngineBenchmarkError> {
        const MAP_NODE_CHARGE: u64 = 48;
        let (table, key, value) = match mutation {
            JournalMutation::Put {
                table, key, value, ..
            } => (*table, key.as_ref(), Some(value.as_ref())),
            JournalMutation::Delete { table, key, .. } => (*table, key.as_ref(), None),
        };
        let index = table as usize;
        let replacement = value.map(<[u8]>::to_vec);
        let prior = self.tables[index].insert(key.to_vec(), replacement.clone());
        if let Some(prior) = prior {
            self.charged_bytes = self
                .charged_bytes
                .saturating_sub(overlay_entry_charge(key, prior.as_deref()));
        }
        self.charged_bytes = self
            .charged_bytes
            .saturating_add(overlay_entry_charge(key, replacement.as_deref()))
            .saturating_add(MAP_NODE_CHARGE);
        Ok(())
    }

    fn table(&self, table: JournalTable) -> &BTreeMap<Vec<u8>, Option<Vec<u8>>> {
        &self.tables[table as usize]
    }

    fn rows(&self) -> usize {
        self.tables.iter().map(BTreeMap::len).sum()
    }

    fn clear(&mut self) {
        self.tables.iter_mut().for_each(BTreeMap::clear);
        self.charged_bytes = 0;
    }
}

fn overlay_entry_charge(key: &[u8], value: Option<&[u8]>) -> u64 {
    u64::try_from(key.len())
        .unwrap_or(u64::MAX)
        .saturating_add(u64::try_from(value.map_or(1, <[u8]>::len)).unwrap_or(u64::MAX))
}

/// Runs the WP-486 benchmark-only journal-overlay mechanics comparison.
///
/// This entry point is feature-gated with the rest of benchmark support and
/// cannot be selected by a production storage adapter.
pub fn run_journal_overlay_mechanics_window(
    path: &Path,
    workload: StateSegmentWorkload,
    commands: usize,
    group_commands: usize,
    retained_entities: usize,
) -> Result<JournalOverlayMechanicsSample, EngineBenchmarkError> {
    if commands == 0
        || commands > MAX_WINDOW_COMMANDS
        || group_commands == 0
        || group_commands > MAX_GROUP_COMMANDS
        || retained_entities == 0
        || retained_entities > 65_536
    {
        return Err(EngineBenchmarkError::InvalidConfiguration);
    }
    let overlay_path = path.with_extension("overlay.redb");
    initialize_engine_mechanics(path)?;
    initialize_engine_mechanics(&overlay_path)?;
    let control = Database::create(path).map_err(|_| EngineBenchmarkError::Engine)?;
    let overlay_base = Database::create(&overlay_path).map_err(|_| EngineBenchmarkError::Engine)?;
    seed_state_segment_projection(
        &control,
        StateSegmentProjection::CurrentSegmentedAuthority,
        retained_entities,
    )?;
    seed_state_segment_projection(
        &overlay_base,
        StateSegmentProjection::CurrentSegmentedAuthority,
        retained_entities,
    )?;
    let first_sequence = u64::try_from(retained_entities)
        .map_err(|_| EngineBenchmarkError::Engine)?
        .checked_add(1)
        .ok_or(EngineBenchmarkError::InvalidConfiguration)?;
    let database_id =
        DatabaseId::from_bytes(uuid_v7_bytes(0x71, 0)).map_err(|_| EngineBenchmarkError::Engine)?;

    let mut current_frame_build = Duration::ZERO;
    let mut current_redb_apply = Duration::ZERO;
    let mut current_hash = [0_u8; 32];
    let writes_before_current = process_write_bytes();
    for start in (0..commands).step_by(group_commands) {
        let end = commands.min(start.saturating_add(group_commands));
        let frame_started = Instant::now();
        let (_, frame) = build_overlay_frame(
            database_id,
            current_hash,
            first_sequence,
            start,
            end,
            workload,
            retained_entities,
        )?;
        current_hash = frame.frame_hash();
        current_frame_build = current_frame_build.saturating_add(frame_started.elapsed());

        let apply_started = Instant::now();
        let mut transaction = control
            .begin_write()
            .map_err(|_| EngineBenchmarkError::Engine)?;
        configure(&mut transaction, EngineDurability::None)?;
        stage_current_state_projection(
            &transaction,
            first_sequence,
            start,
            end,
            workload,
            retained_entities,
        )?;
        transaction
            .commit()
            .map_err(|_| EngineBenchmarkError::Engine)?;
        current_redb_apply = current_redb_apply.saturating_add(apply_started.elapsed());
    }
    let writes_after_current = process_write_bytes();

    let mut overlay = BenchmarkJournalOverlay::default();
    let mut overlay_frame_build = Duration::ZERO;
    let mut overlay_apply = Duration::ZERO;
    let mut overlay_hash = [0_u8; 32];
    let mut encoded_journal_bytes = 0_u64;
    let mut mutation_census = empty_journal_mutation_census();
    let writes_before_overlay = process_write_bytes();
    for start in (0..commands).step_by(group_commands) {
        let end = commands.min(start.saturating_add(group_commands));
        let frame_started = Instant::now();
        let (mutations, frame) = build_overlay_frame(
            database_id,
            overlay_hash,
            first_sequence,
            start,
            end,
            workload,
            retained_entities,
        )?;
        overlay_hash = frame.frame_hash();
        encoded_journal_bytes = encoded_journal_bytes
            .saturating_add(u64::try_from(frame.as_bytes().len()).unwrap_or(u64::MAX));
        observe_journal_mutations(&mut mutation_census, &mutations)?;
        overlay_frame_build = overlay_frame_build.saturating_add(frame_started.elapsed());
        let apply_started = Instant::now();
        for mutation in &mutations {
            overlay.apply(mutation)?;
        }
        overlay_apply = overlay_apply.saturating_add(apply_started.elapsed());
    }
    let writes_after_overlay = process_write_bytes();

    const POINT_READS: usize = 16_384;
    const PAGE_READS: usize = 2_048;
    const PAGE_LIMIT: usize = 50;
    let control_read = control
        .begin_read()
        .map_err(|_| EngineBenchmarkError::Engine)?;
    let overlay_read = overlay_base
        .begin_read()
        .map_err(|_| EngineBenchmarkError::Engine)?;
    let point_started = Instant::now();
    let control_point_read_checksum = point_read_checksum(
        &control_read,
        None,
        first_sequence,
        workload,
        retained_entities,
        commands,
        POINT_READS,
    )?;
    let control_point_reads = point_started.elapsed();
    let point_started = Instant::now();
    let point_read_checksum = point_read_checksum(
        &overlay_read,
        Some(&overlay),
        first_sequence,
        workload,
        retained_entities,
        commands,
        POINT_READS,
    )?;
    let overlay_point_reads = point_started.elapsed();
    let page_started = Instant::now();
    let control_page_read_checksum =
        page_read_checksum(&control_read, None, PAGE_READS, PAGE_LIMIT)?;
    let control_page_reads = page_started.elapsed();
    let page_started = Instant::now();
    let page_read_checksum =
        page_read_checksum(&overlay_read, Some(&overlay), PAGE_READS, PAGE_LIMIT)?;
    let overlay_page_reads = page_started.elapsed();
    drop(control_read);
    drop(overlay_read);

    let overlay_bytes = overlay.charged_bytes;
    let writes_before_checkpoint = process_write_bytes();
    let checkpoint_started = Instant::now();
    let mut checkpoint = overlay_base
        .begin_write()
        .map_err(|_| EngineBenchmarkError::Engine)?;
    configure(&mut checkpoint, EngineDurability::ImmediateOnePhase)?;
    for start in (0..commands).step_by(group_commands) {
        let end = commands.min(start.saturating_add(group_commands));
        stage_current_state_projection(
            &checkpoint,
            first_sequence,
            start,
            end,
            workload,
            retained_entities,
        )?;
    }
    checkpoint
        .commit()
        .map_err(|_| EngineBenchmarkError::Engine)?;
    let checkpoint_elapsed = checkpoint_started.elapsed();
    let writes_after_checkpoint = process_write_bytes();
    overlay.clear();

    Ok(JournalOverlayMechanicsSample {
        workload,
        commands,
        group_commands,
        retained_entities,
        current_frame_build,
        current_redb_apply,
        overlay_frame_build,
        overlay_apply,
        control_point_reads,
        overlay_point_reads,
        control_page_reads,
        overlay_page_reads,
        checkpoint: checkpoint_elapsed,
        current_process_write_bytes: writes_after_current.saturating_sub(writes_before_current),
        overlay_process_write_bytes: writes_after_overlay.saturating_sub(writes_before_overlay),
        checkpoint_process_write_bytes: writes_after_checkpoint
            .saturating_sub(writes_before_checkpoint),
        encoded_journal_bytes,
        mutation_census,
        overlay_bytes,
        overlay_transition_count: commands,
        checkpointed_transition_count: commands,
        overlay_rows_after_checkpoint: overlay.rows(),
        point_read_checksum,
        control_point_read_checksum,
        page_read_checksum,
        control_page_read_checksum,
    })
}

fn build_overlay_frame(
    database_id: DatabaseId,
    previous_hash: [u8; 32],
    first_sequence: u64,
    start: usize,
    end: usize,
    workload: StateSegmentWorkload,
    retained_entities: usize,
) -> Result<(Vec<JournalMutation>, EncodedJournalFrame), EngineBenchmarkError> {
    let commands = end
        .checked_sub(start)
        .filter(|count| *count != 0)
        .ok_or(EngineBenchmarkError::InvalidConfiguration)?;
    let command_count = u16::try_from(commands).map_err(|_| EngineBenchmarkError::Engine)?;
    let first = sequence_at(first_sequence, start)?;
    let last = sequence_at(
        first_sequence,
        end.checked_sub(1)
            .ok_or(EngineBenchmarkError::InvalidConfiguration)?,
    )?;
    let predecessor = CommitSequence::new(
        first
            .checked_sub(1)
            .ok_or(EngineBenchmarkError::InvalidConfiguration)?,
    )
    .ok_or(EngineBenchmarkError::InvalidConfiguration)?;
    let covered = CommitSequence::new(last).ok_or(EngineBenchmarkError::InvalidConfiguration)?;
    let prior_admin_value = u64::try_from(start)
        .map_err(|_| EngineBenchmarkError::Engine)?
        .checked_add(u64::try_from(retained_entities).map_err(|_| EngineBenchmarkError::Engine)?)
        .and_then(|value| value.checked_mul(2))
        .ok_or(EngineBenchmarkError::InvalidConfiguration)?;
    let covered_admin_value = prior_admin_value
        .checked_add(
            u64::try_from(commands)
                .map_err(|_| EngineBenchmarkError::Engine)?
                .checked_mul(2)
                .ok_or(EngineBenchmarkError::InvalidConfiguration)?,
        )
        .ok_or(EngineBenchmarkError::InvalidConfiguration)?;
    let predecessor_admin = AdministrationSequence::new(prior_admin_value)
        .ok_or(EngineBenchmarkError::InvalidConfiguration)?;
    let covered_admin = AdministrationSequence::new(covered_admin_value)
        .ok_or(EngineBenchmarkError::InvalidConfiguration)?;

    let mut before_segment = Vec::with_capacity(commands.saturating_mul(2).saturating_add(1));
    for offset in start..end {
        let command_sequence = sequence_at(first_sequence, offset)?;
        let state_sequence =
            projection_state_sequence(command_sequence, offset, workload, retained_entities)?;
        for (table, tag, bytes) in [
            (JournalTable::Entities, 0x45, ENTITY_VALUE_BYTES),
            (JournalTable::SecondaryIndexes, 0x49, INDEX_VALUE_BYTES),
        ] {
            let key = record_key(state_sequence, tag);
            let value = record_value(command_sequence, bytes, tag);
            let mutation =
                if state_sequence <= u64::try_from(retained_entities).unwrap_or(u64::MAX) {
                    JournalMutation::replace(
                        table,
                        key,
                        &record_value(state_sequence, bytes, tag),
                        value,
                    )
                } else {
                    JournalMutation::put(table, key, value)
                }
                .map_err(|_| EngineBenchmarkError::Engine)?;
            before_segment.push(mutation);
        }
    }
    let epoch_key = record_key(first, 0x58);
    let epoch_value = record_value(first, EPOCH_VALUE_BYTES, 0x58);
    before_segment.push(
        JournalMutation::put(JournalTable::IndexEpochs, epoch_key, epoch_value)
            .map_err(|_| EngineBenchmarkError::Engine)?,
    );
    let segment_key = record_key(first, 0x43);
    let segment_value = record_value(first, segmented_capsule_bytes(commands)?, 0x43);
    let segment_mutation =
        JournalMutation::put(JournalTable::Commits, segment_key, segment_value.clone())
            .map_err(|_| EngineBenchmarkError::Engine)?;
    let prior_allocator = first.to_be_bytes();
    let next_allocator = last
        .checked_add(1)
        .ok_or(EngineBenchmarkError::InvalidConfiguration)?
        .to_be_bytes();
    let allocator_mutation = JournalMutation::replace(
        JournalTable::Meta,
        META_APPLICATION_SEQUENCE.as_bytes(),
        &prior_allocator,
        next_allocator,
    )
    .map_err(|_| EngineBenchmarkError::Engine)?;

    let mut buffer = JournalMutationBuffer::default();
    buffer
        .extend(before_segment.iter().cloned())
        .map_err(|_| EngineBenchmarkError::Engine)?;
    buffer
        .push_command_segment(segment_key, segment_value, commands)
        .map_err(|_| EngineBenchmarkError::Engine)?;
    buffer
        .extend([allocator_mutation.clone()])
        .map_err(|_| EngineBenchmarkError::Engine)?;
    let frame = JournalFrame::encode_buffered_command(
        database_id,
        Some(predecessor),
        Some(covered),
        Some(predecessor_admin),
        Some(covered_admin),
        command_count,
        previous_hash,
        buffer,
    )
    .map_err(|_| EngineBenchmarkError::Engine)?;
    let mut mutations = before_segment;
    mutations.push(segment_mutation);
    mutations.push(allocator_mutation);
    Ok((mutations, frame))
}

fn point_read_checksum(
    transaction: &redb::ReadTransaction,
    overlay: Option<&BenchmarkJournalOverlay>,
    first_sequence: u64,
    workload: StateSegmentWorkload,
    retained_entities: usize,
    commands: usize,
    reads: usize,
) -> Result<u64, EngineBenchmarkError> {
    let table = transaction
        .open_table(ENTITIES)
        .map_err(|_| EngineBenchmarkError::Engine)?;
    let overlay_table = overlay.map(|overlay| overlay.table(JournalTable::Entities));
    let mut checksum = 0_u64;
    for read in 0..reads {
        let offset = read % commands;
        let command_sequence = sequence_at(first_sequence, offset)?;
        let state_sequence =
            projection_state_sequence(command_sequence, offset, workload, retained_entities)?;
        let key = record_key(state_sequence, 0x45);
        let overlay_value = overlay_table.and_then(|table| table.get(key.as_slice()));
        let owned;
        let value = match overlay_value {
            Some(Some(value)) => value.as_slice(),
            Some(None) => return Err(EngineBenchmarkError::Engine),
            None => {
                owned = table
                    .get(key.as_slice())
                    .map_err(|_| EngineBenchmarkError::Engine)?
                    .ok_or(EngineBenchmarkError::Engine)?
                    .value()
                    .to_vec();
                owned.as_slice()
            }
        };
        checksum = checksum_value(checksum, &key, value);
    }
    Ok(checksum)
}

fn page_read_checksum(
    transaction: &redb::ReadTransaction,
    overlay: Option<&BenchmarkJournalOverlay>,
    reads: usize,
    limit: usize,
) -> Result<u64, EngineBenchmarkError> {
    let table = transaction
        .open_table(ENTITIES)
        .map_err(|_| EngineBenchmarkError::Engine)?;
    let empty = BTreeMap::new();
    let overlay_table = overlay
        .map(|overlay| overlay.table(JournalTable::Entities))
        .unwrap_or(&empty);
    let mut checksum = 0_u64;
    for _ in 0..reads {
        let mut base = table.iter().map_err(|_| EngineBenchmarkError::Engine)?;
        let mut delta = overlay_table.iter();
        let mut base_next = match base.next() {
            Some(Ok((key, value))) => Some((key.value().to_vec(), value.value().to_vec())),
            Some(Err(_)) => return Err(EngineBenchmarkError::Engine),
            None => None,
        };
        let mut delta_next = delta.next();
        let mut emitted = 0_usize;
        while emitted < limit && (base_next.is_some() || delta_next.is_some()) {
            match (&base_next, delta_next) {
                (Some((base_key, base_value)), Some((delta_key, _delta_value)))
                    if base_key.as_slice() < delta_key.as_slice() =>
                {
                    checksum = checksum_value(checksum, base_key, base_value);
                    emitted += 1;
                    base_next = match base.next() {
                        Some(Ok((key, value))) => {
                            Some((key.value().to_vec(), value.value().to_vec()))
                        }
                        Some(Err(_)) => return Err(EngineBenchmarkError::Engine),
                        None => None,
                    };
                }
                (Some((base_key, _)), Some((delta_key, delta_value)))
                    if base_key.as_slice() == delta_key.as_slice() =>
                {
                    if let Some(value) = delta_value.as_deref() {
                        checksum = checksum_value(checksum, delta_key, value);
                        emitted += 1;
                    }
                    base_next = match base.next() {
                        Some(Ok((key, value))) => {
                            Some((key.value().to_vec(), value.value().to_vec()))
                        }
                        Some(Err(_)) => return Err(EngineBenchmarkError::Engine),
                        None => None,
                    };
                    delta_next = delta.next();
                }
                (_, Some((delta_key, delta_value))) => {
                    if let Some(value) = delta_value.as_deref() {
                        checksum = checksum_value(checksum, delta_key, value);
                        emitted += 1;
                    }
                    delta_next = delta.next();
                }
                (Some((base_key, base_value)), None) => {
                    checksum = checksum_value(checksum, base_key, base_value);
                    emitted += 1;
                    base_next = match base.next() {
                        Some(Ok((key, value))) => {
                            Some((key.value().to_vec(), value.value().to_vec()))
                        }
                        Some(Err(_)) => return Err(EngineBenchmarkError::Engine),
                        None => None,
                    };
                }
                (None, None) => break,
            }
        }
    }
    Ok(checksum)
}

fn checksum_value(mut checksum: u64, key: &[u8], value: &[u8]) -> u64 {
    for byte in key.iter().chain(value.iter().take(16)) {
        checksum = checksum.rotate_left(5) ^ u64::from(*byte);
    }
    checksum
}

fn process_write_bytes() -> u64 {
    fs::read_to_string("/proc/self/io")
        .ok()
        .and_then(|contents| {
            contents.lines().find_map(|line| {
                line.strip_prefix("write_bytes:")
                    .and_then(|value| value.trim().parse().ok())
            })
        })
        .unwrap_or(0)
}

/// Closed failure from the benchmark-only engine mechanics driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EngineBenchmarkError {
    /// A count was zero or exceeded an accepted bound.
    InvalidConfiguration,
    /// The local database engine or filesystem operation failed.
    Engine,
}

impl fmt::Display for EngineBenchmarkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "engine benchmark configuration is invalid",
            Self::Engine => "engine benchmark operation failed",
        })
    }
}

impl std::error::Error for EngineBenchmarkError {}

/// One measured window through the real durable service-audit repository.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServiceAuditGrowthSample {
    commands: usize,
    preparation: Duration,
    append: Duration,
    file_bytes: u64,
}

impl ServiceAuditGrowthSample {
    /// Complete request lifecycles appended in the window.
    #[must_use]
    pub const fn commands(self) -> usize {
        self.commands
    }

    /// Time spent constructing and semantically validating typed intents.
    #[must_use]
    pub const fn preparation(self) -> Duration {
        self.preparation
    }

    /// Time spent in the repository, including transaction-current reads,
    /// record encoding, table work, and two standard one-phase immediate
    /// durable commits per
    /// request lifecycle.
    #[must_use]
    pub const fn append(self) -> Duration {
        self.append
    }

    /// Database file size after the window.
    #[must_use]
    pub const fn file_bytes(self) -> u64 {
        self.file_bytes
    }
}

/// Persistent benchmark-only handle to the real service-audit repository.
///
/// Construction uses the same standard one-phase immediate operational adapter
/// as the server. It intentionally bypasses only startup evidence orchestration:
/// benchmark setup creates a new empty database whose complete state is known.
pub struct ServiceAuditGrowthHarness {
    path: std::path::PathBuf,
    ports: RedbOperationalPorts,
}

impl ServiceAuditGrowthHarness {
    /// Creates an empty initialized operational fixture.
    pub fn new(path: &Path) -> Result<Self, EngineBenchmarkError> {
        let mut store = RedbStore::open(path).map_err(|_| EngineBenchmarkError::Engine)?;
        let database_id = DatabaseId::from_bytes(uuid_v7_bytes(0x11, 0))
            .map_err(|_| EngineBenchmarkError::Engine)?;
        match store
            .initialize_database(database_id)
            .map_err(|_| EngineBenchmarkError::Engine)?
        {
            DatabaseInitializationResult::Installed(actual) if actual == database_id => {}
            _ => return Err(EngineBenchmarkError::Engine),
        }
        let dormant = RedbDormantPorts {
            shared: store.shared,
        };
        let ports = dormant
            .into_operational_after_catalog_validation()
            .map_err(|_| EngineBenchmarkError::Engine)?;
        Ok(Self {
            path: path.to_path_buf(),
            ports,
        })
    }

    /// Reopens an existing fixture for continued window growth after a measurement drop.
    pub fn reopen(path: &Path) -> Result<Self, EngineBenchmarkError> {
        let store = RedbStore::open(path).map_err(|_| EngineBenchmarkError::Engine)?;
        let dormant = RedbDormantPorts {
            shared: store.shared,
        };
        let ports = dormant
            .into_operational_after_catalog_validation()
            .map_err(|_| EngineBenchmarkError::Engine)?;
        Ok(Self {
            path: path.to_path_buf(),
            ports,
        })
    }

    /// Appends one started/failed lifecycle per command through the real port.
    ///
    /// Uses two independent durable transitions per command (Started, then
    /// Failed). Retained for PERF-013 windows and generation-mode equivalence.
    pub fn run_window(
        &mut self,
        first_command: u64,
        commands: usize,
    ) -> Result<ServiceAuditGrowthSample, EngineBenchmarkError> {
        if first_command == 0 || commands == 0 || commands > MAX_WINDOW_COMMANDS {
            return Err(EngineBenchmarkError::InvalidConfiguration);
        }
        let preparation_started = Instant::now();
        let intents = build_started_failed_intents(first_command, commands)?;
        let preparation = preparation_started.elapsed();

        let append_started = Instant::now();
        for intent in &intents {
            match self
                .ports
                .append_service_audit(intent)
                .map_err(|_| EngineBenchmarkError::Engine)?
            {
                ServiceAuditAppendResult::Appended(_) => {}
                ServiceAuditAppendResult::PhaseConflict => {
                    return Err(EngineBenchmarkError::Engine);
                }
            }
        }
        let append = append_started.elapsed();
        let file_bytes = fs::metadata(&self.path)
            .map_err(|_| EngineBenchmarkError::Engine)?
            .len();
        Ok(ServiceAuditGrowthSample {
            commands,
            preparation,
            append,
            file_bytes,
        })
    }

    /// Appends Started+Failed fused pairs in groups of up to
    /// [`riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS`] commands per durable
    /// transaction via the engine fused staging path (same mechanics as
    /// `append_service_audit_fused_pair`, multi-request).
    ///
    /// History shape matches sequential Started-then-Failed per command (same
    /// administration-sequence order); only the commit batching changes.
    pub fn run_window_grouped_fused(
        &mut self,
        first_command: u64,
        commands: usize,
    ) -> Result<ServiceAuditGrowthSample, EngineBenchmarkError> {
        use crate::administration::stage_service_audit_group_in_write;
        use crate::hooks::RedbTestOperation;

        if first_command == 0 || commands == 0 || commands > MAX_WINDOW_COMMANDS {
            return Err(EngineBenchmarkError::InvalidConfiguration);
        }
        let group_bound = riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS;
        let preparation_started = Instant::now();
        // Pre-build all intents so preparation is comparable to run_window.
        let intents = build_started_failed_intents(first_command, commands)?;
        let preparation = preparation_started.elapsed();

        let append_started = Instant::now();
        let mut offset = 0_usize;
        while offset < intents.len() {
            // Two intents per command; group by command count ≤ group_bound.
            let commands_left = (intents.len() - offset) / 2;
            let group_commands = commands_left.min(group_bound);
            let intent_end = offset
                .checked_add(
                    group_commands
                        .checked_mul(2)
                        .ok_or(EngineBenchmarkError::InvalidConfiguration)?,
                )
                .ok_or(EngineBenchmarkError::InvalidConfiguration)?;
            let group = &intents[offset..intent_end];
            let access = self
                .ports
                .begin_write()
                .map_err(|_| EngineBenchmarkError::Engine)?;
            stage_service_audit_group_in_write(&access, group)
                .map_err(|_| EngineBenchmarkError::Engine)?;
            access
                .commit_for(RedbTestOperation::ServiceAudit)
                .map_err(|_| EngineBenchmarkError::Engine)?;
            offset = intent_end;
        }
        let append = append_started.elapsed();
        let file_bytes = fs::metadata(&self.path)
            .map_err(|_| EngineBenchmarkError::Engine)?
            .len();
        Ok(ServiceAuditGrowthSample {
            commands,
            preparation,
            append,
            file_bytes,
        })
    }
}

fn build_started_failed_intents(
    first_command: u64,
    commands: usize,
) -> Result<Vec<ServiceAuditAppendIntentV1>, EngineBenchmarkError> {
    let mut intents = Vec::with_capacity(
        commands
            .checked_mul(2)
            .ok_or(EngineBenchmarkError::InvalidConfiguration)?,
    );
    for offset in 0..commands {
        let command = sequence_at(first_command, offset)?;
        let request_id = RequestId::from_bytes(uuid_v7_bytes(0x33, command))
            .map_err(|_| EngineBenchmarkError::Engine)?;
        intents.push(service_audit_intent(
            request_id,
            command,
            ServiceAuditPhaseV1::Started,
        )?);
        intents.push(service_audit_intent(
            request_id,
            command,
            ServiceAuditPhaseV1::Failed,
        )?);
    }
    Ok(intents)
}

/// Creates the exact table inventory used by RiffDB and installs durable metadata.
pub fn initialize_engine_mechanics(path: &Path) -> Result<(), EngineBenchmarkError> {
    let database = Database::create(path).map_err(|_| EngineBenchmarkError::Engine)?;
    let mut transaction = database
        .begin_write()
        .map_err(|_| EngineBenchmarkError::Engine)?;
    configure(&mut transaction, EngineDurability::ImmediateTwoPhase)?;
    create_all_tables(&transaction).map_err(|_| EngineBenchmarkError::Engine)?;
    transaction
        .open_table(META)
        .map_err(|_| EngineBenchmarkError::Engine)?
        .insert(META_APPLICATION_SEQUENCE, 1_u64.to_be_bytes().as_slice())
        .map_err(|_| EngineBenchmarkError::Engine)?;
    transaction
        .commit()
        .map_err(|_| EngineBenchmarkError::Engine)
}

/// Runs one WP-485 benchmark-only physical projection window.
///
/// Production adapters cannot select either projection through this API: the
/// complete implementation is feature-gated benchmark support and operates on
/// a caller-owned disposable database path.
pub fn run_state_segment_projection_window(
    path: &Path,
    projection: StateSegmentProjection,
    workload: StateSegmentWorkload,
    commands: usize,
    group_commands: usize,
    retained_entities: usize,
) -> Result<StateSegmentProjectionSample, EngineBenchmarkError> {
    if commands == 0
        || commands > MAX_WINDOW_COMMANDS
        || group_commands == 0
        || group_commands > MAX_GROUP_COMMANDS
        || retained_entities > 65_536
        || (workload != StateSegmentWorkload::DistinctCreates && retained_entities == 0)
    {
        return Err(EngineBenchmarkError::InvalidConfiguration);
    }
    initialize_engine_mechanics(path)?;
    let database = Database::create(path).map_err(|_| EngineBenchmarkError::Engine)?;
    seed_state_segment_projection(&database, projection, retained_entities)?;
    let file_bytes_before = fs::metadata(path)
        .map_err(|_| EngineBenchmarkError::Engine)?
        .len();
    let first_sequence = u64::try_from(retained_entities)
        .map_err(|_| EngineBenchmarkError::Engine)?
        .checked_add(1)
        .ok_or(EngineBenchmarkError::InvalidConfiguration)?;
    let started = Instant::now();
    let mut table_work = Duration::ZERO;
    for group_start in (0..commands).step_by(group_commands) {
        let group_end = commands.min(group_start.saturating_add(group_commands));
        let group_started = Instant::now();
        let mut transaction = database
            .begin_write()
            .map_err(|_| EngineBenchmarkError::Engine)?;
        configure(&mut transaction, EngineDurability::None)?;
        match projection {
            StateSegmentProjection::CurrentSegmentedAuthority => {
                stage_current_state_projection(
                    &transaction,
                    first_sequence,
                    group_start,
                    group_end,
                    workload,
                    retained_entities,
                )?;
            }
            StateSegmentProjection::BenchmarkStateBearingSegment => {
                stage_state_bearing_projection(
                    &transaction,
                    first_sequence,
                    group_start,
                    group_end,
                )?;
            }
        }
        transaction
            .commit()
            .map_err(|_| EngineBenchmarkError::Engine)?;
        table_work = table_work.saturating_add(group_started.elapsed());
    }
    let barrier_started = Instant::now();
    durable_barrier(&database)?;
    let final_durable_barrier = barrier_started.elapsed();
    let elapsed = started.elapsed();
    drop(database);
    let file_bytes_after = fs::metadata(path)
        .map_err(|_| EngineBenchmarkError::Engine)?
        .len();
    let inventory = authoritative_table_inventory_v1(path)?;
    Ok(StateSegmentProjectionSample {
        projection,
        workload,
        commands,
        group_commands,
        retained_entities,
        table_work,
        final_durable_barrier,
        elapsed,
        file_bytes_before,
        file_bytes_after,
        inventory,
    })
}

/// Runs one bounded two-transition command window against a retained database.
///
/// Every command first installs a pending row and reaches the selected commit
/// boundary. Only then does a second transaction replace that row with the same
/// nine-table terminal graph shape used by the authoritative command path.
pub fn run_engine_mechanics_window(
    path: &Path,
    first_sequence: u64,
    commands: usize,
    profile: EngineMechanicsProfile,
) -> Result<EngineMechanicsSample, EngineBenchmarkError> {
    if first_sequence == 0 || commands == 0 || commands > MAX_WINDOW_COMMANDS {
        return Err(EngineBenchmarkError::InvalidConfiguration);
    }
    let command_count = u64::try_from(commands).map_err(|_| EngineBenchmarkError::Engine)?;
    first_sequence
        .checked_add(command_count)
        .ok_or(EngineBenchmarkError::InvalidConfiguration)?;
    let database = Database::create(path).map_err(|_| EngineBenchmarkError::Engine)?;
    let started = Instant::now();
    let mut admission_work = Duration::ZERO;
    let mut admission_commit = Duration::ZERO;
    let mut terminal_work = Duration::ZERO;
    let mut terminal_commit = Duration::ZERO;

    for group_start in (0..commands).step_by(profile.group_commands) {
        let group_end = commands.min(group_start + profile.group_commands);
        if matches!(
            profile.staging_order,
            EngineStagingOrder::CommandMajor | EngineStagingOrder::TableMajor
        ) {
            let work_started = Instant::now();
            let mut transaction = database
                .begin_write()
                .map_err(|_| EngineBenchmarkError::Engine)?;
            configure(&mut transaction, profile.durability)?;
            stage_admissions(&transaction, first_sequence, group_start, group_end)?;
            admission_work = admission_work.saturating_add(work_started.elapsed());
            let commit_started = Instant::now();
            transaction
                .commit()
                .map_err(|_| EngineBenchmarkError::Engine)?;
            admission_commit = admission_commit.saturating_add(commit_started.elapsed());
        }

        let work_started = Instant::now();
        let mut transaction = database
            .begin_write()
            .map_err(|_| EngineBenchmarkError::Engine)?;
        configure(&mut transaction, profile.durability)?;
        match profile.staging_order {
            EngineStagingOrder::CommandMajor => {
                stage_terminals(&transaction, first_sequence, group_start, group_end)?;
            }
            EngineStagingOrder::TableMajor => {
                stage_terminals_table_major(&transaction, first_sequence, group_start, group_end)?;
            }
            EngineStagingOrder::FusedCurrentCapsule => {
                stage_fused_current_capsule(&transaction, first_sequence, group_start, group_end)?;
            }
            EngineStagingOrder::FusedSegmentedCapsule => {
                stage_fused_segmented_capsule(
                    &transaction,
                    first_sequence,
                    group_start,
                    group_end,
                )?;
            }
            EngineStagingOrder::FusedStateBearingSegment => {
                stage_fused_state_bearing_segment(
                    &transaction,
                    first_sequence,
                    group_start,
                    group_end,
                )?;
            }
        }
        terminal_work = terminal_work.saturating_add(work_started.elapsed());
        let commit_started = Instant::now();
        transaction
            .commit()
            .map_err(|_| EngineBenchmarkError::Engine)?;
        terminal_commit = terminal_commit.saturating_add(commit_started.elapsed());
    }

    if profile.durability == EngineDurability::None {
        durable_barrier(&database)?;
    }
    let elapsed = started.elapsed();
    let file_bytes = fs::metadata(path)
        .map_err(|_| EngineBenchmarkError::Engine)?
        .len();
    Ok(EngineMechanicsSample {
        commands,
        admission_work,
        admission_commit,
        terminal_work,
        terminal_commit,
        elapsed,
        file_bytes,
    })
}

/// Lightweight engine reopen (allocator probe only). Series-compatible with
/// pre-Package-H measurements: open + read META, no full evidence drain.
pub fn measure_engine_reopen(path: &Path) -> Result<Duration, EngineBenchmarkError> {
    let started = Instant::now();
    let database = Database::create(path).map_err(|_| EngineBenchmarkError::Engine)?;
    let transaction = database
        .begin_read()
        .map_err(|_| EngineBenchmarkError::Engine)?;
    let table = transaction
        .open_table(META)
        .map_err(|_| EngineBenchmarkError::Engine)?;
    if table
        .get(META_APPLICATION_SEQUENCE)
        .map_err(|_| EngineBenchmarkError::Engine)?
        .is_none()
    {
        return Err(EngineBenchmarkError::Engine);
    }
    drop(table);
    drop(transaction);
    drop(database);
    Ok(started.elapsed())
}

/// Full clean startup drain: open + structural + historical + operational handoff.
pub fn measure_clean_startup(path: &Path) -> Result<Duration, EngineBenchmarkError> {
    let started = Instant::now();
    drain_startup_evidence(path)?;
    Ok(started.elapsed())
}

/// Timing for one PERF-013 clean startup with evidence-drain half-split.
///
/// `first_half` / `second_half` sum wall time over the first and second halves of
/// combined structural + historical evidence *page reads* (by page count, each
/// ExactEnd response counted as one page). Opaque cursors do not expose a
/// retained-command sequence midpoint, so page-count half-split is the drain-API
/// linear check. Validation work is unchanged.
///
/// **Interpretation:** only the *movement* of `first_half / second_half` across
/// retained counts N is meaningful — the absolute ratio is not 1:1 (structural
/// work is front-loaded). A midpoint that crosses the structural→historical
/// junction confounds the ratio; use `structural_pages` / `historical_pages` to
/// detect that.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CleanStartupMeasurement {
    elapsed: Duration,
    first_half: Duration,
    second_half: Duration,
    structural_pages: u64,
    historical_pages: u64,
    evidence_pages: u64,
    /// Observed from the session: whether a verified checkpoint drove this drain.
    checkpoint_verified: bool,
    /// Observed from the session: COMMITS rows actually walked after the
    /// checkpoint bound S (0 on the full-validation path).
    suffix_commands: u64,
}

impl CleanStartupMeasurement {
    /// Full open + structural + historical + operational handoff wall time.
    #[must_use]
    pub const fn elapsed(self) -> Duration {
        self.elapsed
    }

    /// Wall time spent in the first half of evidence page reads.
    #[must_use]
    pub const fn first_half(self) -> Duration {
        self.first_half
    }

    /// Wall time spent in the second half of evidence page reads.
    #[must_use]
    pub const fn second_half(self) -> Duration {
        self.second_half
    }

    /// Structural evidence page reads (including ExactEnd).
    #[must_use]
    pub const fn structural_pages(self) -> u64 {
        self.structural_pages
    }

    /// Historical evidence page reads (including ExactEnd).
    #[must_use]
    pub const fn historical_pages(self) -> u64 {
        self.historical_pages
    }

    /// Structural + historical evidence page reads (including ExactEnd responses).
    #[must_use]
    pub const fn evidence_pages(self) -> u64 {
        self.evidence_pages
    }

    /// Whether the drained session verified a validated-prefix checkpoint
    /// (session observability, never a caller-supplied flag).
    #[must_use]
    pub const fn checkpoint_verified(self) -> bool {
        self.checkpoint_verified
    }

    /// COMMITS rows the session actually walked after the checkpoint bound S
    /// (session observability; 0 on the full-validation path).
    #[must_use]
    pub const fn suffix_commands(self) -> u64 {
        self.suffix_commands
    }
}

/// Removes any stored validated-prefix checkpoint via a raw engine transaction.
///
/// Benchmark-only affordance so a plain `--startup-scale` baseline measures FULL
/// validation even on a reused database whose earlier clean open wrote a
/// checkpoint. Lives behind the `benchmark-support` feature and is called only
/// by the benchmark harness — no production open/validation path can reach it.
pub fn strip_validated_prefix_checkpoint(path: &Path) -> Result<(), EngineBenchmarkError> {
    let database = Database::create(path).map_err(|_| EngineBenchmarkError::Engine)?;
    let transaction = database
        .begin_write()
        .map_err(|_| EngineBenchmarkError::Engine)?;
    {
        let mut meta = transaction
            .open_table(META)
            .map_err(|_| EngineBenchmarkError::Engine)?;
        let _ = meta
            .remove(crate::layout::META_VALIDATED_PREFIX_CHECKPOINT)
            .map_err(|_| EngineBenchmarkError::Engine)?;
    }
    transaction
        .commit()
        .map_err(|_| EngineBenchmarkError::Engine)?;
    Ok(())
}

/// Full clean startup with combined structural+historical page half-split timings.
///
/// `expected_retained_commands` pre-sizes the per-page timing buffer so a 10M-command
/// drain does not reallocate (RSS pollution) mid-measurement.
pub fn measure_clean_startup_linear(
    path: &Path,
    expected_retained_commands: u64,
) -> Result<CleanStartupMeasurement, EngineBenchmarkError> {
    let started = Instant::now();
    let detail = drain_startup_evidence_linear(path, expected_retained_commands)?;
    Ok(CleanStartupMeasurement {
        elapsed: started.elapsed(),
        first_half: detail.first_half,
        second_half: detail.second_half,
        structural_pages: detail.structural_pages,
        historical_pages: detail.historical_pages,
        evidence_pages: detail.evidence_pages,
        checkpoint_verified: detail.checkpoint_verified,
        suffix_commands: detail.suffix_commands,
    })
}

/// Splits ordered page durations into first/second half sums (by page count).
///
/// When the page count is odd the extra page is attributed to the second half
/// (`mid = len / 2`). Empty input yields `(0, 0)`.
#[must_use]
pub fn split_half_page_durations(page_ns: &[u64]) -> (u64, u64) {
    let mid = page_ns.len() / 2;
    let first = page_ns[..mid].iter().copied().sum();
    let second = page_ns[mid..].iter().copied().sum();
    (first, second)
}

/// Soft upper bound on structural+historical page reads for a retained count.
///
/// Page limit is 64 entries; structural evidence is denser than one item per
/// command. Over-estimate slightly so the Vec never reallocates during drain.
#[must_use]
pub fn expected_evidence_page_capacity(retained_commands: u64) -> usize {
    // ~4 evidence items/command worst case + both ExactEnd pages + headroom.
    let items = retained_commands.saturating_mul(4).saturating_add(128);
    let pages = items.div_ceil(32).saturating_add(16);
    usize::try_from(pages).unwrap_or(usize::MAX).max(32)
}

struct DrainLinearDetail {
    first_half: Duration,
    second_half: Duration,
    structural_pages: u64,
    historical_pages: u64,
    evidence_pages: u64,
    checkpoint_verified: bool,
    suffix_commands: u64,
}

fn drain_startup_evidence(path: &Path) -> Result<(), EngineBenchmarkError> {
    let _ = drain_startup_evidence_linear(path, 0)?;
    Ok(())
}

fn drain_startup_evidence_linear(
    path: &Path,
    expected_retained_commands: u64,
) -> Result<DrainLinearDetail, EngineBenchmarkError> {
    use riffdb_storage_api::{
        EvidencePageLimit, HistoricalEvidenceCursor, HistoricalEvidencePage,
        ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
        StartupValidationInputs, StructuralEvidenceCursor, StructuralEvidenceOpen,
        StructuralEvidencePage, StructuralEvidenceSession, StructuralOpenOutcome,
    };
    use riffdb_types::{DigestKeyId, Timestamp};

    let store = RedbStore::open(path).map_err(|_| EngineBenchmarkError::Engine)?;
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).ok_or(EngineBenchmarkError::Engine)?);
    let inputs = StartupValidationInputs::new(
        Timestamp::new(1, 0).map_err(|_| EngineBenchmarkError::Engine)?,
        ReadableCapabilityDigestInventory::new(vec![key])
            .map_err(|_| EngineBenchmarkError::Engine)?,
        ReadableIdempotencyDigestInventory::new(vec![key])
            .map_err(|_| EngineBenchmarkError::Engine)?,
    );
    let mut session = store
        .begin_structural_evidence(inputs)
        .map_err(|_| EngineBenchmarkError::Engine)?;
    // Session observability, never a caller-supplied flag (evidence integrity).
    let checkpoint_verified = session.checkpoint_verified();
    let database_id = session.database_id();
    let open_session_id = session.open_session_id();
    let limit = EvidencePageLimit::new(64).ok_or(EngineBenchmarkError::Engine)?;
    let mut page_ns =
        Vec::with_capacity(expected_evidence_page_capacity(expected_retained_commands));
    let mut structural_pages = 0_u64;
    let mut structural = StructuralEvidenceCursor::start(database_id, open_session_id);
    let structural_end = loop {
        let page_started = Instant::now();
        match session
            .read_structural_evidence(structural, limit)
            .map_err(|_| EngineBenchmarkError::Engine)?
        {
            StructuralEvidencePage::Page { next, .. } => {
                page_ns.push(u64::try_from(page_started.elapsed().as_nanos()).unwrap_or(u64::MAX));
                structural_pages = structural_pages.saturating_add(1);
                structural = next;
            }
            StructuralEvidencePage::ExactEnd(end) => {
                page_ns.push(u64::try_from(page_started.elapsed().as_nanos()).unwrap_or(u64::MAX));
                structural_pages = structural_pages.saturating_add(1);
                break end;
            }
        }
    };
    let mut historical_pages = 0_u64;
    let mut historical = HistoricalEvidenceCursor::start(database_id, open_session_id);
    let historical_end = loop {
        let page_started = Instant::now();
        match session
            .read_historical_evidence(historical, limit)
            .map_err(|_| EngineBenchmarkError::Engine)?
        {
            HistoricalEvidencePage::Page { next, .. } => {
                page_ns.push(u64::try_from(page_started.elapsed().as_nanos()).unwrap_or(u64::MAX));
                historical_pages = historical_pages.saturating_add(1);
                historical = next;
            }
            HistoricalEvidencePage::ExactEnd(end) => {
                page_ns.push(u64::try_from(page_started.elapsed().as_nanos()).unwrap_or(u64::MAX));
                historical_pages = historical_pages.saturating_add(1);
                break end;
            }
        }
    };
    let evidence_pages = u64::try_from(page_ns.len()).unwrap_or(u64::MAX);
    let (first_half_ns, second_half_ns) = split_half_page_durations(&page_ns);
    let suffix_commands = session.checkpoint_suffix_commits();
    let outcome = session
        .finish(structural_end, historical_end)
        .map_err(|_| EngineBenchmarkError::Engine)?;
    match outcome {
        StructuralOpenOutcome::Clean(opened) => {
            let (_, _, _, dormant) = opened.into_parts();
            let _ = dormant
                .into_operational_after_catalog_validation()
                .map_err(|_| EngineBenchmarkError::Engine)?;
        }
        StructuralOpenOutcome::MigrationRequired(_) => {
            return Err(EngineBenchmarkError::Engine);
        }
    }
    Ok(DrainLinearDetail {
        first_half: Duration::from_nanos(first_half_ns),
        second_half: Duration::from_nanos(second_half_ns),
        structural_pages,
        historical_pages,
        evidence_pages,
        checkpoint_verified,
        suffix_commands,
    })
}

fn configure(
    transaction: &mut WriteTransaction,
    durability: EngineDurability,
) -> Result<(), EngineBenchmarkError> {
    let (redb_durability, two_phase) = match durability {
        EngineDurability::None => (Durability::None, false),
        EngineDurability::ImmediateOnePhase => (Durability::Immediate, false),
        EngineDurability::ImmediateTwoPhase => (Durability::Immediate, true),
    };
    transaction.set_two_phase_commit(two_phase);
    transaction
        .set_durability(redb_durability)
        .map_err(|_| EngineBenchmarkError::Engine)
}

fn stage_admissions(
    transaction: &WriteTransaction,
    first_sequence: u64,
    start: usize,
    end: usize,
) -> Result<(), EngineBenchmarkError> {
    let mut pending = transaction
        .open_table(IDEMPOTENCY_PENDING)
        .map_err(|_| EngineBenchmarkError::Engine)?;
    for offset in start..end {
        let sequence = sequence_at(first_sequence, offset)?;
        let key = record_key(sequence, 0x59);
        let value = record_value(sequence, PENDING_VALUE_BYTES, 0x50);
        pending
            .insert(key.as_slice(), value.as_slice())
            .map_err(|_| EngineBenchmarkError::Engine)?;
    }
    Ok(())
}

fn stage_terminals(
    transaction: &WriteTransaction,
    first_sequence: u64,
    start: usize,
    end: usize,
) -> Result<(), EngineBenchmarkError> {
    for offset in start..end {
        let sequence = sequence_at(first_sequence, offset)?;
        let key = record_key(sequence, 0x59);
        let mut pending = transaction
            .open_table(IDEMPOTENCY_PENDING)
            .map_err(|_| EngineBenchmarkError::Engine)?;
        if pending
            .remove(key.as_slice())
            .map_err(|_| EngineBenchmarkError::Engine)?
            .is_none()
        {
            return Err(EngineBenchmarkError::Engine);
        }
        drop(pending);
        insert_record(transaction, ENTITIES, sequence, 0x45, ENTITY_VALUE_BYTES)?;
        insert_record(
            transaction,
            SECONDARY_INDEXES,
            sequence,
            0x49,
            INDEX_VALUE_BYTES,
        )?;
        insert_record(transaction, INDEX_EPOCHS, sequence, 0x58, EPOCH_VALUE_BYTES)?;
        insert_record(
            transaction,
            PROVENANCE,
            sequence,
            0x50,
            PROVENANCE_VALUE_BYTES,
        )?;
        insert_record(transaction, EVENTS, sequence, 0x56, EVENT_VALUE_BYTES)?;
        insert_record(
            transaction,
            EVENT_ROUTES,
            sequence,
            0x52,
            EVENT_ROUTE_VALUE_BYTES,
        )?;
        insert_record(transaction, OUTBOX, sequence, 0x4f, OUTBOX_VALUE_BYTES)?;
        insert_record(transaction, COMMITS, sequence, 0x43, COMMIT_VALUE_BYTES)?;
        insert_record(
            transaction,
            IDEMPOTENCY,
            sequence,
            0x59,
            OUTCOME_VALUE_BYTES,
        )?;
        insert_record(transaction, AUDIT, sequence, 0xa1, AUDIT_VALUE_BYTES)?;
        insert_record(transaction, AUDIT, sequence, 0xa2, AUDIT_VALUE_BYTES)?;
        insert_record(
            transaction,
            AUDIT_BY_REQUEST,
            sequence,
            0xb1,
            AUDIT_REQUEST_VALUE_BYTES,
        )?;
        insert_record(
            transaction,
            AUDIT_BY_REQUEST,
            sequence,
            0xb2,
            AUDIT_REQUEST_VALUE_BYTES,
        )?;
        transaction
            .open_table(META)
            .map_err(|_| EngineBenchmarkError::Engine)?
            .insert(
                META_APPLICATION_SEQUENCE,
                sequence
                    .checked_add(1)
                    .ok_or(EngineBenchmarkError::Engine)?
                    .to_be_bytes()
                    .as_slice(),
            )
            .map_err(|_| EngineBenchmarkError::Engine)?;
    }
    Ok(())
}

fn stage_terminals_table_major(
    transaction: &WriteTransaction,
    first_sequence: u64,
    start: usize,
    end: usize,
) -> Result<(), EngineBenchmarkError> {
    {
        let mut pending = transaction
            .open_table(IDEMPOTENCY_PENDING)
            .map_err(|_| EngineBenchmarkError::Engine)?;
        for offset in start..end {
            let sequence = sequence_at(first_sequence, offset)?;
            let key = record_key(sequence, 0x59);
            if pending
                .remove(key.as_slice())
                .map_err(|_| EngineBenchmarkError::Engine)?
                .is_none()
            {
                return Err(EngineBenchmarkError::Engine);
            }
        }
    }
    insert_record_group(
        transaction,
        ENTITIES,
        first_sequence,
        start,
        end,
        0x45,
        ENTITY_VALUE_BYTES,
    )?;
    insert_record_group(
        transaction,
        SECONDARY_INDEXES,
        first_sequence,
        start,
        end,
        0x49,
        INDEX_VALUE_BYTES,
    )?;
    insert_record_group(
        transaction,
        INDEX_EPOCHS,
        first_sequence,
        start,
        end,
        0x58,
        EPOCH_VALUE_BYTES,
    )?;
    insert_record_group(
        transaction,
        PROVENANCE,
        first_sequence,
        start,
        end,
        0x50,
        PROVENANCE_VALUE_BYTES,
    )?;
    insert_record_group(
        transaction,
        EVENTS,
        first_sequence,
        start,
        end,
        0x56,
        EVENT_VALUE_BYTES,
    )?;
    insert_record_group(
        transaction,
        EVENT_ROUTES,
        first_sequence,
        start,
        end,
        0x52,
        EVENT_ROUTE_VALUE_BYTES,
    )?;
    insert_record_group(
        transaction,
        OUTBOX,
        first_sequence,
        start,
        end,
        0x4f,
        OUTBOX_VALUE_BYTES,
    )?;
    insert_record_group(
        transaction,
        COMMITS,
        first_sequence,
        start,
        end,
        0x43,
        COMMIT_VALUE_BYTES,
    )?;
    insert_record_group(
        transaction,
        IDEMPOTENCY,
        first_sequence,
        start,
        end,
        0x59,
        OUTCOME_VALUE_BYTES,
    )?;
    {
        let mut audit = transaction
            .open_table(AUDIT)
            .map_err(|_| EngineBenchmarkError::Engine)?;
        for offset in start..end {
            let sequence = sequence_at(first_sequence, offset)?;
            for tag in [0xa1, 0xa2] {
                let key = record_key(sequence, tag);
                let value = record_value(sequence, AUDIT_VALUE_BYTES, tag);
                audit
                    .insert(key.as_slice(), value.as_slice())
                    .map_err(|_| EngineBenchmarkError::Engine)?;
            }
        }
    }
    {
        let mut audit_by_request = transaction
            .open_table(AUDIT_BY_REQUEST)
            .map_err(|_| EngineBenchmarkError::Engine)?;
        for offset in start..end {
            let sequence = sequence_at(first_sequence, offset)?;
            for tag in [0xb1, 0xb2] {
                let key = record_key(sequence, tag);
                let value = record_value(sequence, AUDIT_REQUEST_VALUE_BYTES, tag);
                audit_by_request
                    .insert(key.as_slice(), value.as_slice())
                    .map_err(|_| EngineBenchmarkError::Engine)?;
            }
        }
    }
    let last_sequence = sequence_at(
        first_sequence,
        end.checked_sub(1)
            .ok_or(EngineBenchmarkError::InvalidConfiguration)?,
    )?;
    transaction
        .open_table(META)
        .map_err(|_| EngineBenchmarkError::Engine)?
        .insert(
            META_APPLICATION_SEQUENCE,
            last_sequence
                .checked_add(1)
                .ok_or(EngineBenchmarkError::Engine)?
                .to_be_bytes()
                .as_slice(),
        )
        .map_err(|_| EngineBenchmarkError::Engine)?;
    Ok(())
}

fn stage_fused_current_capsule(
    transaction: &WriteTransaction,
    first_sequence: u64,
    start: usize,
    end: usize,
) -> Result<(), EngineBenchmarkError> {
    insert_record_group(
        transaction,
        ENTITIES,
        first_sequence,
        start,
        end,
        0x45,
        ENTITY_VALUE_BYTES,
    )?;
    insert_record_group(
        transaction,
        SECONDARY_INDEXES,
        first_sequence,
        start,
        end,
        0x49,
        INDEX_VALUE_BYTES,
    )?;
    insert_record_group(
        transaction,
        INDEX_EPOCHS,
        first_sequence,
        start,
        end,
        0x58,
        EPOCH_VALUE_BYTES,
    )?;
    insert_record_group(
        transaction,
        PROVENANCE,
        first_sequence,
        start,
        end,
        0x50,
        32,
    )?;
    insert_record_group(
        transaction,
        EVENTS,
        first_sequence,
        start,
        end,
        0x56,
        EVENT_VALUE_BYTES,
    )?;
    insert_record_group(
        transaction,
        EVENT_ROUTES,
        first_sequence,
        start,
        end,
        0x52,
        EVENT_ROUTE_VALUE_BYTES,
    )?;
    insert_record_group(transaction, OUTBOX, first_sequence, start, end, 0x4f, 96)?;
    insert_record_group(
        transaction,
        COMMITS,
        first_sequence,
        start,
        end,
        0x43,
        PROVENANCE_VALUE_BYTES + COMMIT_VALUE_BYTES + OUTCOME_VALUE_BYTES + 2 * AUDIT_VALUE_BYTES,
    )?;
    insert_record_group(
        transaction,
        IDEMPOTENCY,
        first_sequence,
        start,
        end,
        0x59,
        32,
    )?;
    insert_dual_locator_group(transaction, AUDIT, first_sequence, start, end, [0xa1, 0xa2])?;
    insert_dual_locator_group(
        transaction,
        AUDIT_BY_REQUEST,
        first_sequence,
        start,
        end,
        [0xb1, 0xb2],
    )?;
    stage_group_allocator(transaction, first_sequence, end)
}

fn seed_state_segment_projection(
    database: &Database,
    projection: StateSegmentProjection,
    retained_entities: usize,
) -> Result<(), EngineBenchmarkError> {
    if retained_entities == 0 {
        return Ok(());
    }
    for start in (0..retained_entities).step_by(MAX_GROUP_COMMANDS) {
        let end = retained_entities.min(start.saturating_add(MAX_GROUP_COMMANDS));
        let mut transaction = database
            .begin_write()
            .map_err(|_| EngineBenchmarkError::Engine)?;
        configure(&mut transaction, EngineDurability::None)?;
        match projection {
            StateSegmentProjection::CurrentSegmentedAuthority => {
                insert_projection_state_rows(
                    &transaction,
                    1,
                    start,
                    end,
                    StateSegmentWorkload::DistinctCreates,
                    0,
                )?;
                let segment_sequence = sequence_at(1, start)?;
                let commands = end
                    .checked_sub(start)
                    .ok_or(EngineBenchmarkError::InvalidConfiguration)?;
                insert_record(
                    &transaction,
                    INDEX_EPOCHS,
                    segment_sequence,
                    0x58,
                    EPOCH_VALUE_BYTES,
                )?;
                insert_record(
                    &transaction,
                    COMMITS,
                    segment_sequence,
                    0x43,
                    segmented_capsule_bytes(commands)?,
                )?;
            }
            StateSegmentProjection::BenchmarkStateBearingSegment => {
                insert_state_bearing_segment(&transaction, 1, start, end)?;
            }
        }
        stage_group_allocator(&transaction, 1, end)?;
        transaction
            .commit()
            .map_err(|_| EngineBenchmarkError::Engine)?;
    }
    durable_barrier(database)
}

fn stage_current_state_projection(
    transaction: &WriteTransaction,
    first_sequence: u64,
    start: usize,
    end: usize,
    workload: StateSegmentWorkload,
    retained_entities: usize,
) -> Result<(), EngineBenchmarkError> {
    insert_projection_state_rows(
        transaction,
        first_sequence,
        start,
        end,
        workload,
        retained_entities,
    )?;
    let commands = end
        .checked_sub(start)
        .ok_or(EngineBenchmarkError::InvalidConfiguration)?;
    let segment_sequence = sequence_at(first_sequence, start)?;
    insert_record(
        transaction,
        INDEX_EPOCHS,
        segment_sequence,
        0x58,
        EPOCH_VALUE_BYTES,
    )?;
    insert_record(
        transaction,
        COMMITS,
        segment_sequence,
        0x43,
        segmented_capsule_bytes(commands)?,
    )?;
    stage_group_allocator(transaction, first_sequence, end)
}

fn stage_state_bearing_projection(
    transaction: &WriteTransaction,
    first_sequence: u64,
    start: usize,
    end: usize,
) -> Result<(), EngineBenchmarkError> {
    insert_state_bearing_segment(transaction, first_sequence, start, end)?;
    stage_group_allocator(transaction, first_sequence, end)
}

fn insert_projection_state_rows(
    transaction: &WriteTransaction,
    first_sequence: u64,
    start: usize,
    end: usize,
    workload: StateSegmentWorkload,
    retained_entities: usize,
) -> Result<(), EngineBenchmarkError> {
    let mut entities = transaction
        .open_table(ENTITIES)
        .map_err(|_| EngineBenchmarkError::Engine)?;
    let mut indexes = transaction
        .open_table(SECONDARY_INDEXES)
        .map_err(|_| EngineBenchmarkError::Engine)?;
    for offset in start..end {
        let command_sequence = sequence_at(first_sequence, offset)?;
        let state_sequence =
            projection_state_sequence(command_sequence, offset, workload, retained_entities)?;
        let entity_key = record_key(state_sequence, 0x45);
        let entity_value = record_value(command_sequence, ENTITY_VALUE_BYTES, 0x45);
        entities
            .insert(entity_key.as_slice(), entity_value.as_slice())
            .map_err(|_| EngineBenchmarkError::Engine)?;
        let index_key = record_key(state_sequence, 0x49);
        let index_value = record_value(command_sequence, INDEX_VALUE_BYTES, 0x49);
        indexes
            .insert(index_key.as_slice(), index_value.as_slice())
            .map_err(|_| EngineBenchmarkError::Engine)?;
    }
    Ok(())
}

fn projection_state_sequence(
    command_sequence: u64,
    offset: usize,
    workload: StateSegmentWorkload,
    retained_entities: usize,
) -> Result<u64, EngineBenchmarkError> {
    match workload {
        StateSegmentWorkload::DistinctCreates => Ok(command_sequence),
        StateSegmentWorkload::RetainedUpdates => retained_state_sequence(offset, retained_entities),
        StateSegmentWorkload::MixedCreatesAndUpdates if offset % 4 == 3 => {
            retained_state_sequence(offset / 4, retained_entities)
        }
        StateSegmentWorkload::MixedCreatesAndUpdates => Ok(command_sequence),
    }
}

fn retained_state_sequence(
    offset: usize,
    retained_entities: usize,
) -> Result<u64, EngineBenchmarkError> {
    if retained_entities == 0 {
        return Err(EngineBenchmarkError::InvalidConfiguration);
    }
    let member = offset % retained_entities;
    u64::try_from(member)
        .map_err(|_| EngineBenchmarkError::Engine)?
        .checked_add(1)
        .ok_or(EngineBenchmarkError::InvalidConfiguration)
}

fn segmented_capsule_bytes(commands: usize) -> Result<usize, EngineBenchmarkError> {
    segmented_capsule_command_bytes()?
        .checked_mul(commands)
        .ok_or(EngineBenchmarkError::InvalidConfiguration)
}

fn segmented_capsule_command_bytes() -> Result<usize, EngineBenchmarkError> {
    PROVENANCE_VALUE_BYTES
        .checked_add(COMMIT_VALUE_BYTES)
        .and_then(|value| value.checked_add(OUTCOME_VALUE_BYTES))
        .and_then(|value| value.checked_add(2 * AUDIT_VALUE_BYTES))
        .and_then(|value| value.checked_add(EVENT_VALUE_BYTES))
        .and_then(|value| value.checked_add(EPOCH_VALUE_BYTES))
        .ok_or(EngineBenchmarkError::InvalidConfiguration)
}

fn insert_state_bearing_segment(
    transaction: &WriteTransaction,
    first_sequence: u64,
    start: usize,
    end: usize,
) -> Result<(), EngineBenchmarkError> {
    let commands = end
        .checked_sub(start)
        .ok_or(EngineBenchmarkError::InvalidConfiguration)?;
    let segment_sequence = sequence_at(first_sequence, start)?;
    let command_bytes = segmented_capsule_command_bytes()?
        .checked_add(ENTITY_VALUE_BYTES)
        .and_then(|value| value.checked_add(INDEX_VALUE_BYTES))
        .ok_or(EngineBenchmarkError::InvalidConfiguration)?;
    insert_record(
        transaction,
        COMMITS,
        segment_sequence,
        0x53,
        command_bytes
            .checked_mul(commands)
            .ok_or(EngineBenchmarkError::InvalidConfiguration)?,
    )
}

fn stage_fused_segmented_capsule(
    transaction: &WriteTransaction,
    first_sequence: u64,
    start: usize,
    end: usize,
) -> Result<(), EngineBenchmarkError> {
    insert_record_group(
        transaction,
        ENTITIES,
        first_sequence,
        start,
        end,
        0x45,
        ENTITY_VALUE_BYTES,
    )?;
    insert_record_group(
        transaction,
        SECONDARY_INDEXES,
        first_sequence,
        start,
        end,
        0x49,
        INDEX_VALUE_BYTES,
    )?;
    // Outbox delivery state is mutable independently of the immutable event
    // segment, so it remains one compact row per command in this model.
    insert_record_group(transaction, OUTBOX, first_sequence, start, end, 0x4f, 96)?;

    let commands = end
        .checked_sub(start)
        .ok_or(EngineBenchmarkError::InvalidConfiguration)?;
    let segment_sequence = sequence_at(first_sequence, start)?;
    insert_record(
        transaction,
        INDEX_EPOCHS,
        segment_sequence,
        0x58,
        EPOCH_VALUE_BYTES,
    )?;
    insert_record(
        transaction,
        EVENTS,
        segment_sequence,
        0x56,
        EVENT_VALUE_BYTES
            .checked_mul(commands)
            .ok_or(EngineBenchmarkError::InvalidConfiguration)?,
    )?;
    let capsule_bytes = PROVENANCE_VALUE_BYTES
        .checked_add(COMMIT_VALUE_BYTES)
        .and_then(|value| value.checked_add(OUTCOME_VALUE_BYTES))
        .and_then(|value| value.checked_add(2 * AUDIT_VALUE_BYTES))
        .and_then(|value| value.checked_mul(commands))
        .ok_or(EngineBenchmarkError::InvalidConfiguration)?;
    insert_record(transaction, COMMITS, segment_sequence, 0x43, capsule_bytes)?;
    stage_group_allocator(transaction, first_sequence, end)
}

fn stage_fused_state_bearing_segment(
    transaction: &WriteTransaction,
    first_sequence: u64,
    start: usize,
    end: usize,
) -> Result<(), EngineBenchmarkError> {
    // Every immutable command fact and complete current-state transition is
    // projected into one group segment. Independently mutable delivery rows
    // remain possible, but the audited production command path creates none.
    // This function is reachable only through benchmark support.
    insert_state_bearing_segment(transaction, first_sequence, start, end)?;
    stage_group_allocator(transaction, first_sequence, end)
}

fn insert_dual_locator_group(
    transaction: &WriteTransaction,
    definition: redb::TableDefinition<&[u8], &[u8]>,
    first_sequence: u64,
    start: usize,
    end: usize,
    tags: [u8; 2],
) -> Result<(), EngineBenchmarkError> {
    let mut table = transaction
        .open_table(definition)
        .map_err(|_| EngineBenchmarkError::Engine)?;
    for offset in start..end {
        let sequence = sequence_at(first_sequence, offset)?;
        for tag in tags {
            let key = record_key(sequence, tag);
            let value = record_value(sequence, 32, tag);
            table
                .insert(key.as_slice(), value.as_slice())
                .map_err(|_| EngineBenchmarkError::Engine)?;
        }
    }
    Ok(())
}

fn stage_group_allocator(
    transaction: &WriteTransaction,
    first_sequence: u64,
    end: usize,
) -> Result<(), EngineBenchmarkError> {
    let last_sequence = sequence_at(
        first_sequence,
        end.checked_sub(1)
            .ok_or(EngineBenchmarkError::InvalidConfiguration)?,
    )?;
    transaction
        .open_table(META)
        .map_err(|_| EngineBenchmarkError::Engine)?
        .insert(
            META_APPLICATION_SEQUENCE,
            last_sequence
                .checked_add(1)
                .ok_or(EngineBenchmarkError::Engine)?
                .to_be_bytes()
                .as_slice(),
        )
        .map_err(|_| EngineBenchmarkError::Engine)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn insert_record_group(
    transaction: &WriteTransaction,
    definition: redb::TableDefinition<&[u8], &[u8]>,
    first_sequence: u64,
    start: usize,
    end: usize,
    tag: u8,
    bytes: usize,
) -> Result<(), EngineBenchmarkError> {
    let mut table = transaction
        .open_table(definition)
        .map_err(|_| EngineBenchmarkError::Engine)?;
    for offset in start..end {
        let sequence = sequence_at(first_sequence, offset)?;
        let key = record_key(sequence, tag);
        let value = record_value(sequence, bytes, tag);
        table
            .insert(key.as_slice(), value.as_slice())
            .map_err(|_| EngineBenchmarkError::Engine)?;
    }
    Ok(())
}

fn insert_record(
    transaction: &WriteTransaction,
    definition: redb::TableDefinition<&[u8], &[u8]>,
    sequence: u64,
    tag: u8,
    bytes: usize,
) -> Result<(), EngineBenchmarkError> {
    let key = record_key(sequence, tag);
    let value = record_value(sequence, bytes, tag);
    transaction
        .open_table(definition)
        .map_err(|_| EngineBenchmarkError::Engine)?
        .insert(key.as_slice(), value.as_slice())
        .map_err(|_| EngineBenchmarkError::Engine)?;
    Ok(())
}

fn sequence_at(first: u64, offset: usize) -> Result<u64, EngineBenchmarkError> {
    first
        .checked_add(u64::try_from(offset).map_err(|_| EngineBenchmarkError::Engine)?)
        .ok_or(EngineBenchmarkError::Engine)
}

fn record_key(sequence: u64, tag: u8) -> [u8; 16] {
    let mut key = [0_u8; 16];
    key[0] = tag;
    key[8..].copy_from_slice(&sequence.to_be_bytes());
    key
}

fn record_value(sequence: u64, bytes: usize, tag: u8) -> Vec<u8> {
    let mut value = vec![tag; bytes];
    value[..8].copy_from_slice(&sequence.to_be_bytes());
    value
}

fn durable_barrier(database: &Database) -> Result<(), EngineBenchmarkError> {
    let mut transaction = database
        .begin_write()
        .map_err(|_| EngineBenchmarkError::Engine)?;
    configure(&mut transaction, EngineDurability::ImmediateTwoPhase)?;
    transaction
        .commit()
        .map_err(|_| EngineBenchmarkError::Engine)
}

fn service_audit_intent(
    request_id: RequestId,
    command: u64,
    phase: ServiceAuditPhaseV1,
) -> Result<ServiceAuditAppendIntentV1, EngineBenchmarkError> {
    let seconds = i64::try_from(command).map_err(|_| EngineBenchmarkError::Engine)?;
    let principal = AuditPrincipalV1::new(
        ActorId::new("command-growth-agent").map_err(|_| EngineBenchmarkError::Engine)?,
        ActorKind::Service,
        CapabilityId::from_bytes(uuid_v7_bytes(0x22, 0))
            .map_err(|_| EngineBenchmarkError::Engine)?,
        NonZeroU64::MIN,
    );
    ServiceAuditAppendIntentV1::new(
        request_id,
        Timestamp::new(seconds, 0).map_err(|_| EngineBenchmarkError::Engine)?,
        ServiceOperationV1::ExecuteCommand,
        phase,
        principal,
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::None,
    )
    .map_err(|_| EngineBenchmarkError::Engine)
}

fn uuid_v7_bytes(tag: u8, sequence: u64) -> [u8; 16] {
    let mut bytes = [tag; 16];
    bytes[0..6].copy_from_slice(&sequence.to_be_bytes()[2..]);
    bytes[6] = 0x70 | (tag & 0x0f);
    bytes[8] = 0x80 | (tag & 0x3f);
    bytes[9..].copy_from_slice(&sequence.to_be_bytes()[1..]);
    bytes
}

#[cfg(test)]
mod tests {
    use super::{
        EngineDurability, EngineMechanicsProfile, EngineStagingOrder, JournalMutationCensusV1,
        ServiceAuditGrowthHarness, StateSegmentProjection, StateSegmentWorkload,
        authoritative_table_inventory_v1, expected_evidence_page_capacity,
        initialize_engine_mechanics, measure_clean_startup, measure_clean_startup_linear,
        run_engine_mechanics_window, run_journal_overlay_mechanics_window,
        run_state_segment_projection_window, split_half_page_durations,
    };
    use crate::journal::JournalTable;
    use std::time::Duration;

    #[test]
    fn split_half_page_durations_empty_and_balanced() {
        assert_eq!(split_half_page_durations(&[]), (0, 0));
        assert_eq!(split_half_page_durations(&[10]), (0, 10));
        assert_eq!(split_half_page_durations(&[10, 20]), (10, 20));
        assert_eq!(split_half_page_durations(&[1, 2, 3, 4]), (3, 7));
        assert_eq!(split_half_page_durations(&[1, 2, 3]), (1, 5));
    }

    #[test]
    fn expected_page_capacity_grows_with_retained() {
        assert!(
            expected_evidence_page_capacity(10_000_000) > expected_evidence_page_capacity(1_000)
        );
        assert!(expected_evidence_page_capacity(0) >= 32);
    }

    #[test]
    fn clean_startup_linear_matches_plain_measurement_shape() {
        let dir = tempfile_dir();
        let path = dir.join("linear-check.redb");
        let mut harness = ServiceAuditGrowthHarness::new(&path).expect("new harness");
        harness.run_window(1, 32).expect("seed window");
        drop(harness);

        let plain = measure_clean_startup(&path).expect("plain startup");
        let linear = measure_clean_startup_linear(&path, 32).expect("linear startup");
        assert!(linear.elapsed() > Duration::ZERO);
        assert!(plain > Duration::ZERO);
        // Page-read halves are subsets of the full open+drain+handoff wall time.
        let halves = linear.first_half().saturating_add(linear.second_half());
        assert!(halves <= linear.elapsed());
        assert!(linear.evidence_pages() >= 1);
        assert_eq!(
            linear.evidence_pages(),
            linear
                .structural_pages()
                .saturating_add(linear.historical_pages())
        );
    }

    #[test]
    fn grouped_fused_and_ungrouped_generation_yield_identical_drain_shape() {
        // Equivalence: same Started/Failed history order → same evidence page counts.
        let dir = tempfile_dir();
        let ungrouped_path = dir.join("ungrouped.redb");
        let grouped_path = dir.join("grouped.redb");
        const N: usize = 128;

        let mut ungrouped = ServiceAuditGrowthHarness::new(&ungrouped_path).expect("ungrouped new");
        ungrouped.run_window(1, N).expect("ungrouped generate");
        drop(ungrouped);

        let mut grouped = ServiceAuditGrowthHarness::new(&grouped_path).expect("grouped new");
        grouped
            .run_window_grouped_fused(1, N)
            .expect("grouped generate");
        drop(grouped);

        let u = measure_clean_startup_linear(&ungrouped_path, N as u64).expect("ungrouped drain");
        let g = measure_clean_startup_linear(&grouped_path, N as u64).expect("grouped drain");
        assert_eq!(
            u.structural_pages(),
            g.structural_pages(),
            "structural pages"
        );
        assert_eq!(
            u.historical_pages(),
            g.historical_pages(),
            "historical pages"
        );
        assert_eq!(u.evidence_pages(), g.evidence_pages(), "evidence pages");
        assert!(u.structural_pages() >= 1);
        assert!(u.historical_pages() >= 1);
    }

    #[test]
    fn engine_mechanics_profile_rejects_zero_group() {
        assert!(EngineMechanicsProfile::new(EngineDurability::None, 0).is_err());
    }

    #[test]
    fn state_bearing_projection_is_explicitly_benchmark_only() {
        assert_eq!(
            EngineStagingOrder::FusedStateBearingSegment.label(),
            "fused_state_bearing_segment"
        );
    }

    #[test]
    fn state_bearing_projection_folds_state_rows_without_inventing_delivery_rows() {
        let dir = tempfile_dir();
        let current = run_state_segment_projection_window(
            &dir.join("current-projection.redb"),
            StateSegmentProjection::CurrentSegmentedAuthority,
            StateSegmentWorkload::DistinctCreates,
            8,
            8,
            0,
        )
        .expect("current projection");
        let proposed = run_state_segment_projection_window(
            &dir.join("state-bearing-projection.redb"),
            StateSegmentProjection::BenchmarkStateBearingSegment,
            StateSegmentWorkload::DistinctCreates,
            8,
            8,
            0,
        )
        .expect("proposed projection");
        let rows = |sample: &super::StateSegmentProjectionSample, name: &str| {
            sample
                .inventory()
                .iter()
                .find(|table| table.name() == name)
                .expect("closed table")
                .rows()
        };
        assert_eq!(rows(&current, "entities"), 8);
        assert_eq!(rows(&current, "secondary_indexes"), 8);
        assert_eq!(rows(&current, "events"), 0);
        assert_eq!(rows(&proposed, "entities"), 0);
        assert_eq!(rows(&proposed, "secondary_indexes"), 0);
        assert_eq!(rows(&proposed, "events"), 0);
        assert_eq!(rows(&current, "commits"), 1);
        assert_eq!(rows(&proposed, "commits"), 1);
        assert_eq!(rows(&current, "outbox"), 0);
        assert_eq!(rows(&proposed, "outbox"), 0);
    }

    #[test]
    fn journal_overlay_probe_preserves_exact_rows_and_bounded_reads() {
        let dir = tempfile_dir();
        let sample = run_journal_overlay_mechanics_window(
            &dir.join("journal-overlay.redb"),
            StateSegmentWorkload::MixedCreatesAndUpdates,
            128,
            32,
            4_096,
        )
        .expect("journal overlay mechanics");
        assert_eq!(sample.commands(), 128);
        assert_eq!(sample.group_commands(), 32);
        assert!(sample.encoded_journal_bytes() > 0);
        assert_eq!(sample.mutation_census().len(), JournalTable::ALL.len());
        assert_eq!(sample.mutation_census()[0].table(), "meta");
        assert_eq!(
            sample.mutation_census().last().map(|entry| entry.table()),
            Some("entity_chain_heads")
        );
        let observed_mutations = sample
            .mutation_census()
            .iter()
            .map(JournalMutationCensusV1::mutations)
            .sum::<u64>();
        assert!(observed_mutations > 0);
        assert_eq!(
            sample
                .mutation_census()
                .iter()
                .map(JournalMutationCensusV1::v1_fixed_header_bytes)
                .sum::<u64>(),
            observed_mutations * 44
        );
        assert_eq!(sample.overlay_transition_count(), 128);
        assert_eq!(sample.checkpointed_transition_count(), 128);
        assert_eq!(sample.overlay_rows_after_checkpoint(), 0);
        assert_eq!(
            sample.point_read_checksum(),
            sample.control_point_read_checksum()
        );
        assert_eq!(
            sample.page_read_checksum(),
            sample.control_page_read_checksum()
        );
    }

    #[test]
    fn initialize_and_window_still_compile_paths() {
        let dir = tempfile_dir();
        let path = dir.join("mechanics.redb");
        initialize_engine_mechanics(&path).expect("init");
        let profile =
            EngineMechanicsProfile::new(EngineDurability::ImmediateOnePhase, 1).expect("profile");
        let sample = run_engine_mechanics_window(&path, 1, 4, profile).expect("window");
        assert_eq!(sample.commands(), 4);
    }

    #[test]
    fn command_table_inventory_attributes_rows_and_redb_pages_by_closed_table() {
        let dir = tempfile_dir();
        let path = dir.join("table-inventory.redb");
        initialize_engine_mechanics(&path).expect("init");
        let before = authoritative_table_inventory_v1(&path).expect("before inventory");
        assert_eq!(before.len(), 13);
        assert_eq!(before[0].name(), "meta");
        assert!(before.iter().skip(1).all(|table| table.rows() == 0));

        let profile =
            EngineMechanicsProfile::new(EngineDurability::ImmediateOnePhase, 4).expect("profile");
        run_engine_mechanics_window(&path, 1, 4, profile).expect("window");
        let after = authoritative_table_inventory_v1(&path).expect("after inventory");
        let rows = |name: &str| {
            after
                .iter()
                .find(|table| table.name() == name)
                .expect("closed table")
                .rows()
        };
        for name in [
            "entities",
            "secondary_indexes",
            "index_epochs",
            "idempotency",
            "commits",
            "provenance",
            "events",
            "outbox",
        ] {
            assert_eq!(rows(name), 4, "{name}");
        }
        assert_eq!(rows("idempotency_pending"), 0);
        assert_eq!(rows("event_routes"), 4);
        assert_eq!(rows("audit"), 8);
        assert_eq!(rows("audit_by_request"), 8);
        assert!(after.iter().any(|table| table.leaf_pages() > 0));
    }

    struct TestDirectory(std::path::PathBuf);

    impl std::ops::Deref for TestDirectory {
        type Target = std::path::Path;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn tempfile_dir() -> TestDirectory {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = crate::test_path::root().join(format!("riffdb-bench-support-{stamp}"));
        std::fs::create_dir_all(&path).expect("mkdir");
        TestDirectory(path)
    }
}
