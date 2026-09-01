//! One-transaction owned composite-query snapshots for redb.

use std::collections::BTreeMap;
#[cfg(test)]
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use riffdb_policy::{
    AuthorizedIndexedRelationshipLookupV1, AuthorizedProjectedRowAdmissionV1,
    AuthorizedQueryRowPolicyContextV1, MAX_PROJECTED_POLICY_CANDIDATES_V1,
    ProjectedPolicyCandidateObservationV1,
};
use riffdb_query_executor::{
    BoundPredicate, CoveredResultBatch, LongPatternCandidateBatch, MAX_QUERY_SCANNED_ROWS,
    QueryBackendFault, QueryContinuation, QueryExecutionError, QueryExecutionPort,
    QueryExecutionRequest, QueryNearestPage, QueryOwnedSnapshot, QueryParameters, QueryReadView,
    QueryRow, QueryScanPage, VectorInspectionCandidateV1, VectorInspectionSnapshotV1,
    VectorInspectionTargetV1, covered_row_matches_predicates_v1, execute_in_snapshot,
    execute_page_in_snapshot, execute_policy_operational_page_in_snapshot,
    execute_policy_page_in_snapshot, execute_policy_provider_page_in_snapshot,
    execute_provider_page_in_snapshot, validate_query_execution_group,
};
use riffdb_query_ir::{
    AccessDirection, CoveredResultSourceV1, QueryAccessKind, QueryAccessProgramV1, QueryAccessStep,
    QueryPredicateOperator,
};
use riffdb_storage_api::{
    EntityTarget, PartitionIndexTarget, StorageError, StorageErrorKind, VectorObservationTargetV1,
};
use riffdb_types::{
    ApplicationRoleHash, CanonicalValue, EntityKey, EntityTypeId, FieldId, IndexEntryKey,
};

use crate::codec::{
    decode_entity_record_v1, decode_entity_record_v1_profiled, decode_index_entry_v2,
    decode_index_epoch_v1, decode_vector_evidence_index_v1, decode_vector_observation_v1,
};
use crate::error::storage_error;
use crate::journal::JournalTable;
use crate::keys::{
    decode_index_entry_key, decode_vector_evidence_index_key, encode_entity_key,
    encode_partition_index_key, encode_vector_evidence_index_key,
    encode_vector_evidence_index_prefix, encode_vector_observation_key,
};
use crate::store::{RedbOperationalPorts, RedbReadAccess};

const PUBLICATION_OUTER_LOCK: usize = 0;
const PUBLICATION_VIEW_CAPTURE: usize = 1;
const FRONTIER_CAPTURE: usize = 2;
const POINT_LOOKUP: usize = 3;
const ENVELOPE_IDENTITY_BOUNDS: usize = 4;
const PAYLOAD_CHECKSUM: usize = 5;
const WIRE_PREFLIGHT: usize = 6;
const PROST_DECODE: usize = 7;
const CANONICAL_REENCODE: usize = 8;
const SEMANTIC_RECONSTRUCT: usize = 9;
const TARGET_VALIDATE: usize = 10;
const ROW_POLICY: usize = 11;
const ROW_MATERIALIZE: usize = 12;
const INDEX_EPOCH_READ: usize = 13;
const INDEX_RANGE_READ: usize = 14;
const INDEX_ENTRY_DECODE: usize = 15;
const SCAN_SETUP: usize = 16;
/// Exclusive end of the disjoint nested-stage run measured inside view calls.
///
/// `point_open_table`, `point_btree_get` and `point_value_copy` follow it and
/// decompose `point_lookup`, so they are already inside the subtracted total.
const NESTED_STAGE_END: usize = 17;
const POINT_OPEN_TABLE: usize = 17;
const POINT_BTREE_GET: usize = 18;
const POINT_VALUE_COPY: usize = 19;
const VIEW_CALL_RESIDUAL: usize = 20;
const PROGRAM_DRIVE_EXCLUSIVE: usize = 21;

#[derive(Clone, Copy, Default)]
struct QueryExecuteProfile {
    stage_ns: [u64; crate::QUERY_EXECUTE_STAGE_LABELS_V1.len()],
    overlay_transitions: u64,
    overlay_bytes: u64,
    authority_tail_bytes: u64,
    authority_tail_commands: u64,
    entity_point_reads: u64,
    index_rows: u64,
    index_range_reads: u64,
    program_steps: u64,
    /// Wall time spent inside every storage read-view callback of one execute.
    view_total_ns: u64,
}

struct QueryExecuteWindowCounters {
    count: AtomicU64,
    stage_ns: [AtomicU64; crate::QUERY_EXECUTE_STAGE_LABELS_V1.len()],
    overlay_transitions_sum: AtomicU64,
    overlay_transitions_max: AtomicU64,
    overlay_bytes_sum: AtomicU64,
    overlay_bytes_max: AtomicU64,
    authority_tail_bytes_sum: AtomicU64,
    authority_tail_bytes_max: AtomicU64,
    authority_tail_commands_sum: AtomicU64,
    authority_tail_commands_max: AtomicU64,
    entity_point_reads_sum: AtomicU64,
    entity_point_reads_max: AtomicU64,
    index_rows_sum: AtomicU64,
    index_rows_max: AtomicU64,
    index_range_reads_sum: AtomicU64,
    index_range_reads_max: AtomicU64,
    program_steps_sum: AtomicU64,
    program_steps_max: AtomicU64,
}

impl QueryExecuteWindowCounters {
    fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            stage_ns: std::array::from_fn(|_| AtomicU64::new(0)),
            overlay_transitions_sum: AtomicU64::new(0),
            overlay_transitions_max: AtomicU64::new(0),
            overlay_bytes_sum: AtomicU64::new(0),
            overlay_bytes_max: AtomicU64::new(0),
            authority_tail_bytes_sum: AtomicU64::new(0),
            authority_tail_bytes_max: AtomicU64::new(0),
            authority_tail_commands_sum: AtomicU64::new(0),
            authority_tail_commands_max: AtomicU64::new(0),
            entity_point_reads_sum: AtomicU64::new(0),
            entity_point_reads_max: AtomicU64::new(0),
            index_rows_sum: AtomicU64::new(0),
            index_rows_max: AtomicU64::new(0),
            index_range_reads_sum: AtomicU64::new(0),
            index_range_reads_max: AtomicU64::new(0),
            program_steps_sum: AtomicU64::new(0),
            program_steps_max: AtomicU64::new(0),
        }
    }
}

struct QueryExecuteCensus {
    total_count: AtomicU64,
    windows: [QueryExecuteWindowCounters; crate::QUERY_EXECUTE_WINDOW_COUNT_V1],
}

impl QueryExecuteCensus {
    fn new() -> Self {
        Self {
            total_count: AtomicU64::new(0),
            windows: std::array::from_fn(|_| QueryExecuteWindowCounters::new()),
        }
    }
}

static QUERY_EXECUTE_DIAGNOSTICS: OnceLock<bool> = OnceLock::new();
static QUERY_EXECUTE_CENSUS: OnceLock<QueryExecuteCensus> = OnceLock::new();

pub(crate) fn query_execute_diagnostics_enabled() -> bool {
    *QUERY_EXECUTE_DIAGNOSTICS.get_or_init(|| {
        std::env::var_os("RIFFDB_QUERY_EXECUTE_DIAGNOSTICS").is_some_and(|value| value == "1")
    })
}

fn record_query_execute_profile(profile: QueryExecuteProfile) {
    let census = QUERY_EXECUTE_CENSUS.get_or_init(QueryExecuteCensus::new);
    let ordinal = census.total_count.fetch_add(1, Ordering::Relaxed);
    let unbounded_window = usize::try_from(ordinal)
        .unwrap_or(usize::MAX)
        .saturating_div(crate::QUERY_EXECUTE_WINDOW_WIDTH_V1);
    let window_index = unbounded_window.min(crate::QUERY_EXECUTE_WINDOW_COUNT_V1 - 1);
    let window = &census.windows[window_index];
    window.count.fetch_add(1, Ordering::Relaxed);
    for (counter, elapsed) in window.stage_ns.iter().zip(profile.stage_ns) {
        saturating_atomic_add(counter, elapsed);
    }
    saturating_atomic_add(&window.overlay_transitions_sum, profile.overlay_transitions);
    atomic_max(&window.overlay_transitions_max, profile.overlay_transitions);
    saturating_atomic_add(&window.overlay_bytes_sum, profile.overlay_bytes);
    atomic_max(&window.overlay_bytes_max, profile.overlay_bytes);
    saturating_atomic_add(
        &window.authority_tail_bytes_sum,
        profile.authority_tail_bytes,
    );
    atomic_max(
        &window.authority_tail_bytes_max,
        profile.authority_tail_bytes,
    );
    saturating_atomic_add(
        &window.authority_tail_commands_sum,
        profile.authority_tail_commands,
    );
    atomic_max(
        &window.authority_tail_commands_max,
        profile.authority_tail_commands,
    );
    saturating_atomic_add(&window.entity_point_reads_sum, profile.entity_point_reads);
    atomic_max(&window.entity_point_reads_max, profile.entity_point_reads);
    saturating_atomic_add(&window.index_rows_sum, profile.index_rows);
    atomic_max(&window.index_rows_max, profile.index_rows);
    saturating_atomic_add(&window.index_range_reads_sum, profile.index_range_reads);
    atomic_max(&window.index_range_reads_max, profile.index_range_reads);
    saturating_atomic_add(&window.program_steps_sum, profile.program_steps);
    atomic_max(&window.program_steps_max, profile.program_steps);
}

fn saturating_atomic_add(target: &AtomicU64, value: u64) {
    let _ = target.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

fn atomic_max(target: &AtomicU64, value: u64) {
    let _ = target.fetch_max(value, Ordering::Relaxed);
}

pub(crate) fn query_execute_census_v1() -> crate::QueryExecuteCensusV1 {
    let Some(census) = QUERY_EXECUTE_CENSUS.get() else {
        return crate::QueryExecuteCensusV1 {
            total_count: 0,
            windows: [crate::QueryExecuteWindowV1::default(); crate::QUERY_EXECUTE_WINDOW_COUNT_V1],
        };
    };
    crate::QueryExecuteCensusV1 {
        total_count: census.total_count.load(Ordering::Relaxed),
        windows: std::array::from_fn(|index| {
            let window = &census.windows[index];
            crate::QueryExecuteWindowV1 {
                count: window.count.load(Ordering::Relaxed),
                stage_ns: std::array::from_fn(|stage| {
                    window.stage_ns[stage].load(Ordering::Relaxed)
                }),
                overlay_transitions_sum: window.overlay_transitions_sum.load(Ordering::Relaxed),
                overlay_transitions_max: window.overlay_transitions_max.load(Ordering::Relaxed),
                overlay_bytes_sum: window.overlay_bytes_sum.load(Ordering::Relaxed),
                overlay_bytes_max: window.overlay_bytes_max.load(Ordering::Relaxed),
                authority_tail_bytes_sum: window.authority_tail_bytes_sum.load(Ordering::Relaxed),
                authority_tail_bytes_max: window.authority_tail_bytes_max.load(Ordering::Relaxed),
                authority_tail_commands_sum: window
                    .authority_tail_commands_sum
                    .load(Ordering::Relaxed),
                authority_tail_commands_max: window
                    .authority_tail_commands_max
                    .load(Ordering::Relaxed),
                entity_point_reads_sum: window.entity_point_reads_sum.load(Ordering::Relaxed),
                entity_point_reads_max: window.entity_point_reads_max.load(Ordering::Relaxed),
                index_rows_sum: window.index_rows_sum.load(Ordering::Relaxed),
                index_rows_max: window.index_rows_max.load(Ordering::Relaxed),
                index_range_reads_sum: window.index_range_reads_sum.load(Ordering::Relaxed),
                index_range_reads_max: window.index_range_reads_max.load(Ordering::Relaxed),
                program_steps_sum: window.program_steps_sum.load(Ordering::Relaxed),
                program_steps_max: window.program_steps_max.load(Ordering::Relaxed),
            }
        }),
    }
}

fn elapsed_nanos(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// Per-table open counts for falsifying lazy opens (test-only).
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct QueryTableOpenCounts {
    commits: u64,
    entities: u64,
    indexes: u64,
    epochs: u64,
}

/// Serializes counting assertions against concurrent query-running tests.
#[cfg(test)]
static QUERY_TABLE_OPEN_TEST_LOCK: Mutex<()> = Mutex::new(());

#[cfg(test)]
static QUERY_TABLE_OPENS_COMMITS: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static QUERY_TABLE_OPENS_ENTITIES: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static QUERY_TABLE_OPENS_INDEXES: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static QUERY_TABLE_OPENS_EPOCHS: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
fn reset_query_table_open_counts() {
    QUERY_TABLE_OPENS_COMMITS.store(0, Ordering::Relaxed);
    QUERY_TABLE_OPENS_ENTITIES.store(0, Ordering::Relaxed);
    QUERY_TABLE_OPENS_INDEXES.store(0, Ordering::Relaxed);
    QUERY_TABLE_OPENS_EPOCHS.store(0, Ordering::Relaxed);
}

#[cfg(test)]
fn query_table_open_counts() -> QueryTableOpenCounts {
    QueryTableOpenCounts {
        commits: QUERY_TABLE_OPENS_COMMITS.load(Ordering::Relaxed),
        entities: QUERY_TABLE_OPENS_ENTITIES.load(Ordering::Relaxed),
        indexes: QUERY_TABLE_OPENS_INDEXES.load(Ordering::Relaxed),
        epochs: QUERY_TABLE_OPENS_EPOCHS.load(Ordering::Relaxed),
    }
}

#[derive(Clone, Copy)]
enum QueryTableKind {
    Commits,
    Entities,
    Indexes,
    Epochs,
}

/// Test-only observation of composite-query table opens. Compiled out of
/// normal builds (same pattern as `note_query_module_pool_dispatch`).
#[cfg(test)]
fn note_query_table_open(kind: QueryTableKind) {
    let counter = match kind {
        QueryTableKind::Commits => &QUERY_TABLE_OPENS_COMMITS,
        QueryTableKind::Entities => &QUERY_TABLE_OPENS_ENTITIES,
        QueryTableKind::Indexes => &QUERY_TABLE_OPENS_INDEXES,
        QueryTableKind::Epochs => &QUERY_TABLE_OPENS_EPOCHS,
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

#[cfg(not(test))]
const fn note_query_table_open(_kind: QueryTableKind) {}

impl QueryExecutionPort for RedbOperationalPorts {
    fn execute_query_page(
        &self,
        program: &QueryAccessProgramV1,
        parameters: &QueryParameters,
        prior: Option<&QueryContinuation>,
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        let diagnostics = query_execute_diagnostics_enabled();
        let (transaction, acquire_profile) = if diagnostics {
            self.begin_composite_read_profiled()
                .map_err(map_storage_query_error)?
        } else {
            (
                self.begin_composite_read()
                    .map_err(map_storage_query_error)?,
                Default::default(),
            )
        };
        note_query_table_open(QueryTableKind::Commits);
        let frontier_started = diagnostics.then(Instant::now);
        let (frontier, authority_profile) = if diagnostics {
            transaction
                .application_frontier_profiled()
                .map_err(map_storage_query_error)?
        } else {
            (
                transaction
                    .application_frontier()
                    .map_err(map_storage_query_error)?,
                Default::default(),
            )
        };
        let head = frontier.map_or(0, riffdb_types::CommitSequence::get);
        let mut profile = diagnostics.then(QueryExecuteProfile::default);
        if let Some(profile) = profile.as_mut() {
            profile.stage_ns[PUBLICATION_OUTER_LOCK] = acquire_profile.outer_lock_ns;
            profile.stage_ns[PUBLICATION_VIEW_CAPTURE] = acquire_profile.view_capture_ns;
            profile.stage_ns[FRONTIER_CAPTURE] = frontier_started.map_or(0, elapsed_nanos);
            let (transitions, bytes) = transaction.composite_overlay_diagnostic();
            profile.overlay_transitions = transitions;
            profile.overlay_bytes = bytes;
            profile.authority_tail_bytes = authority_profile.physical_bytes;
            profile.authority_tail_commands = authority_profile.logical_commands;
        }
        // Entities / index / epoch tables open on first touch only.
        let mut view = RedbQueryView {
            transaction: &transaction,
            entities_touched: false,
            indexes_touched: false,
            epochs_touched: false,
            head,
            program,
            parameters,
            profile,
        };
        let drive_started = diagnostics.then(Instant::now);
        let result = execute_page_in_snapshot(program, parameters, prior, &mut view);
        if let Some(mut profile) = view.profile.take() {
            let drive_ns = drive_started.map_or(0, elapsed_nanos);
            let nested_ns = profile.stage_ns[POINT_LOOKUP..NESTED_STAGE_END]
                .iter()
                .copied()
                .fold(0_u64, u64::saturating_add);
            profile.stage_ns[VIEW_CALL_RESIDUAL] = profile.view_total_ns.saturating_sub(nested_ns);
            profile.stage_ns[PROGRAM_DRIVE_EXCLUSIVE] =
                drive_ns.saturating_sub(profile.view_total_ns);
            record_query_execute_profile(profile);
        }
        result
    }

    fn execute_query_group(
        &self,
        requests: &[QueryExecutionRequest<'_>],
    ) -> Result<Vec<QueryOwnedSnapshot>, QueryExecutionError> {
        validate_query_execution_group(requests)?;
        let transaction = self
            .begin_composite_read()
            .map_err(map_storage_query_error)?;
        note_query_table_open(QueryTableKind::Commits);
        let head = transaction
            .application_frontier()
            .map_err(map_storage_query_error)?
            .map_or(0, riffdb_types::CommitSequence::get);
        requests
            .iter()
            .map(|request| {
                let mut view = RedbQueryView {
                    transaction: &transaction,
                    entities_touched: false,
                    indexes_touched: false,
                    epochs_touched: false,
                    head,
                    program: request.program(),
                    parameters: request.parameters(),
                    profile: None,
                };
                execute_in_snapshot(request.program(), request.parameters(), &mut view)
            })
            .collect()
    }

    fn execute_policy_query_group(
        &self,
        requests: &[QueryExecutionRequest<'_>],
        policy: &AuthorizedQueryRowPolicyContextV1,
    ) -> Result<Vec<QueryOwnedSnapshot>, QueryExecutionError> {
        validate_query_execution_group(requests)?;
        let transaction = self
            .begin_composite_read()
            .map_err(map_storage_query_error)?;
        note_query_table_open(QueryTableKind::Commits);
        let head = transaction
            .application_frontier()
            .map_err(map_storage_query_error)?
            .map_or(0, riffdb_types::CommitSequence::get);
        requests
            .iter()
            .map(|request| {
                let mut view = RedbQueryView {
                    transaction: &transaction,
                    entities_touched: false,
                    indexes_touched: false,
                    epochs_touched: false,
                    head,
                    program: request.program(),
                    parameters: request.parameters(),
                    profile: None,
                };
                execute_policy_page_in_snapshot(
                    request.program(),
                    request.parameters(),
                    None,
                    &mut view,
                    policy,
                )
            })
            .collect()
    }

    fn authorize_projected_candidates(
        &self,
        entity: EntityTypeId,
        candidates: &[EntityKey],
        policy: &AuthorizedQueryRowPolicyContextV1,
    ) -> Result<AuthorizedProjectedRowAdmissionV1, QueryExecutionError> {
        if candidates.len() > MAX_PROJECTED_POLICY_CANDIDATES_V1 {
            return Err(QueryExecutionError::BoundExceeded);
        }
        let transaction = self
            .begin_composite_read()
            .map_err(map_storage_query_error)?;
        note_query_table_open(QueryTableKind::Entities);
        let mut indexes_opened = false;
        let mut observations = Vec::with_capacity(candidates.len());
        for key in candidates {
            let target = EntityTarget::new(entity, key.clone())
                .map_err(|_| QueryExecutionError::BackendIntegrity)?;
            let Some(encoded) = transaction
                .read_value(JournalTable::Entities, encode_entity_key(key))
                .map_err(map_storage_query_error)?
            else {
                observations.push(ProjectedPolicyCandidateObservationV1::missing(key.clone()));
                continue;
            };
            let record = decode_entity_record_v1(&encoded)
                .map_err(map_storage_query_error)?
                .into_parts()
                .0;
            if record.target() != &target {
                return Err(QueryExecutionError::BackendIntegrity);
            }
            let lookups = policy
                .relationship_lookups(entity, record.fields())
                .map_err(|_| QueryExecutionError::BackendIntegrity)?;
            if !lookups.is_empty() && !indexes_opened {
                note_query_table_open(QueryTableKind::Indexes);
                indexes_opened = true;
            }
            let evidence = lookups
                .iter()
                .map(|lookup| authoritative_indexed_relationship_exists(&transaction, lookup))
                .collect::<Result<Vec<_>, _>>()
                .map_err(map_storage_query_error)?;
            observations.push(ProjectedPolicyCandidateObservationV1::current(
                key.clone(),
                record.fields().clone(),
                evidence,
            ));
        }
        policy
            .authorize_projected_candidates(entity, observations)
            .map_err(|_| QueryExecutionError::BackendIntegrity)
    }

    fn inspect_vector_evidence(
        &self,
        target: &VectorInspectionTargetV1,
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<VectorInspectionSnapshotV1, QueryExecutionError> {
        let lower_target = VectorObservationTargetV1::new(
            target.lineage().clone(),
            target.partition().clone(),
            target.entity(),
            target.field(),
        );
        if target
            .after()
            .is_some_and(|after| after.entity_type_id() != target.entity())
        {
            return Err(QueryExecutionError::InvalidProgram);
        }
        let transaction = self
            .begin_composite_read()
            .map_err(map_storage_query_error)?;
        let frontier = transaction
            .application_frontier()
            .map_err(map_storage_query_error)?;
        let observation_key = encode_vector_observation_key(&lower_target)
            .map_err(|_| QueryExecutionError::BackendIntegrity)?;
        let observation = transaction
            .read_value(JournalTable::VectorObservations, &observation_key)
            .map_err(map_storage_query_error)?
            .map(|bytes| {
                decode_vector_observation_v1(&bytes)
                    .map(|decoded| decoded.into_parts().0)
                    .map_err(map_storage_query_error)
            })
            .transpose()?;
        if observation
            .as_ref()
            .is_some_and(|observation| observation.target() != &lower_target)
        {
            return Err(QueryExecutionError::BackendIntegrity);
        }

        let prefix = encode_vector_evidence_index_prefix(&lower_target)
            .map_err(|_| QueryExecutionError::BackendIntegrity)?;
        let upper = exclusive_prefix_end(&prefix).ok_or(QueryExecutionError::BackendIntegrity)?;
        let start = target
            .after()
            .map_or_else(
                || Ok(prefix.clone()),
                |after| encode_vector_evidence_index_key(&lower_target, after),
            )
            .map_err(|_| QueryExecutionError::BackendIntegrity)?;
        let limit = usize::from(target.limit().get());
        let rows = transaction
            .read_range(
                JournalTable::VectorEvidenceIndex,
                &start,
                &upper,
                limit.saturating_add(2),
            )
            .map_err(map_storage_query_error)?;
        let mut candidates = Vec::with_capacity(limit);
        let mut more = false;
        for (key, value) in rows {
            let (decoded_target, entity_key) = decode_vector_evidence_index_key(&key)
                .map_err(|_| QueryExecutionError::BackendIntegrity)?;
            if target.after().is_some_and(|after| after == &entity_key) {
                continue;
            }
            if candidates.len() == limit {
                more = true;
                break;
            }
            let entry = decode_vector_evidence_index_v1(&value)
                .map_err(map_storage_query_error)?
                .into_parts()
                .0;
            if decoded_target != lower_target
                || entry.target() != &lower_target
                || entry.entity_key() != &entity_key
            {
                return Err(QueryExecutionError::BackendIntegrity);
            }
            candidates.push(VectorInspectionCandidateV1::new(
                entity_key,
                entry.newest_source_write(),
                entry
                    .embedding_write()
                    .map(|write| (write.sequence(), write.metadata().clone())),
            ));
        }
        let continuation = more.then(|| {
            candidates
                .last()
                .expect("non-exact bounded vector page is nonempty")
                .entity_key()
                .clone()
        });
        let admission = match policy {
            Some(policy) => {
                let mut observations = Vec::with_capacity(candidates.len());
                for candidate in &candidates {
                    let key = candidate.entity_key();
                    let entity_target = EntityTarget::new(target.entity(), key.clone())
                        .map_err(|_| QueryExecutionError::BackendIntegrity)?;
                    let Some(encoded) = transaction
                        .read_value(JournalTable::Entities, encode_entity_key(key))
                        .map_err(map_storage_query_error)?
                    else {
                        observations
                            .push(ProjectedPolicyCandidateObservationV1::missing(key.clone()));
                        continue;
                    };
                    let record = decode_entity_record_v1(&encoded)
                        .map_err(map_storage_query_error)?
                        .into_parts()
                        .0;
                    if record.target() != &entity_target {
                        return Err(QueryExecutionError::BackendIntegrity);
                    }
                    let lookups = policy
                        .relationship_lookups(target.entity(), record.fields())
                        .map_err(|_| QueryExecutionError::BackendIntegrity)?;
                    let evidence = lookups
                        .iter()
                        .map(|lookup| {
                            authoritative_indexed_relationship_exists(&transaction, lookup)
                        })
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(map_storage_query_error)?;
                    observations.push(ProjectedPolicyCandidateObservationV1::current(
                        key.clone(),
                        record.fields().clone(),
                        evidence,
                    ));
                }
                Some(
                    policy
                        .authorize_projected_candidates(target.entity(), observations)
                        .map_err(|_| QueryExecutionError::BackendIntegrity)?,
                )
            }
            None => None,
        };
        let (total_entities, stale_entities, model_counts, revision) = observation.map_or_else(
            || (0, 0, BTreeMap::new(), None),
            |observation| {
                (
                    observation.total_entities(),
                    observation.source_stale_entities(),
                    observation
                        .model_counts()
                        .map(|(metadata, count)| (metadata.clone(), count))
                        .collect(),
                    Some(observation.revision()),
                )
            },
        );
        Ok(VectorInspectionSnapshotV1::new(
            total_entities,
            stale_entities,
            model_counts,
            revision,
            frontier,
            candidates,
            continuation,
            !more,
            admission,
        ))
    }

    fn execute_operational_query_page(
        &self,
        program: &QueryAccessProgramV1,
        aggregates: &[riffdb_query_ir::OperationalAggregateV1],
        parameters: &QueryParameters,
        prior: Option<&QueryContinuation>,
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        let transaction = self
            .begin_composite_read()
            .map_err(map_storage_query_error)?;
        note_query_table_open(QueryTableKind::Commits);
        let head = transaction
            .application_frontier()
            .map_err(map_storage_query_error)?
            .map_or(0, riffdb_types::CommitSequence::get);
        let mut view = RedbQueryView {
            transaction: &transaction,
            entities_touched: false,
            indexes_touched: false,
            epochs_touched: false,
            head,
            program,
            parameters,
            profile: None,
        };
        riffdb_query_executor::execute_operational_page_in_snapshot(
            program, aggregates, parameters, prior, &mut view,
        )
    }

    fn execute_policy_query_page(
        &self,
        program: &QueryAccessProgramV1,
        parameters: &QueryParameters,
        prior: Option<&QueryContinuation>,
        policy: &AuthorizedQueryRowPolicyContextV1,
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        let transaction = self
            .begin_composite_read()
            .map_err(map_storage_query_error)?;
        note_query_table_open(QueryTableKind::Commits);
        let head = transaction
            .application_frontier()
            .map_err(map_storage_query_error)?
            .map_or(0, riffdb_types::CommitSequence::get);
        let mut view = RedbQueryView {
            transaction: &transaction,
            entities_touched: false,
            indexes_touched: false,
            epochs_touched: false,
            head,
            program,
            parameters,
            profile: None,
        };
        execute_policy_page_in_snapshot(program, parameters, prior, &mut view, policy)
    }

    fn execute_policy_operational_query_page(
        &self,
        program: &QueryAccessProgramV1,
        aggregates: &[riffdb_query_ir::OperationalAggregateV1],
        parameters: &QueryParameters,
        prior: Option<&QueryContinuation>,
        policy: &AuthorizedQueryRowPolicyContextV1,
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        let transaction = self
            .begin_composite_read()
            .map_err(map_storage_query_error)?;
        note_query_table_open(QueryTableKind::Commits);
        let head = transaction
            .application_frontier()
            .map_err(map_storage_query_error)?
            .map_or(0, riffdb_types::CommitSequence::get);
        let mut view = RedbQueryView {
            transaction: &transaction,
            entities_touched: false,
            indexes_touched: false,
            epochs_touched: false,
            head,
            program,
            parameters,
            profile: None,
        };
        execute_policy_operational_page_in_snapshot(
            program, aggregates, parameters, prior, &mut view, policy,
        )
    }

    fn execute_provider_query_page(
        &self,
        program: &QueryAccessProgramV1,
        parameters: &QueryParameters,
        prior: Option<&QueryContinuation>,
        policy_shape: ApplicationRoleHash,
        proof: &riffdb_projection::ResultSetEpochProofV1,
        batches: &[LongPatternCandidateBatch],
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        let transaction = self
            .begin_composite_read()
            .map_err(map_storage_query_error)?;
        note_query_table_open(QueryTableKind::Commits);
        let head = transaction
            .application_frontier()
            .map_err(map_storage_query_error)?
            .map_or(0, riffdb_types::CommitSequence::get);
        let mut view = RedbQueryView {
            transaction: &transaction,
            entities_touched: false,
            indexes_touched: false,
            epochs_touched: false,
            head,
            program,
            parameters,
            profile: None,
        };
        execute_provider_page_in_snapshot(
            program,
            parameters,
            prior,
            &mut view,
            policy_shape,
            proof,
            batches,
        )
    }

    fn execute_policy_provider_query_page(
        &self,
        program: &QueryAccessProgramV1,
        parameters: &QueryParameters,
        prior: Option<&QueryContinuation>,
        policy: &AuthorizedQueryRowPolicyContextV1,
        policy_shape: ApplicationRoleHash,
        proof: &riffdb_projection::ResultSetEpochProofV1,
        batches: &[LongPatternCandidateBatch],
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        let transaction = self
            .begin_composite_read()
            .map_err(map_storage_query_error)?;
        note_query_table_open(QueryTableKind::Commits);
        let head = transaction
            .application_frontier()
            .map_err(map_storage_query_error)?
            .map_or(0, riffdb_types::CommitSequence::get);
        let mut view = RedbQueryView {
            transaction: &transaction,
            entities_touched: false,
            indexes_touched: false,
            epochs_touched: false,
            head,
            program,
            parameters,
            profile: None,
        };
        execute_policy_provider_page_in_snapshot(
            program,
            parameters,
            prior,
            &mut view,
            policy,
            policy_shape,
            proof,
            batches,
        )
    }
}

fn map_storage_query_error(error: StorageError) -> QueryExecutionError {
    match storage_query_fault(&error) {
        QueryBackendFault::Unavailable => QueryExecutionError::BackendUnavailable,
        QueryBackendFault::Integrity => QueryExecutionError::BackendIntegrity,
        QueryBackendFault::LimitExceeded => QueryExecutionError::BackendLimitExceeded,
    }
}

const fn storage_query_fault(error: &StorageError) -> QueryBackendFault {
    match error.kind() {
        StorageErrorKind::Unavailable | StorageErrorKind::CommitStatusUnknown => {
            QueryBackendFault::Unavailable
        }
        StorageErrorKind::LimitExceeded => QueryBackendFault::LimitExceeded,
        StorageErrorKind::CorruptData
        | StorageErrorKind::IncompatibleFormat
        | StorageErrorKind::InvariantViolation
        | StorageErrorKind::SequenceExhausted
        | StorageErrorKind::HistoryPruned => QueryBackendFault::Integrity,
    }
}

struct RedbQueryView<'a> {
    transaction: &'a RedbReadAccess,
    entities_touched: bool,
    indexes_touched: bool,
    epochs_touched: bool,
    head: u64,
    program: &'a QueryAccessProgramV1,
    parameters: &'a QueryParameters,
    profile: Option<QueryExecuteProfile>,
}

impl RedbQueryView<'_> {
    fn touch_entities(&mut self) {
        if !self.entities_touched {
            note_query_table_open(QueryTableKind::Entities);
            self.entities_touched = true;
        }
    }

    fn touch_indexes(&mut self) {
        if !self.indexes_touched {
            note_query_table_open(QueryTableKind::Indexes);
            self.indexes_touched = true;
        }
    }

    fn touch_epochs(&mut self) {
        if !self.epochs_touched {
            note_query_table_open(QueryTableKind::Epochs);
            self.epochs_touched = true;
        }
    }

    fn read_entity(
        &mut self,
        target: &EntityTarget,
    ) -> Result<Option<riffdb_storage_api::StoredEntityRecordV1>, StorageError> {
        self.touch_entities();
        let key = encode_entity_key(target.key());
        let lookup_started = self.profile.as_ref().map(|_| {
            crate::journal::reset_point_read_substages();
            Instant::now()
        });
        let outcome = self.transaction.read_value(JournalTable::Entities, key);
        if let (Some(profile), Some(started)) = (self.profile.as_mut(), lookup_started) {
            profile.stage_ns[POINT_LOOKUP] =
                profile.stage_ns[POINT_LOOKUP].saturating_add(elapsed_nanos(started));
            profile.entity_point_reads = profile.entity_point_reads.saturating_add(1);
            let [open_ns, get_ns, copy_ns] = crate::journal::point_read_substages();
            profile.stage_ns[POINT_OPEN_TABLE] =
                profile.stage_ns[POINT_OPEN_TABLE].saturating_add(open_ns);
            profile.stage_ns[POINT_BTREE_GET] =
                profile.stage_ns[POINT_BTREE_GET].saturating_add(get_ns);
            profile.stage_ns[POINT_VALUE_COPY] =
                profile.stage_ns[POINT_VALUE_COPY].saturating_add(copy_ns);
        }
        let Some(encoded) = outcome? else {
            return Ok(None);
        };
        let decoded = if let Some(profile) = self.profile.as_mut() {
            let (decoded, decode) = decode_entity_record_v1_profiled(&encoded)?;
            profile.stage_ns[ENVELOPE_IDENTITY_BOUNDS] = profile.stage_ns[ENVELOPE_IDENTITY_BOUNDS]
                .saturating_add(decode.identity_bounds_ns);
            profile.stage_ns[PAYLOAD_CHECKSUM] =
                profile.stage_ns[PAYLOAD_CHECKSUM].saturating_add(decode.checksum_ns);
            profile.stage_ns[WIRE_PREFLIGHT] =
                profile.stage_ns[WIRE_PREFLIGHT].saturating_add(decode.wire_preflight_ns);
            profile.stage_ns[PROST_DECODE] =
                profile.stage_ns[PROST_DECODE].saturating_add(decode.prost_decode_ns);
            profile.stage_ns[CANONICAL_REENCODE] =
                profile.stage_ns[CANONICAL_REENCODE].saturating_add(decode.canonical_reencode_ns);
            profile.stage_ns[SEMANTIC_RECONSTRUCT] = profile.stage_ns[SEMANTIC_RECONSTRUCT]
                .saturating_add(decode.semantic_reconstruct_ns);
            decoded.into_parts().0
        } else {
            decode_entity_record_v1(&encoded)?.into_parts().0
        };
        let validate_started = self.profile.as_ref().map(|_| Instant::now());
        if decoded.target() != target {
            return Err(corrupt());
        }
        if let (Some(profile), Some(started)) = (self.profile.as_mut(), validate_started) {
            profile.stage_ns[TARGET_VALIDATE] =
                profile.stage_ns[TARGET_VALIDATE].saturating_add(elapsed_nanos(started));
        }
        Ok(Some(decoded))
    }

    fn read_epoch(&mut self, target: &PartitionIndexTarget) -> Result<u64, StorageError> {
        let started = self.profile.as_ref().map(|_| Instant::now());
        let epoch = self.read_epoch_inner(target);
        if let (Some(profile), Some(started)) = (self.profile.as_mut(), started) {
            profile.stage_ns[INDEX_EPOCH_READ] =
                profile.stage_ns[INDEX_EPOCH_READ].saturating_add(elapsed_nanos(started));
        }
        epoch
    }

    fn read_epoch_inner(&mut self, target: &PartitionIndexTarget) -> Result<u64, StorageError> {
        self.touch_epochs();
        let key = encode_partition_index_key(target);
        let Some(encoded) = self
            .transaction
            .read_value(JournalTable::IndexEpochs, &key)?
        else {
            return Ok(0);
        };
        let epoch = decode_index_epoch_v1(&encoded)?.into_parts().0;
        if epoch.target() != target {
            return Err(corrupt());
        }
        Ok(epoch.epoch().get())
    }

    /// Reads one bounded secondary-index window, charging the storage segment
    /// to `index_range_read` rather than leaving it in the drive residual.
    fn read_index_range(
        &mut self,
        direction: AccessDirection,
        start_inclusive: &[u8],
        end_exclusive: &[u8],
        max_rows: usize,
    ) -> Result<Vec<riffdb_storage_api::CompositeRow>, StorageError> {
        let started = self.profile.as_ref().map(|_| Instant::now());
        let rows = match direction {
            AccessDirection::Forward => self.transaction.read_range(
                JournalTable::SecondaryIndexes,
                start_inclusive,
                end_exclusive,
                max_rows,
            ),
            AccessDirection::Reverse => self.transaction.read_range_reverse(
                JournalTable::SecondaryIndexes,
                start_inclusive,
                end_exclusive,
                max_rows,
            ),
        };
        if let (Some(profile), Some(started)) = (self.profile.as_mut(), started) {
            profile.stage_ns[INDEX_RANGE_READ] =
                profile.stage_ns[INDEX_RANGE_READ].saturating_add(elapsed_nanos(started));
            profile.index_range_reads = profile.index_range_reads.saturating_add(1);
            if let Ok(rows) = &rows {
                profile.index_rows = profile
                    .index_rows
                    .saturating_add(u64::try_from(rows.len()).unwrap_or(u64::MAX));
            }
        }
        rows
    }

    fn note_program_step(&mut self) {
        if let Some(profile) = self.profile.as_mut() {
            profile.program_steps = profile.program_steps.saturating_add(1);
        }
    }

    /// Opens one read-view callback window, returning its start instant.
    fn begin_view_call(&self) -> Option<Instant> {
        self.profile.as_ref().map(|_| Instant::now())
    }

    /// Closes the window opened by [`Self::begin_view_call`].
    fn end_view_call(&mut self, started: Option<Instant>) {
        if let (Some(profile), Some(started)) = (self.profile.as_mut(), started) {
            profile.view_total_ns = profile.view_total_ns.saturating_add(elapsed_nanos(started));
        }
    }
}

impl QueryReadView for RedbQueryView<'_> {
    type Error = StorageError;

    fn fault(&self, error: &Self::Error) -> QueryBackendFault {
        storage_query_fault(error)
    }

    fn application_head(&self) -> u64 {
        self.head
    }

    fn point(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Option<QueryRow>, Self::Error> {
        let call = self.begin_view_call();
        let result = self.point_inner(step, predicates, policy);
        self.end_view_call(call);
        result
    }

    fn dependent_point_batch(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[Vec<BoundPredicate>],
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Vec<Option<QueryRow>>, Self::Error> {
        let call = self.begin_view_call();
        let result = self.dependent_point_batch_inner(step, predicates, policy);
        self.end_view_call(call);
        result
    }

    fn scan(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        limit: u64,
        after: Option<&[u8]>,
        after_inclusive: bool,
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryScanPage, Self::Error> {
        let call = self.begin_view_call();
        let result = self.scan_inner(step, predicates, limit, after, after_inclusive, policy);
        self.end_view_call(call);
        result
    }

    fn scan_covered(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        limit: u64,
        after: Option<&[u8]>,
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Option<CoveredResultBatch>, Self::Error> {
        let call = self.begin_view_call();
        let result = self.scan_covered_inner(step, predicates, limit, after, policy);
        self.end_view_call(call);
        result
    }

    fn nearest(
        &mut self,
        _step: &QueryAccessStep,
        _predicates: &[BoundPredicate],
        _k: u32,
        _policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryNearestPage, Self::Error> {
        // Row-store does not support vector nearest-neighbor search (ADR-0091).
        // Nearest queries must be routed through the columnar projection engine.
        Err(invariant())
    }
}

impl RedbQueryView<'_> {
    fn point_inner(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Option<QueryRow>, StorageError> {
        self.note_program_step();
        let started = self.profile.as_ref().map(|_| Instant::now());
        let plan = RowMaterializePlan::for_step(self.program, step)?;
        if let (Some(profile), Some(started)) = (self.profile.as_mut(), started) {
            profile.stage_ns[SCAN_SETUP] =
                profile.stage_ns[SCAN_SETUP].saturating_add(elapsed_nanos(started));
        }
        self.point_with_plan(step, predicates, &plan, policy)
    }

    fn dependent_point_batch_inner(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[Vec<BoundPredicate>],
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Vec<Option<QueryRow>>, StorageError> {
        if !matches!(
            step.access(),
            QueryAccessKind::DependentPointBatch { .. }
                | QueryAccessKind::CandidateRootHydration { .. }
        ) {
            return Err(invariant());
        }
        self.note_program_step();
        let started = self.profile.as_ref().map(|_| Instant::now());
        // One plan for the whole batch (not per predicate/row).
        let plan = RowMaterializePlan::for_step(self.program, step)?;
        if let (Some(profile), Some(started)) = (self.profile.as_mut(), started) {
            profile.stage_ns[SCAN_SETUP] =
                profile.stage_ns[SCAN_SETUP].saturating_add(elapsed_nanos(started));
        }
        predicates
            .iter()
            .map(|predicates| self.point_with_plan(step, predicates, &plan, policy))
            .collect()
    }

    fn scan_inner(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        limit: u64,
        after: Option<&[u8]>,
        after_inclusive: bool,
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryScanPage, StorageError> {
        self.note_program_step();
        let setup_started = self.profile.as_ref().map(|_| Instant::now());
        let direction = match step.access() {
            QueryAccessKind::Index { direction, .. }
            | QueryAccessKind::PartitionSetIndex { direction, .. } => direction,
            _ => return Err(invariant()),
        };
        let schema = step.internal_index_key_schema().ok_or_else(invariant)?;
        let partition_value = predicates
            .iter()
            .find(|predicate| {
                predicate.operator() == riffdb_query_ir::QueryPredicateOperator::Equal
            })
            .filter(|predicate| {
                step.predicates().iter().any(|source| {
                    source.field() == predicate.field()
                        && matches!(
                            source.value(),
                            riffdb_query_ir::QueryPredicateValue::Parameter(name)
                                if name == self.program.partition_parameter()
                        )
                })
            })
            .map(BoundPredicate::value)
            .ok_or_else(invariant)?;
        let partition = step
            .internal_partition_key_schema()
            .encode_partition(std::slice::from_ref(partition_value))
            .map_err(|_| invariant())?;
        let generation_target =
            PartitionIndexTarget::new(partition, step.internal_index_id().ok_or_else(invariant)?);
        if let (Some(profile), Some(started)) = (self.profile.as_mut(), setup_started) {
            profile.stage_ns[SCAN_SETUP] =
                profile.stage_ns[SCAN_SETUP].saturating_add(elapsed_nanos(started));
        }
        let epoch = self.read_epoch(&generation_target)?;
        let setup_started = self.profile.as_ref().map(|_| Instant::now());
        let page_limit =
            usize::try_from(limit).map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        let fetch_limit = page_limit.saturating_add(1);
        let mut entries = Vec::<(IndexEntryKey, QueryRow)>::new();
        let scan_ceiling = usize::try_from(MAX_QUERY_SCANNED_ROWS)
            .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        let mut inspected = 0usize;
        let mut partition_candidates = 0usize;
        let schedule = riffdb_query_executor::bound_index_range_schedule_v1(step, predicates)
            .map_err(|_| invariant())?;
        let plan = RowMaterializePlan::for_step(self.program, step)?;
        if let (Some(profile), Some(started)) = (self.profile.as_mut(), setup_started) {
            profile.stage_ns[SCAN_SETUP] =
                profile.stage_ns[SCAN_SETUP].saturating_add(elapsed_nanos(started));
        }

        self.touch_indexes();
        'ranges: for range in schedule.ranges() {
            let Some(window) = range.resume_window(*direction, after, after_inclusive) else {
                continue;
            };
            let remaining_scan = scan_ceiling.saturating_sub(inspected);
            if remaining_scan == 0 {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
            let inclusive_end = window.include_end_equal().then(|| {
                let mut end = window.end_exclusive().to_vec();
                end.push(0);
                end
            });
            let rows = self.read_index_range(
                *direction,
                window.start_inclusive(),
                inclusive_end
                    .as_deref()
                    .unwrap_or_else(|| window.end_exclusive()),
                remaining_scan,
            )?;
            let inspected_this_prefix = rows.len();
            inspected = inspected
                .checked_add(inspected_this_prefix)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            for entry in rows {
                if matches!(direction, AccessDirection::Forward)
                    && window.skip_start_equal()
                    && entry.0.as_ref() == window.start_inclusive()
                {
                    continue;
                }
                let entry_started = self.profile.as_ref().map(|_| Instant::now());
                let decoded = decode_current_index_entry(entry)?;
                if decoded.1.partition_key() != generation_target.partition_key() {
                    if let (Some(profile), Some(started)) = (self.profile.as_mut(), entry_started) {
                        profile.stage_ns[INDEX_ENTRY_DECODE] = profile.stage_ns[INDEX_ENTRY_DECODE]
                            .saturating_add(elapsed_nanos(started));
                    }
                    continue;
                }
                partition_candidates = partition_candidates
                    .checked_add(1)
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                // This scan reads the owning entity row, so the index key's
                // leading component values are never looked at. Validating the
                // key without materializing them keeps every rejection this
                // path had and drops two per-row vectors plus a key copy.
                let entity_key = schema
                    .decode_index_entity_key(&decoded.0)
                    .map_err(|_| corrupt())?;
                let target = EntityTarget::new(step.internal_entity_id(), entity_key)
                    .map_err(|_| corrupt())?;
                if let (Some(profile), Some(started)) = (self.profile.as_mut(), entry_started) {
                    profile.stage_ns[INDEX_ENTRY_DECODE] =
                        profile.stage_ns[INDEX_ENTRY_DECODE].saturating_add(elapsed_nanos(started));
                }
                let record = self.read_entity(&target)?.ok_or_else(corrupt)?;
                let policy_started = self.profile.as_ref().map(|_| Instant::now());
                let allowed =
                    self.allows_policy_record(policy, step.internal_entity_id(), record.fields())?;
                if let (Some(profile), Some(started)) = (self.profile.as_mut(), policy_started) {
                    profile.stage_ns[ROW_POLICY] =
                        profile.stage_ns[ROW_POLICY].saturating_add(elapsed_nanos(started));
                }
                if !allowed {
                    continue;
                }
                let materialize_started = self.profile.as_ref().map(|_| Instant::now());
                let row = plan.materialize(&record)?;
                if let (Some(profile), Some(started)) = (self.profile.as_mut(), materialize_started)
                {
                    profile.stage_ns[ROW_MATERIALIZE] =
                        profile.stage_ns[ROW_MATERIALIZE].saturating_add(elapsed_nanos(started));
                }
                entries.push((decoded.0, row));
                if entries.len() == fetch_limit {
                    break 'ranges;
                }
            }
            if inspected_this_prefix == remaining_scan {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
        }
        // Continuation only when an extra matching entry was observed. Bound is
        // the last included key; the peeked row is never returned. Charge the
        // peeked observation to scanned_rows for accurate fuel accounting.
        let has_more = entries.len() > page_limit;
        if has_more {
            entries.truncate(page_limit);
        }
        let scanned_rows = u64::try_from(partition_candidates)
            .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        let continuation = has_more
            .then(|| entries.last().map(|entry| entry.0.as_bytes().to_vec()))
            .flatten();
        let rows = entries.into_iter().map(|(_, row)| row).collect();
        match continuation {
            Some(continuation) => {
                QueryScanPage::continued(rows, epoch, scanned_rows.max(1), continuation)
                    .ok_or_else(invariant)
            }
            None => {
                QueryScanPage::policy_exact_end(rows, epoch, scanned_rows).ok_or_else(invariant)
            }
        }
    }

    fn scan_covered_inner(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        limit: u64,
        after: Option<&[u8]>,
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Option<CoveredResultBatch>, StorageError> {
        let Some(layout) = step.covered_result_layout() else {
            return Ok(None);
        };
        self.note_program_step();
        let setup_started = self.profile.as_ref().map(|_| Instant::now());
        if policy.is_some() {
            return Err(invariant());
        }
        let QueryAccessKind::Index { direction, .. } = step.access() else {
            return Err(invariant());
        };
        let schema = step.internal_index_key_schema().ok_or_else(invariant)?;
        let partition_value = self
            .parameters
            .get(self.program.partition_parameter())
            .ok_or_else(invariant)?;
        let partition = step
            .internal_partition_key_schema()
            .encode_partition(std::slice::from_ref(partition_value))
            .map_err(|_| invariant())?;
        let generation_target =
            PartitionIndexTarget::new(partition, step.internal_index_id().ok_or_else(invariant)?);
        if let (Some(profile), Some(started)) = (self.profile.as_mut(), setup_started) {
            profile.stage_ns[SCAN_SETUP] =
                profile.stage_ns[SCAN_SETUP].saturating_add(elapsed_nanos(started));
        }
        let epoch = self.read_epoch(&generation_target)?;
        let setup_started = self.profile.as_ref().map(|_| Instant::now());
        let page_limit =
            usize::try_from(limit).map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        let fetch_limit = page_limit.saturating_add(1);
        let scan_ceiling = usize::try_from(MAX_QUERY_SCANNED_ROWS)
            .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        let mut entries = Vec::<(IndexEntryKey, Vec<CanonicalValue>)>::new();
        let mut inspected = 0usize;
        let mut partition_candidates = 0usize;
        // CanonicalRecord already stores fields in stable-ID order. Normalize
        // the compiler witness once, rather than allocating and sorting two
        // temporary ID vectors for every candidate row.
        let mut expected_cover_ids = layout.internal_cover_field_ids().to_vec();
        expected_cover_ids.sort_unstable();
        let layout_cover_positions = layout
            .fields()
            .iter()
            .map(|field| match field.internal_source() {
                CoveredResultSourceV1::Cover => expected_cover_ids
                    .binary_search(&field.internal_field_id())
                    .map_err(|_| invariant()),
                CoveredResultSourceV1::IndexKey(_) | CoveredResultSourceV1::EntityKey(_) => {
                    Ok(usize::MAX)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let schedule = riffdb_query_executor::bound_index_range_schedule_v1(step, predicates)
            .map_err(|_| invariant())?;
        if let (Some(profile), Some(started)) = (self.profile.as_mut(), setup_started) {
            profile.stage_ns[SCAN_SETUP] =
                profile.stage_ns[SCAN_SETUP].saturating_add(elapsed_nanos(started));
        }

        self.touch_indexes();
        'ranges: for range in schedule.ranges() {
            let Some(window) = range.resume_window(*direction, after, false) else {
                continue;
            };
            let remaining_scan = scan_ceiling.saturating_sub(inspected);
            if remaining_scan == 0 {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
            let rows = self.read_index_range(
                *direction,
                window.start_inclusive(),
                window.end_exclusive(),
                remaining_scan,
            )?;
            let inspected_this_prefix = rows.len();
            inspected = inspected
                .checked_add(inspected_this_prefix)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            let entries_started = self.profile.as_ref().map(|_| Instant::now());
            for entry in rows {
                if matches!(direction, AccessDirection::Forward)
                    && window.skip_start_equal()
                    && entry.0.as_ref() == window.start_inclusive()
                {
                    continue;
                }
                let (entry_key, stored) = decode_current_index_entry(entry)?;
                if stored.partition_key() != generation_target.partition_key() {
                    continue;
                }
                partition_candidates = partition_candidates
                    .checked_add(1)
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                let contract = self.program.contract();
                if stored.schema_binding().lineage() != contract.lineage()
                    || stored.schema_binding().contract_version() != contract.version()
                    || stored.schema_binding().bundle_hash() != contract.bundle_hash()
                {
                    return Err(corrupt());
                }
                let covered_fields = stored.covered_values().fields();
                if covered_fields.len() != expected_cover_ids.len()
                    || covered_fields
                        .iter()
                        .zip(&expected_cover_ids)
                        .any(|((actual, _), expected)| actual != expected)
                {
                    return Err(corrupt());
                }
                let decoded_key = schema.decode_index(&entry_key).map_err(|_| corrupt())?;
                let entity_values = step
                    .internal_entity_key_schema()
                    .decode_entity(decoded_key.entity_key())
                    .map_err(|_| corrupt())?;
                let values = layout
                    .fields()
                    .iter()
                    .zip(&layout_cover_positions)
                    .map(|(field, cover_position)| match field.internal_source() {
                        CoveredResultSourceV1::IndexKey(position) => decoded_key
                            .values()
                            .get(usize::from(position))
                            .cloned()
                            .ok_or_else(corrupt),
                        CoveredResultSourceV1::EntityKey(position) => entity_values
                            .get(usize::from(position))
                            .cloned()
                            .ok_or_else(corrupt),
                        CoveredResultSourceV1::Cover => covered_fields
                            .get(*cover_position)
                            .map(|(_, value)| value.clone())
                            .ok_or_else(corrupt),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if !covered_row_matches_predicates_v1(layout, &values, predicates)
                    .map_err(|_| invariant())?
                {
                    continue;
                }
                entries.push((entry_key, values));
                if entries.len() == fetch_limit {
                    if let (Some(profile), Some(started)) = (self.profile.as_mut(), entries_started)
                    {
                        profile.stage_ns[INDEX_ENTRY_DECODE] = profile.stage_ns[INDEX_ENTRY_DECODE]
                            .saturating_add(elapsed_nanos(started));
                    }
                    break 'ranges;
                }
            }
            if let (Some(profile), Some(started)) = (self.profile.as_mut(), entries_started) {
                profile.stage_ns[INDEX_ENTRY_DECODE] =
                    profile.stage_ns[INDEX_ENTRY_DECODE].saturating_add(elapsed_nanos(started));
            }
            if inspected_this_prefix == remaining_scan {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
        }

        let has_more = entries.len() > page_limit;
        if has_more {
            entries.truncate(page_limit);
        }
        let scanned_rows = u64::try_from(partition_candidates)
            .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        let continuation = has_more
            .then(|| entries.last().map(|entry| entry.0.as_bytes().to_vec()))
            .flatten();
        let rows = entries.into_iter().map(|(_, row)| row).collect();
        CoveredResultBatch::checked(layout.clone(), rows, epoch, scanned_rows, 0, continuation)
            .map(Some)
            .ok_or_else(invariant)
    }

    fn point_with_plan(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        plan: &RowMaterializePlan,
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Option<QueryRow>, StorageError> {
        let key_fields = match step.access() {
            QueryAccessKind::Point { key_fields }
            | QueryAccessKind::DependentPointBatch { key_fields, .. }
            | QueryAccessKind::CandidateRootHydration { key_fields, .. } => key_fields,
            QueryAccessKind::Index { .. }
            | QueryAccessKind::PartitionSetIndex { .. }
            | QueryAccessKind::LongPatternCandidate { .. } => {
                return Err(invariant());
            }
            QueryAccessKind::Nearest { .. } => return Err(invariant()),
        };
        let values = key_fields
            .iter()
            .map(|field| exact_value(predicates, field))
            .collect::<Result<Vec<_>, _>>()?;
        if values
            .iter()
            .any(|value| matches!(value, CanonicalValue::Null))
        {
            return Ok(None);
        }
        let key = step
            .internal_entity_key_schema()
            .encode_entity(&values)
            .map_err(|_| invariant())?;
        let target = EntityTarget::new(step.internal_entity_id(), key).map_err(|_| invariant())?;
        let Some(record) = self.read_entity(&target)? else {
            return Ok(None);
        };
        let policy_started = self.profile.as_ref().map(|_| Instant::now());
        let allowed =
            self.allows_policy_record(policy, step.internal_entity_id(), record.fields())?;
        if let (Some(profile), Some(started)) = (self.profile.as_mut(), policy_started) {
            profile.stage_ns[ROW_POLICY] =
                profile.stage_ns[ROW_POLICY].saturating_add(elapsed_nanos(started));
        }
        if !allowed {
            return Ok(None);
        }
        let materialize_started = self.profile.as_ref().map(|_| Instant::now());
        let result = plan.materialize(&record).map(Some);
        if let (Some(profile), Some(started)) = (self.profile.as_mut(), materialize_started) {
            profile.stage_ns[ROW_MATERIALIZE] =
                profile.stage_ns[ROW_MATERIALIZE].saturating_add(elapsed_nanos(started));
        }
        result
    }

    fn allows_policy_record(
        &mut self,
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
        entity: riffdb_types::EntityTypeId,
        row: &riffdb_types::CanonicalRecord,
    ) -> Result<bool, StorageError> {
        let Some(policy) = policy else {
            return Ok(true);
        };
        if !policy.protects(entity) {
            return Ok(true);
        }
        let lookups = policy
            .relationship_lookups(entity, row)
            .map_err(|_| invariant())?;
        let mut evidence = Vec::with_capacity(lookups.len());
        for lookup in &lookups {
            evidence.push(self.indexed_relationship_exists(lookup)?);
        }
        Ok(policy.allows(entity, row, &evidence))
    }

    fn indexed_relationship_exists(
        &mut self,
        lookup: &AuthorizedIndexedRelationshipLookupV1,
    ) -> Result<bool, StorageError> {
        let upper = exclusive_prefix_end(lookup.index_prefix()).ok_or_else(invariant)?;
        self.touch_indexes();
        let maximum = usize::try_from(MAX_QUERY_SCANNED_ROWS)
            .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        let rows = self.read_index_range(
            AccessDirection::Forward,
            lookup.index_prefix(),
            &upper,
            maximum,
        )?;
        if rows.len() == maximum {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let started = self.profile.as_ref().map(|_| Instant::now());
        for row in rows {
            let (_, entry) = decode_current_index_entry(row)?;
            if entry.partition_key() == lookup.partition() {
                if let (Some(profile), Some(started)) = (self.profile.as_mut(), started) {
                    profile.stage_ns[INDEX_ENTRY_DECODE] =
                        profile.stage_ns[INDEX_ENTRY_DECODE].saturating_add(elapsed_nanos(started));
                }
                return Ok(true);
            }
        }
        if let (Some(profile), Some(started)) = (self.profile.as_mut(), started) {
            profile.stage_ns[INDEX_ENTRY_DECODE] =
                profile.stage_ns[INDEX_ENTRY_DECODE].saturating_add(elapsed_nanos(started));
        }
        Ok(false)
    }
}

fn decode_current_index_entry(
    entry: riffdb_storage_api::CompositeRow,
) -> Result<(IndexEntryKey, riffdb_storage_api::StoredIndexEntryV2), StorageError> {
    let (physical_key, encoded) = entry;
    let key = decode_index_entry_key(&physical_key).map_err(|_| corrupt())?;
    let decoded = decode_index_entry_v2(&encoded)?.into_parts().0;
    if decoded.key() != &key {
        return Err(corrupt());
    }
    Ok((key, decoded))
}

fn exclusive_prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    let position = upper.iter().rposition(|byte| *byte != u8::MAX)?;
    upper[position] = upper[position].saturating_add(1);
    upper.truncate(position + 1);
    Some(upper)
}

fn authoritative_indexed_relationship_exists(
    transaction: &RedbReadAccess,
    lookup: &AuthorizedIndexedRelationshipLookupV1,
) -> Result<bool, StorageError> {
    let upper = exclusive_prefix_end(lookup.index_prefix()).ok_or_else(invariant)?;
    let maximum = usize::try_from(MAX_QUERY_SCANNED_ROWS)
        .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
    let rows = transaction.read_range(
        JournalTable::SecondaryIndexes,
        lookup.index_prefix(),
        &upper,
        maximum,
    )?;
    if rows.len() == maximum {
        return Err(storage_error(StorageErrorKind::LimitExceeded));
    }
    for row in rows {
        let (_, entry) = decode_current_index_entry(row)?;
        if entry.partition_key() == lookup.partition() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn exact_value(predicates: &[BoundPredicate], field: &str) -> Result<CanonicalValue, StorageError> {
    predicates
        .iter()
        .find(|predicate| {
            predicate.field() == field && predicate.operator() == QueryPredicateOperator::Equal
        })
        .map(|predicate| predicate.value().clone())
        .ok_or_else(invariant)
}

/// Per-step field-name interning for one-pass row materialization.
///
/// Callers build this once per access step (scan loop, single point, or whole
/// dependent-point batch) and share it across every row of that step so
/// entity/field `Arc<str>` names are not reconstructed per row.
struct RowMaterializePlan {
    entity: Arc<str>,
    /// Needed `(FieldId, name)` pairs sorted by field ID for a dual-pointer merge
    /// against the canonically ordered stored record.
    needed: Vec<(FieldId, Arc<str>)>,
}

impl RowMaterializePlan {
    fn for_step(
        program: &QueryAccessProgramV1,
        step: &QueryAccessStep,
    ) -> Result<Self, StorageError> {
        let access = program
            .internal_entity_access(step.entity())
            .ok_or_else(invariant)?;
        let mut needed = access
            .internal_fields()
            .map(|(name, id)| (id, Arc::<str>::from(name)))
            .collect::<Vec<_>>();
        needed.sort_by_key(|(id, _)| id.get());
        Ok(Self {
            entity: Arc::<str>::from(step.entity()),
            needed,
        })
    }

    fn materialize(
        &self,
        record: &riffdb_storage_api::StoredEntityRecordV1,
    ) -> Result<QueryRow, StorageError> {
        let stored = record.fields().fields();
        let mut fields = BTreeMap::new();
        let mut store_index = 0usize;
        // Merge requires needed FieldIds unique (name uniqueness is plan-checked;
        // duplicate FieldIds would yield CorruptData rather than last-wins map).
        for (need_id, name) in &self.needed {
            while store_index < stored.len() && stored[store_index].0.get() < need_id.get() {
                store_index += 1;
            }
            if store_index >= stored.len() || stored[store_index].0 != *need_id {
                return Err(corrupt());
            }
            // Single ownership transfer into the row pipeline for this field value.
            fields.insert(Arc::clone(name), stored[store_index].1.clone());
            store_index += 1;
        }
        QueryRow::from_shared(Arc::clone(&self.entity), fields)
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))
    }
}

const fn invariant() -> StorageError {
    storage_error(StorageErrorKind::InvariantViolation)
}

const fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use riffdb_contract_compiler::compile_contract_source;
    use riffdb_query_compiler::compile_query;
    use riffdb_query_executor::{
        QueryContinuation, QueryExecutionPort, QueryParameters, QueryResultValue,
    };
    use riffdb_query_ir::SymbolicCatalog;
    use riffdb_riffql_syntax::parse_query;
    use riffdb_storage_api::{
        ApplicationSequenceAllocator, DatabaseInitializationPort, DurableKeySchemaBindingV1,
        EntityTarget, StoredEntityRecordV1, StoredIndexEntryV2,
    };
    use riffdb_types::{
        AggregateTypeId, CanonicalRecord, CanonicalValue, CommitSequence, DatabaseId,
        EntityVersion, PartitionKeyBuilder,
    };

    use super::*;
    use crate::codec::{
        encode_application_sequence_allocator_v1, encode_entity_record_v1, encode_index_entry_v2,
    };
    use crate::keys::{encode_application_sequence_key, encode_entity_key};
    use crate::layout::{COMMITS, ENTITIES, META, META_APPLICATION_SEQUENCE, SECONDARY_INDEXES};
    use crate::store::RedbStore;

    const CONTRACT: &str = include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");
    const POINT_QUERY: &str = r#"
query PointTicket(
    $organization_id: Organization.organization_id,
    $ticket_id: Ticket.ticket_id,
) {
    one ticket from Ticket
        where organization_id == $organization_id && ticket_id == $ticket_id
        else NotFound
    return Found { ticket: ticket { ticket_id title } }
    outcomes Found | NotFound
}
"#;
    const MEMBERS_QUERY: &str = r#"
query ProjectMembers(
    $organization_id: Organization.organization_id,
    $project_id: Project.project_id,
    $after: Cursor?,
) {
    many memberships from ProjectMember
        where organization_id == $organization_id && project_id == $project_id
        order by user_id asc
        take 1 after $after
    return Found { members: memberships { user_id role } }
    outcomes Found
}
"#;
    const MEMBERS_RANGE_QUERY: &str = r#"
query ProjectMembersInRange(
    $organization_id: Organization.organization_id,
    $project_id: Project.project_id,
    $lower: ProjectMember.user_id,
    $upper: ProjectMember.user_id,
    $after: Cursor?,
) {
    many memberships from ProjectMember
        where organization_id == $organization_id && project_id == $project_id
          && user_id >= $lower && user_id < $upper
        order by user_id asc
        take 1 after $after
    return Found { members: memberships { user_id role } }
    outcomes Found
}
"#;
    const BOARD_QUERY: &str = include_str!("../../../queries/ticketdesk/board_page_450.riffq");
    /// Whole-directory scope: the database and every side file it grows live
    /// in one [`crate::test_path::ScopedDirectory`] removed on drop — pass,
    /// fail, or panic.
    struct TestPath(
        PathBuf,
        // Held only so `Drop` removes the whole scope.
        #[allow(dead_code)] crate::test_path::ScopedDirectory,
    );

    impl TestPath {
        fn new() -> Self {
            let scope = crate::test_path::ScopedDirectory::new("query");
            Self(scope.join("db.redb"), scope)
        }
    }

    #[test]
    fn point_query_frontier_does_not_decode_retained_command_history() {
        let bundle = compile_contract_source(CONTRACT).expect("contract");
        let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
        let program =
            compile_query(&parse_query(POINT_QUERY).expect("parse"), &catalog).expect("program");
        let step = &program.steps()[0];
        let organization = CanonicalValue::Uuid([1; 16]);
        let ticket = CanonicalValue::Uuid([2; 16]);
        let key = step
            .internal_entity_key_schema()
            .encode_entity(&[organization.clone(), ticket.clone()])
            .expect("key");
        let target = EntityTarget::new(step.internal_entity_id(), key).expect("target");
        let access = program
            .internal_entity_access("Ticket")
            .expect("Ticket access");
        let fields = CanonicalRecord::new(vec![
            (
                access
                    .internal_field_id("organization_id")
                    .expect("organization field"),
                organization.clone(),
            ),
            (
                access.internal_field_id("ticket_id").expect("ticket field"),
                ticket.clone(),
            ),
            (
                access.internal_field_id("title").expect("title field"),
                CanonicalValue::string("one transaction").expect("title"),
            ),
        ])
        .expect("fields");
        let record = StoredEntityRecordV1::new(
            target,
            EntityVersion::first(),
            bundle.contract_version(),
            DurableKeySchemaBindingV1::new(
                bundle.lineage().clone(),
                bundle.contract_version(),
                bundle.bundle_hash(),
            ),
            fields,
        )
        .expect("record");

        let path = TestPath::new();
        let mut store = RedbStore::open(&path.0).expect("store");
        let database_id =
            DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x11; 10])
                .expect("database ID");
        store.initialize_database(database_id).expect("initialize");
        let ports = RedbOperationalPorts {
            shared: Arc::clone(&store.shared),
        };
        let access_write = ports.begin_write().expect("write");
        {
            let encoded = encode_entity_record_v1(&record).expect("encode");
            let mut table = access_write
                .transaction()
                .expect("transaction")
                .open_table(ENTITIES)
                .expect("entities");
            table
                .insert(encode_entity_key(record.target().key()), encoded.as_bytes())
                .expect("insert");
        }
        access_write.commit().expect("commit entity seed");
        drop(ports);
        drop(store);

        let reopened = RedbStore::open(&path.0).expect("reopen store");
        let ports = crate::store::RedbDormantPorts {
            shared: reopened.shared,
        }
        .into_operational_after_catalog_validation()
        .expect("activate restarted ports");

        // The restarted operational query must derive its frontier from the
        // allocator, not repeat the command-segment validation owned by
        // startup and recovery. An opaque post-activation row makes any
        // accidental decode fail this real QueryExecutionPort regression.
        let first = CommitSequence::first();
        let access_write = ports.begin_write().expect("write opaque authority");
        {
            let mut table = access_write
                .transaction()
                .expect("transaction")
                .open_table(COMMITS)
                .expect("commits");
            table
                .insert(
                    encode_application_sequence_key(first).as_slice(),
                    &[0x5a_u8; 4096][..],
                )
                .expect("insert opaque retained command history");
        }
        {
            let allocator = encode_application_sequence_allocator_v1(
                ApplicationSequenceAllocator::Next(first.checked_next().expect("second")),
            )
            .expect("allocator");
            let mut table = access_write
                .transaction()
                .expect("transaction")
                .open_table(META)
                .expect("meta");
            table
                .insert(META_APPLICATION_SEQUENCE, allocator.as_bytes())
                .expect("advance allocator");
        }
        access_write.commit().expect("commit opaque authority");

        let parameters = QueryParameters::checked(BTreeMap::from([
            ("organization_id".to_owned(), organization),
            ("ticket_id".to_owned(), ticket),
        ]))
        .expect("parameters");
        let _serial = QUERY_TABLE_OPEN_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        reset_query_table_open_counts();
        let snapshot = ports.execute_query(&program, &parameters).expect("query");
        assert_eq!(snapshot.outcome(), "Found");
        assert!(matches!(
            snapshot.fields().get("ticket"),
            Some(QueryResultValue::One(row)) if row.field("title").is_some()
        ));
        let opens = query_table_open_counts();
        assert_eq!(
            opens,
            QueryTableOpenCounts {
                commits: 1,
                entities: 1,
                indexes: 0,
                epochs: 0,
            },
            "point query opens only commits (head) and entities; eager open would also open indexes/epochs"
        );
    }

    #[test]
    fn index_page_batches_entity_reads_inside_the_same_redb_transaction() {
        let bundle = compile_contract_source(CONTRACT).expect("contract");
        let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
        let program =
            compile_query(&parse_query(MEMBERS_QUERY).expect("parse"), &catalog).expect("program");
        let step = &program.steps()[0];
        let organization = CanonicalValue::Uuid([1; 16]);
        let project = CanonicalValue::Uuid([2; 16]);
        let binding = DurableKeySchemaBindingV1::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
        );
        let access = program
            .internal_entity_access("ProjectMember")
            .expect("access");
        let mut records = Vec::new();
        let mut indexes = Vec::new();
        for (ordinal, partition_ordinal) in [(2_u8, 9_u8), (3_u8, 1_u8), (4_u8, 1_u8)] {
            let user = CanonicalValue::Uuid([ordinal; 16]);
            let key = step
                .internal_entity_key_schema()
                .encode_entity(&[organization.clone(), project.clone(), user.clone()])
                .expect("entity key");
            let target = EntityTarget::new(step.internal_entity_id(), key.clone()).expect("target");
            let fields = CanonicalRecord::new(vec![
                (
                    access
                        .internal_field_id("organization_id")
                        .expect("organization"),
                    organization.clone(),
                ),
                (
                    access.internal_field_id("project_id").expect("project"),
                    project.clone(),
                ),
                (
                    access.internal_field_id("user_id").expect("user"),
                    user.clone(),
                ),
                (
                    access.internal_field_id("role").expect("role"),
                    CanonicalValue::string("member").expect("role"),
                ),
            ])
            .expect("fields");
            records.push(
                StoredEntityRecordV1::new(
                    target,
                    EntityVersion::first(),
                    bundle.contract_version(),
                    binding.clone(),
                    fields,
                )
                .expect("record"),
            );
            let index_key = step
                .internal_index_key_schema()
                .expect("index schema")
                .encode_index(&[organization.clone(), project.clone(), user], key)
                .expect("index key");
            let mut partition =
                PartitionKeyBuilder::new(AggregateTypeId::new(4).expect("aggregate"));
            partition
                .push_uuid(&[partition_ordinal; 16])
                .expect("partition component");
            indexes.push(
                StoredIndexEntryV2::new(
                    index_key,
                    binding.clone(),
                    CanonicalRecord::new(Vec::new()).expect("cover"),
                    partition.finish().expect("partition"),
                )
                .expect("index row"),
            );
        }

        let path = TestPath::new();
        let mut store = RedbStore::open(&path.0).expect("store");
        let database_id =
            DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x12; 10])
                .expect("database ID");
        store.initialize_database(database_id).expect("initialize");
        let ports = RedbOperationalPorts {
            shared: Arc::clone(&store.shared),
        };
        let access_write = ports.begin_write().expect("write");
        {
            let mut table = access_write
                .transaction()
                .expect("transaction")
                .open_table(ENTITIES)
                .expect("entities");
            for record in &records {
                let encoded = encode_entity_record_v1(record).expect("encode entity");
                table
                    .insert(encode_entity_key(record.target().key()), encoded.as_bytes())
                    .expect("insert entity");
            }
        }
        {
            let mut table = access_write
                .transaction()
                .expect("transaction")
                .open_table(SECONDARY_INDEXES)
                .expect("indexes");
            for index in &indexes {
                let encoded = encode_index_entry_v2(index).expect("encode index");
                table
                    .insert(index.key().as_bytes(), encoded.as_bytes())
                    .expect("insert index");
            }
        }
        access_write.commit().expect("commit");

        let parameters = QueryParameters::checked(BTreeMap::from([
            ("organization_id".to_owned(), organization.clone()),
            ("project_id".to_owned(), project.clone()),
        ]))
        .expect("parameters");
        let _serial = QUERY_TABLE_OPEN_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        reset_query_table_open_counts();
        let snapshot = ports.execute_query(&program, &parameters).expect("query");
        let opens = query_table_open_counts();
        assert_eq!(
            opens,
            QueryTableOpenCounts {
                commits: 1,
                entities: 1,
                indexes: 1,
                epochs: 1,
            },
            "index-step query opens commits, entities, indexes, and epochs exactly once each"
        );
        assert!(matches!(
            snapshot.fields().get("members"),
            Some(QueryResultValue::Many(rows)) if rows.len() == 1
        ));
        let first_user = match snapshot.fields().get("members") {
            Some(QueryResultValue::Many(rows)) => rows[0].field("user_id").cloned(),
            _ => None,
        };
        assert_ne!(
            first_user,
            Some(CanonicalValue::Uuid([2; 16])),
            "a foreign-partition index row must not consume the page limit"
        );
        let cursor = QueryContinuation::checked(
            snapshot
                .continuation_binding()
                .expect("continuation binding")
                .to_owned(),
            snapshot.continuation().expect("continuation").to_vec(),
            snapshot.index_epochs().clone(),
        )
        .expect("cursor");
        let second = ports
            .execute_query_page(&program, &parameters, Some(&cursor))
            .expect("second page");
        assert!(matches!(
            second.fields().get("members"),
            Some(QueryResultValue::Many(rows))
                if rows.len() == 1 && rows[0].field("user_id").cloned() != first_user
        ));

        let range_program = compile_query(
            &parse_query(MEMBERS_RANGE_QUERY).expect("parse range"),
            &catalog,
        )
        .expect("range program");
        let range_parameters = QueryParameters::checked(BTreeMap::from([
            ("organization_id".to_owned(), organization),
            ("project_id".to_owned(), project),
            ("lower".to_owned(), CanonicalValue::Uuid([3; 16])),
            ("upper".to_owned(), CanonicalValue::Uuid([5; 16])),
        ]))
        .expect("range parameters");
        let range_first = ports
            .execute_query_page(&range_program, &range_parameters, None)
            .expect("first range page");
        assert!(matches!(
            range_first.fields().get("members"),
            Some(QueryResultValue::Many(rows))
                if rows.len() == 1
                    && rows[0].field("user_id") == Some(&CanonicalValue::Uuid([3; 16]))
        ));
        let range_cursor = QueryContinuation::checked(
            range_first
                .continuation_binding()
                .expect("range continuation binding")
                .to_owned(),
            range_first
                .continuation()
                .expect("range continuation")
                .to_vec(),
            range_first.index_epochs().clone(),
        )
        .expect("range cursor");
        let range_second = ports
            .execute_query_page(&range_program, &range_parameters, Some(&range_cursor))
            .expect("second range page");
        assert!(matches!(
            range_second.fields().get("members"),
            Some(QueryResultValue::Many(rows))
                if rows.len() == 1
                    && rows[0].field("user_id") == Some(&CanonicalValue::Uuid([4; 16]))
        ));
        assert!(range_second.continuation().is_none());
    }

    #[test]
    fn covered_index_page_never_opens_or_reads_the_entity_table() {
        let bundle = compile_contract_source(CONTRACT).expect("covered contract");
        let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
        let program =
            compile_query(&parse_query(BOARD_QUERY).expect("parse"), &catalog).expect("program");
        let step = &program.steps()[0];
        assert!(step.covered_result_layout().is_some());

        let organization = CanonicalValue::Uuid([1; 16]);
        let project = CanonicalValue::Uuid([2; 16]);
        let ticket = CanonicalValue::Uuid([3; 16]);
        let reporter = CanonicalValue::Uuid([4; 16]);
        let assignee = CanonicalValue::Uuid([5; 16]);
        let status_schema = bundle
            .schema()
            .enums()
            .iter()
            .find(|enumeration| enumeration.name() == "TicketStatus")
            .expect("status enum");
        let status = CanonicalValue::Enum {
            type_id: status_schema.id(),
            variant_id: status_schema
                .variants()
                .iter()
                .find(|variant| variant.name() == "Open")
                .expect("Open")
                .id(),
        };
        let entity_key = step
            .internal_entity_key_schema()
            .encode_entity(&[organization.clone(), ticket.clone()])
            .expect("entity key");
        let index_key = step
            .internal_index_key_schema()
            .expect("index schema")
            .encode_index(
                &[
                    organization.clone(),
                    project.clone(),
                    status.clone(),
                    ticket,
                ],
                entity_key,
            )
            .expect("index key");
        let access = program
            .internal_entity_access("Ticket")
            .expect("Ticket access");
        let covered_values = CanonicalRecord::new(vec![
            (
                access.internal_field_id("title").expect("title"),
                CanonicalValue::string("covered ticket").expect("title value"),
            ),
            (
                access.internal_field_id("reporter_id").expect("reporter"),
                reporter,
            ),
            (
                access.internal_field_id("assignee_id").expect("assignee"),
                assignee,
            ),
        ])
        .expect("covered values");
        let binding = DurableKeySchemaBindingV1::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
        );
        let partition = step
            .internal_partition_key_schema()
            .encode_partition(std::slice::from_ref(&organization))
            .expect("partition");
        let index = StoredIndexEntryV2::new(
            index_key.clone(),
            binding.clone(),
            covered_values,
            partition.clone(),
        )
        .expect("covered index");

        let path = TestPath::new();
        let mut store = RedbStore::open(&path.0).expect("store");
        let database_id =
            DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x13; 10])
                .expect("database ID");
        store.initialize_database(database_id).expect("initialize");
        let ports = RedbOperationalPorts {
            shared: Arc::clone(&store.shared),
        };
        let access_write = ports.begin_write().expect("write");
        {
            let mut table = access_write
                .transaction()
                .expect("transaction")
                .open_table(SECONDARY_INDEXES)
                .expect("indexes");
            let encoded = encode_index_entry_v2(&index).expect("encode index");
            table
                .insert(index.key().as_bytes(), encoded.as_bytes())
                .expect("insert index");
        }
        access_write.commit().expect("commit");

        let parameters = QueryParameters::checked(BTreeMap::from([
            ("organization_id".to_owned(), organization),
            ("project_id".to_owned(), project),
            ("status".to_owned(), status),
        ]))
        .expect("parameters");
        let _serial = QUERY_TABLE_OPEN_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        reset_query_table_open_counts();
        let snapshot = ports.execute_query(&program, &parameters).expect("query");
        let covered = snapshot
            .covered_result()
            .expect("positional result retained");
        assert_eq!(covered.result_name(), "tickets");
        assert_eq!(covered.entity(), "Ticket");
        assert_eq!(covered.rows().len(), 1);
        let title_position = covered
            .fields()
            .position(|field| field == "title")
            .expect("title position");
        assert_eq!(
            covered.rows()[0][title_position],
            CanonicalValue::string("covered ticket").expect("title")
        );
        assert_eq!(
            query_table_open_counts(),
            QueryTableOpenCounts {
                commits: 1,
                entities: 0,
                indexes: 1,
                epochs: 1,
            },
            "sealed cover execution must not open the entity table"
        );

        let missing_cover = StoredIndexEntryV2::new(
            index_key,
            binding,
            CanonicalRecord::new(vec![
                (
                    access.internal_field_id("title").expect("title"),
                    CanonicalValue::string("covered ticket").expect("title value"),
                ),
                (
                    access.internal_field_id("reporter_id").expect("reporter"),
                    CanonicalValue::Uuid([4; 16]),
                ),
            ])
            .expect("missing cover record"),
            partition,
        )
        .expect("structurally encodable but semantically incomplete cover");
        let access_write = ports.begin_write().expect("write corrupt cover");
        {
            let mut table = access_write
                .transaction()
                .expect("transaction")
                .open_table(SECONDARY_INDEXES)
                .expect("indexes");
            let encoded = encode_index_entry_v2(&missing_cover).expect("encode missing cover");
            table
                .insert(missing_cover.key().as_bytes(), encoded.as_bytes())
                .expect("replace index");
        }
        access_write.commit().expect("commit corrupt cover");
        reset_query_table_open_counts();
        assert_eq!(
            ports.execute_query(&program, &parameters),
            Err(QueryExecutionError::BackendIntegrity),
            "a marked plan must fail closed rather than hydrate on incomplete coverage"
        );
        assert_eq!(query_table_open_counts().entities, 0);
    }

    #[test]
    fn exact_end_page_mints_no_continuation_when_page_fills_the_range() {
        const EXACT_END_QUERY: &str = r#"
query list_members_exact(
    $organization_id: Organization.organization_id,
    $project_id: Project.project_id,
) {
    many memberships from ProjectMember
        where organization_id == $organization_id && project_id == $project_id
        order by user_id asc
        take 2
    return Found { members: memberships { user_id role } }
    outcomes Found
}
"#;
        let bundle = compile_contract_source(CONTRACT).expect("contract");
        let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
        let program = compile_query(&parse_query(EXACT_END_QUERY).expect("parse"), &catalog)
            .expect("program");
        let step = &program.steps()[0];
        let organization = CanonicalValue::Uuid([1; 16]);
        let project = CanonicalValue::Uuid([2; 16]);
        let binding = DurableKeySchemaBindingV1::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
        );
        let access = program
            .internal_entity_access("ProjectMember")
            .expect("access");
        let mut records = Vec::new();
        let mut indexes = Vec::new();
        for (ordinal, partition_ordinal) in [(2_u8, 9_u8), (3_u8, 1_u8), (4_u8, 1_u8)] {
            let user = CanonicalValue::Uuid([ordinal; 16]);
            let key = step
                .internal_entity_key_schema()
                .encode_entity(&[organization.clone(), project.clone(), user.clone()])
                .expect("entity key");
            let target = EntityTarget::new(step.internal_entity_id(), key.clone()).expect("target");
            let fields = CanonicalRecord::new(vec![
                (
                    access
                        .internal_field_id("organization_id")
                        .expect("organization"),
                    organization.clone(),
                ),
                (
                    access.internal_field_id("project_id").expect("project"),
                    project.clone(),
                ),
                (
                    access.internal_field_id("user_id").expect("user"),
                    user.clone(),
                ),
                (
                    access.internal_field_id("role").expect("role"),
                    CanonicalValue::string("member").expect("role"),
                ),
            ])
            .expect("fields");
            records.push(
                StoredEntityRecordV1::new(
                    target,
                    EntityVersion::first(),
                    bundle.contract_version(),
                    binding.clone(),
                    fields,
                )
                .expect("record"),
            );
            let index_key = step
                .internal_index_key_schema()
                .expect("index schema")
                .encode_index(&[organization.clone(), project.clone(), user], key)
                .expect("index key");
            let mut partition =
                PartitionKeyBuilder::new(AggregateTypeId::new(4).expect("aggregate"));
            partition
                .push_uuid(&[partition_ordinal; 16])
                .expect("partition component");
            indexes.push(
                StoredIndexEntryV2::new(
                    index_key,
                    binding.clone(),
                    CanonicalRecord::new(Vec::new()).expect("cover"),
                    partition.finish().expect("partition"),
                )
                .expect("index row"),
            );
        }

        let path = TestPath::new();
        let mut store = RedbStore::open(&path.0).expect("store");
        let database_id =
            DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x22; 10])
                .expect("database ID");
        store.initialize_database(database_id).expect("initialize");
        let ports = RedbOperationalPorts {
            shared: Arc::clone(&store.shared),
        };
        let access_write = ports.begin_write().expect("write");
        {
            let mut table = access_write
                .transaction()
                .expect("transaction")
                .open_table(ENTITIES)
                .expect("entities");
            for record in &records {
                let encoded = encode_entity_record_v1(record).expect("encode entity");
                table
                    .insert(encode_entity_key(record.target().key()), encoded.as_bytes())
                    .expect("insert entity");
            }
        }
        {
            let mut table = access_write
                .transaction()
                .expect("transaction")
                .open_table(SECONDARY_INDEXES)
                .expect("indexes");
            for index in &indexes {
                let encoded = encode_index_entry_v2(index).expect("encode index");
                table
                    .insert(index.key().as_bytes(), encoded.as_bytes())
                    .expect("insert index");
            }
        }
        access_write.commit().expect("commit");

        let parameters = QueryParameters::checked(BTreeMap::from([
            ("organization_id".to_owned(), organization),
            ("project_id".to_owned(), project),
        ]))
        .expect("parameters");
        let _serial = QUERY_TABLE_OPEN_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let snapshot = ports.execute_query(&program, &parameters).expect("query");
        assert!(matches!(
            snapshot.fields().get("members"),
            Some(QueryResultValue::Many(rows)) if rows.len() == 2
        ));
        assert!(
            snapshot.continuation().is_none() && snapshot.continuation_binding().is_none(),
            "exact-end page must not mint a continuation"
        );
    }

    /// Lazy open of a missing ENTITIES table yields the same backend integrity
    /// classification as the pre-R3 eager path.
    #[test]
    fn missing_entities_table_on_point_query_is_backend_integrity() {
        use riffdb_query_executor::QueryExecutionError;

        let bundle = compile_contract_source(CONTRACT).expect("contract");
        let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
        let program =
            compile_query(&parse_query(POINT_QUERY).expect("parse"), &catalog).expect("program");
        let organization = CanonicalValue::Uuid([1; 16]);
        let ticket = CanonicalValue::Uuid([2; 16]);

        let path = TestPath::new();
        let mut store = RedbStore::open(&path.0).expect("store");
        let database_id =
            DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x13; 10])
                .expect("database ID");
        store.initialize_database(database_id).expect("initialize");
        let ports = RedbOperationalPorts {
            shared: Arc::clone(&store.shared),
        };
        // Drop the entities table after init so first touch fails.
        {
            let access = ports.begin_write().expect("write");
            access
                .transaction()
                .expect("txn")
                .delete_table(ENTITIES)
                .expect("delete entities");
            access.commit().expect("commit");
        }
        let parameters = QueryParameters::checked(BTreeMap::from([
            ("organization_id".to_owned(), organization),
            ("ticket_id".to_owned(), ticket),
        ]))
        .expect("parameters");
        let _serial = QUERY_TABLE_OPEN_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let error = ports
            .execute_query(&program, &parameters)
            .expect_err("missing entities must fail");
        assert_eq!(
            error,
            QueryExecutionError::BackendIntegrity,
            "lazy first-touch of a missing ENTITIES table must keep the pre-R3 integrity taxonomy"
        );
    }
}
