//! Format-local bounded scratch construction for one unselected V2 generation.
//!
//! Scratch lives only below the generation-derived `.tmp` directory. It is
//! neither authoritative storage nor a readable generation member, and is
//! removed before ROOT-V1 can be written.

use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BinaryHeap};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use riffdb_storage_api::{
    ApplicationExportSourceRecordV1, AuthoritativeEntitySnapshotReader, AuthoritativePointReader,
    AuthoritativeScanReader, CommitScanPageV1, CommitScanRequest, MAX_ENTITY_MUTATIONS,
    StorageScanLimit,
};
use riffdb_types::{
    CanonicalValue, CommitSequence, EntityVersion, FrontierPosition, LogicalTime, MAX_KEY_BYTES,
    NANOS_PER_SECOND, Timestamp, decode_canonical_value, encode_canonical_value,
};

use crate::checkpoint::checksum_bytes;
use crate::store::{LiveRow, OrgKey, PrimaryKeyBytes, project_cells};
use crate::{
    ColumnarError, ColumnarSpecReplayLimitsV1, ColumnarTestBoundary, ColumnarTestController,
    RegisteredDefinition,
};

const SCRATCH_DIRECTORY: &str = ".rebuild-scratch-v1";
const OBSERVATION_MAGIC: &[u8; 8] = b"RDBV2OB\0";
const COLLAPSED_MAGIC: &[u8; 8] = b"RDBV2RW\0";
const EVENT_MAGIC: &[u8; 8] = b"RDBV2EV\0";
const MAX_RUN_ROWS: usize = 256;
const MAX_RUN_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;
const MERGE_FAN_IN: usize = 8;
const MAX_PROVIDER_STATE_BYTES_PER_ROW: usize = 16_384;
const MAX_PROJECTED_ROW_PAYLOAD: usize =
    MAX_KEY_BYTES + MAX_PROVIDER_STATE_BYTES_PER_ROW + 1 + 8 + 8 + 4 + 4;
const MAX_OBSERVATION_PAYLOAD: usize = MAX_KEY_BYTES + 8 + 1 + 4 + MAX_PROJECTED_ROW_PAYLOAD;
const MAX_SCRATCH_FRAME_PAYLOAD: usize = MAX_OBSERVATION_PAYLOAD;
const FRAME_OVERHEAD: u64 = 4 + 32;

const MAX_PARTITION_LANE_ROWS: usize =
    crate::MAX_COLUMNAR_MANIFEST_V2_SEGMENTS * crate::MAX_SEGMENT_V2_ROWS;

/// File ceiling for a fresh V2 build whose snapshot and tail reader are the
/// same immutable pin at `snapshot_frontier` (hence an empty retained suffix).
/// Includes all partition lanes, manifests, segments, ROOT-V1, two empty tail
/// runs and one in-progress immutable member. This is not a retained-tail bound.
pub const V2_SNAPSHOT_BUILD_MAX_FILES: usize = crate::MAX_COLUMNAR_GENERATION_ROOT_V1_PARTITIONS
    * (crate::MAX_COLUMNAR_MANIFEST_V2_SEGMENTS + 2)
    + 4;

/// Conservative live-byte ceiling for the fresh snapshot-only build described
/// by [`V2_SNAPSHOT_BUILD_MAX_FILES`], including interrupted partial writes.
/// The lane row ceiling is enforced before append, even for invalid input.
pub const V2_SNAPSHOT_BUILD_MAX_BYTES: u64 = crate::MAX_COLUMNAR_GENERATION_ROOT_V1_PARTITIONS
    as u64
    * (8 + MAX_PARTITION_LANE_ROWS as u64 * (MAX_PROJECTED_ROW_PAYLOAD as u64 + FRAME_OVERHEAD)
        + crate::MAX_COLUMNAR_MANIFEST_V2_BYTES as u64
        + crate::MAX_COLUMNAR_MANIFEST_V2_SEGMENTS as u64 * crate::MAX_SEGMENT_V2_BYTES as u64)
    + crate::MAX_COLUMNAR_GENERATION_ROOT_V1_BYTES as u64
    + crate::MAX_SEGMENT_V2_BYTES as u64
    + 16;

// Count final merged rows, not pre-tail snapshot rows: a retained deletion may
// shrink a snapshot that would otherwise exceed the final manifest capacity.
struct PartitionLane {
    path: PathBuf,
    remaining: usize,
}

impl PartitionLane {
    fn append(&mut self, row: &ProjectedRow) -> Result<(), ColumnarV2StreamingError> {
        let remaining = self
            .remaining
            .checked_sub(1)
            .ok_or(ColumnarV2StreamingError::BoundExceeded)?;
        append_frame(&self.path, &encode_projected_row(row)?)?;
        self.remaining = remaining;
        Ok(())
    }
}

/// Closed failure from bounded format-local rebuild scratch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColumnarV2StreamingError {
    /// The worker's monotonic stop token was observed at a page boundary.
    Cancelled,
    /// Exact retained-tail age exceeded the compiler-sealed limit.
    ReplayAge,
    /// Exact encoded retained-tail bytes exceeded the compiler-sealed limit.
    ReplayBytes,
    /// Exact retained sequence distance exceeded the compiler-sealed limit.
    ReplayBacklog,
    /// The server-owned UTC sample failed after the frozen tail scan.
    Clock,
    /// A fixed format or implementation bound was exceeded.
    BoundExceeded,
    /// Authoritative evidence or scratch bytes were inconsistent.
    Invalid,
    /// A storage or filesystem operation failed.
    Io,
}

impl fmt::Display for ColumnarV2StreamingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Cancelled => "columnar V2 streaming rebuild cancelled",
            Self::ReplayAge => "columnar V2 replay-age ceiling exceeded",
            Self::ReplayBytes => "columnar V2 replay-byte ceiling exceeded",
            Self::ReplayBacklog => "columnar V2 replay-backlog ceiling exceeded",
            Self::Clock => "columnar V2 replay-age UTC sample failed",
            Self::BoundExceeded => "columnar V2 streaming rebuild bound exceeded",
            Self::Invalid => "columnar V2 streaming rebuild evidence is invalid",
            Self::Io => "columnar V2 streaming rebuild I/O failed",
        })
    }
}

impl std::error::Error for ColumnarV2StreamingError {}

/// Test-private-style boundedness observations returned only to the internal
/// generation builder. They never cross a query, metric, log, or protocol.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ColumnarV2StreamingStats {
    peak_buffered_rows: usize,
    peak_buffered_bytes: usize,
    peak_open_runs: usize,
    peak_live_row_bytes: usize,
    tail_observations: u64,
}

impl ColumnarV2StreamingStats {
    #[cfg(test)]
    const fn peak_buffered_rows(self) -> usize {
        self.peak_buffered_rows
    }

    #[cfg(test)]
    const fn peak_buffered_bytes(self) -> usize {
        self.peak_buffered_bytes
    }

    #[cfg(test)]
    const fn peak_open_runs(self) -> usize {
        self.peak_open_runs
    }

    #[cfg(test)]
    const fn peak_live_row_bytes(self) -> usize {
        self.peak_live_row_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProjectedRow {
    key: PrimaryKeyBytes,
    version: EntityVersion,
    organization: OrgKey,
    cells: Vec<CanonicalValue>,
}

impl ProjectedRow {
    fn from_record(
        definition: &RegisteredDefinition,
        record: &riffdb_storage_api::StoredEntityRecordV1,
    ) -> Result<Self, ColumnarV2StreamingError> {
        if record.target().entity_type_id() != definition.entity_type_id() {
            return Err(ColumnarV2StreamingError::Invalid);
        }
        let (organization, cells) = project_cells(
            record.fields().fields(),
            definition.projected_fields(),
            definition.org_scope_field(),
        )
        .map_err(map_columnar_error)?;
        let organization = OrgKey::from_value(&organization).map_err(map_columnar_error)?;
        let row = Self {
            key: PrimaryKeyBytes::from_entity_key_bytes(record.target().key().as_bytes().to_vec()),
            version: record.entity_version(),
            organization,
            cells,
        };
        if encode_projected_row(&row)?.len() > MAX_PROJECTED_ROW_PAYLOAD {
            return Err(ColumnarV2StreamingError::BoundExceeded);
        }
        Ok(row)
    }

    pub(crate) fn into_parts(self) -> (OrgKey, PrimaryKeyBytes, LiveRow) {
        (
            self.organization,
            self.key,
            LiveRow {
                entity_version: self.version,
                cells: self.cells,
            },
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TailObservationKind {
    Matched(ProjectedRow),
    ForwardRace(EntityVersion),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TailObservation {
    key: PrimaryKeyBytes,
    sequence: CommitSequence,
    kind: TailObservationKind,
}

impl Ord for TailObservation {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.sequence.cmp(&other.sequence))
    }
}

impl PartialOrd for TailObservation {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DeferralEvent {
    sequence: CommitSequence,
    delta: i8,
}

impl Ord for DeferralEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        self.sequence
            .cmp(&other.sequence)
            .then_with(|| self.delta.cmp(&other.delta))
    }
}

impl PartialOrd for DeferralEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Default)]
struct ScratchBudget {
    ceiling: u64,
    live: u64,
    peak: u64,
}

impl ScratchBudget {
    fn new(limits: ColumnarSpecReplayLimitsV1) -> Result<Self, ColumnarV2StreamingError> {
        let observations = limits
            .backlog()
            .checked_mul(MAX_ENTITY_MUTATIONS as u64)
            .ok_or(ColumnarV2StreamingError::BoundExceeded)?;
        let expanded = observations
            .checked_mul(
                (MAX_OBSERVATION_PAYLOAD as u64)
                    .checked_add(FRAME_OVERHEAD)
                    .ok_or(ColumnarV2StreamingError::BoundExceeded)?,
            )
            .ok_or(ColumnarV2StreamingError::BoundExceeded)?;
        // At fixed-fan-in merge boundaries inputs and their replacement runs
        // coexist. Deferral event runs and the collapsed latest-row run can
        // coexist with the canonical observation runs. Four expansion shares
        // plus the accepted exact commit-byte ceiling cover that live set.
        let ceiling = expanded
            .checked_mul(4)
            .and_then(|bytes| bytes.checked_add(limits.bytes()))
            .ok_or(ColumnarV2StreamingError::BoundExceeded)?;
        Ok(Self {
            ceiling,
            ..Self::default()
        })
    }

    fn add(&mut self, bytes: u64) -> Result<(), ColumnarV2StreamingError> {
        let next = self
            .live
            .checked_add(bytes)
            .ok_or(ColumnarV2StreamingError::BoundExceeded)?;
        if next > self.ceiling {
            return Err(ColumnarV2StreamingError::BoundExceeded);
        }
        self.live = next;
        self.peak = self.peak.max(self.live);
        Ok(())
    }

    fn ensure_add(&self, bytes: u64) -> Result<(), ColumnarV2StreamingError> {
        if self
            .live
            .checked_add(bytes)
            .is_none_or(|next| next > self.ceiling)
        {
            Err(ColumnarV2StreamingError::BoundExceeded)
        } else {
            Ok(())
        }
    }

    fn remove(&mut self, bytes: u64) -> Result<(), ColumnarV2StreamingError> {
        self.live = self
            .live
            .checked_sub(bytes)
            .ok_or(ColumnarV2StreamingError::Invalid)?;
        Ok(())
    }
}

struct ScratchDirectory {
    path: PathBuf,
    armed: bool,
}

impl ScratchDirectory {
    fn create(generation_tmp: &Path) -> Result<Self, ColumnarV2StreamingError> {
        let path = generation_tmp.join(SCRATCH_DIRECTORY);
        if path.exists() {
            fs::remove_dir_all(&path).map_err(|_| ColumnarV2StreamingError::Io)?;
        }
        fs::create_dir(&path).map_err(|_| ColumnarV2StreamingError::Io)?;
        Ok(Self { path, armed: true })
    }

    fn cleanup(&mut self) -> Result<(), ColumnarV2StreamingError> {
        if self.armed {
            fs::remove_dir_all(&self.path).map_err(|_| ColumnarV2StreamingError::Io)?;
            self.armed = false;
        }
        Ok(())
    }
}

impl Drop for ScratchDirectory {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

/// One collapsed latest-by-key tail plus its exact safe frontier.
pub(crate) struct ColumnarV2StreamingRows {
    scratch: ScratchDirectory,
    collapsed_path: PathBuf,
    frontier: FrontierPosition,
}

/// Refuses an already excessive snapshot inventory before the generation
/// directory or any scratch member is created. Tail moves/additions are
/// checked again against the exact merged final stream.
pub(crate) fn preflight_snapshot_partition_bound<S>(
    snapshot: &S,
    definition: &RegisteredDefinition,
    maximum: usize,
    mut stop_before_next_page: impl FnMut() -> bool,
) -> Result<(), ColumnarV2StreamingError>
where
    S: AuthoritativeEntitySnapshotReader + ?Sized,
{
    let limit = StorageScanLimit::new(500).ok_or(ColumnarV2StreamingError::Invalid)?;
    let mut continuation: Option<Box<[u8]>> = None;
    let mut organizations = BTreeMap::<OrgKey, ()>::new();
    let mut previous_key: Option<PrimaryKeyBytes> = None;
    loop {
        let page = snapshot
            .read_entity_type_page(definition.entity_type_id(), continuation.as_deref(), limit)
            .map_err(|_| ColumnarV2StreamingError::Io)?;
        for source in page.records() {
            let ApplicationExportSourceRecordV1::Entity(record) = source else {
                return Err(ColumnarV2StreamingError::Invalid);
            };
            let row = ProjectedRow::from_record(definition, record)?;
            if previous_key.as_ref().is_some_and(|key| key >= &row.key) {
                return Err(ColumnarV2StreamingError::Invalid);
            }
            previous_key = Some(row.key.clone());
            organizations.insert(row.organization, ());
            if organizations.len() > maximum {
                return Err(ColumnarV2StreamingError::BoundExceeded);
            }
        }
        if page.exact_end() {
            return Ok(());
        }
        if stop_before_next_page() {
            return Err(ColumnarV2StreamingError::Cancelled);
        }
        continuation = page.continuation().map(Into::into);
    }
}

impl ColumnarV2StreamingRows {
    /// Scans one frozen retained tail and builds bounded external merge runs.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build<R>(
        generation_tmp: &Path,
        definition: &RegisteredDefinition,
        snapshot_frontier: FrontierPosition,
        reader: &R,
        limits: ColumnarSpecReplayLimitsV1,
        sample_utc_after_tail_scan: impl FnOnce() -> Result<Timestamp, ColumnarV2StreamingError>,
        mut stop_before_next_page: impl FnMut() -> bool,
        controller: Option<&ColumnarTestController>,
    ) -> Result<Self, ColumnarV2StreamingError>
    where
        R: AuthoritativePointReader + AuthoritativeScanReader + ?Sized,
    {
        let scratch = ScratchDirectory::create(generation_tmp)?;
        let mut budget = ScratchBudget::new(limits)?;
        let mut stats = ColumnarV2StreamingStats::default();
        let observation_runs = scan_observation_runs(
            &scratch.path,
            definition,
            snapshot_frontier,
            reader,
            limits,
            sample_utc_after_tail_scan,
            &mut stop_before_next_page,
            &mut budget,
            &mut stats,
            controller,
        )?;
        let frontier = observation_runs.frontier;
        let canonical_observations = merge_observation_runs(
            &scratch.path,
            observation_runs.paths,
            &mut budget,
            &mut stats,
            controller,
        )?;
        let event_runs = build_deferral_event_runs(
            &scratch.path,
            &canonical_observations,
            &mut budget,
            &mut stats,
        )?;
        let safe_frontier = resolve_safe_frontier(
            snapshot_frontier,
            frontier,
            &scratch.path,
            event_runs,
            &mut budget,
            &mut stats,
        )?;
        let collapsed_path = collapse_latest_rows(
            &scratch.path,
            &canonical_observations,
            safe_frontier,
            &mut budget,
        )?;
        remove_budgeted(&canonical_observations, &mut budget)?;
        Ok(Self {
            scratch,
            collapsed_path,
            frontier: safe_frontier,
        })
    }

    /// Matched frontier represented by the collapsed stream.
    #[must_use]
    pub(crate) const fn frontier(&self) -> FrontierPosition {
        self.frontier
    }

    pub(crate) fn reader(&self) -> Result<ProjectedRowReader, ColumnarV2StreamingError> {
        ProjectedRowReader::open(&self.collapsed_path, COLLAPSED_MAGIC)
    }

    /// Rewinds the pinned authoritative snapshot and merges it with the one
    /// key-collapsed safe tail. Only one source page, one tail frame, and one
    /// projected row are live at a time.
    pub(crate) fn visit_final_rows<S>(
        &self,
        snapshot: &S,
        definition: &RegisteredDefinition,
        mut stop_before_next_page: impl FnMut() -> bool,
        mut visit: impl FnMut(ProjectedRow) -> Result<(), ColumnarV2StreamingError>,
    ) -> Result<(), ColumnarV2StreamingError>
    where
        S: AuthoritativeEntitySnapshotReader + ?Sized,
    {
        let mut tail = self.reader()?;
        let mut next_tail = tail.next()?;
        let limit = StorageScanLimit::new(500).ok_or(ColumnarV2StreamingError::Invalid)?;
        let mut continuation: Option<Box<[u8]>> = None;
        let mut previous_snapshot_key: Option<PrimaryKeyBytes> = None;
        loop {
            let page = snapshot
                .read_entity_type_page(definition.entity_type_id(), continuation.as_deref(), limit)
                .map_err(|_| ColumnarV2StreamingError::Io)?;
            for source in page.records() {
                let ApplicationExportSourceRecordV1::Entity(record) = source else {
                    return Err(ColumnarV2StreamingError::Invalid);
                };
                let snapshot_row = ProjectedRow::from_record(definition, record)?;
                if previous_snapshot_key
                    .as_ref()
                    .is_some_and(|key| key >= &snapshot_row.key)
                {
                    return Err(ColumnarV2StreamingError::Invalid);
                }
                previous_snapshot_key = Some(snapshot_row.key.clone());
                while next_tail
                    .as_ref()
                    .is_some_and(|row| row.key < snapshot_row.key)
                {
                    visit(next_tail.take().ok_or(ColumnarV2StreamingError::Invalid)?)?;
                    next_tail = tail.next()?;
                }
                if next_tail
                    .as_ref()
                    .is_some_and(|row| row.key == snapshot_row.key)
                {
                    visit(next_tail.take().ok_or(ColumnarV2StreamingError::Invalid)?)?;
                    next_tail = tail.next()?;
                } else {
                    visit(snapshot_row)?;
                }
            }
            if page.exact_end() {
                break;
            }
            if stop_before_next_page() {
                return Err(ColumnarV2StreamingError::Cancelled);
            }
            continuation = page.continuation().map(Into::into);
        }
        while let Some(row) = next_tail {
            visit(row)?;
            next_tail = tail.next()?;
        }
        Ok(())
    }

    /// Writes one canonical key-ordered lane per preflighted organization.
    /// Opening one append handle per row is deliberate: the handle ceiling is
    /// one, independent of the at-most-4,096 root inventory.
    pub(crate) fn write_partition_lanes<S>(
        &self,
        snapshot: &S,
        definition: &RegisteredDefinition,
        organizations: &[OrgKey],
        stop_before_next_page: impl FnMut() -> bool,
    ) -> Result<Vec<(OrgKey, PathBuf)>, ColumnarV2StreamingError>
    where
        S: AuthoritativeEntitySnapshotReader + ?Sized,
    {
        let mut lanes = Vec::with_capacity(organizations.len());
        let mut identities = BTreeMap::new();
        for (ordinal, organization) in organizations.iter().enumerate() {
            let path = self
                .scratch
                .path
                .join(format!("partition-{ordinal:04x}.lane"));
            let bytes = write_records(
                &path,
                COLLAPSED_MAGIC,
                std::iter::empty::<Result<Vec<u8>, ColumnarV2StreamingError>>(),
            )?;
            if bytes != 8 {
                return Err(ColumnarV2StreamingError::Invalid);
            }
            identities.insert(
                organization.clone(),
                PartitionLane {
                    path: path.clone(),
                    remaining: MAX_PARTITION_LANE_ROWS,
                },
            );
            lanes.push((organization.clone(), path));
        }
        self.visit_final_rows(snapshot, definition, stop_before_next_page, |row| {
            let lane = identities
                .get_mut(&row.organization)
                .ok_or(ColumnarV2StreamingError::Invalid)?;
            lane.append(&row)
        })?;
        Ok(lanes)
    }

    /// Removes all run and collapsed-tail scratch. Callers must do this before
    /// writing ROOT-V1 or renaming the generation directory.
    pub(crate) fn cleanup(mut self) -> Result<(), ColumnarV2StreamingError> {
        self.scratch.cleanup()
    }
}

struct ObservationRuns {
    paths: Vec<PathBuf>,
    frontier: FrontierPosition,
}

#[allow(clippy::too_many_arguments)]
fn scan_observation_runs<R>(
    scratch: &Path,
    definition: &RegisteredDefinition,
    snapshot_frontier: FrontierPosition,
    reader: &R,
    limits: ColumnarSpecReplayLimitsV1,
    sample_utc_after_tail_scan: impl FnOnce() -> Result<Timestamp, ColumnarV2StreamingError>,
    stop_before_next_page: &mut impl FnMut() -> bool,
    budget: &mut ScratchBudget,
    stats: &mut ColumnarV2StreamingStats,
    controller: Option<&ColumnarTestController>,
) -> Result<ObservationRuns, ColumnarV2StreamingError>
where
    R: AuthoritativePointReader + AuthoritativeScanReader + ?Sized,
{
    let limit = StorageScanLimit::new(64).ok_or(ColumnarV2StreamingError::Invalid)?;
    let mut request = match snapshot_frontier {
        FrontierPosition::BeforeFirst => CommitScanRequest::initial(limit),
        FrontierPosition::AppliedThrough(sequence) => {
            CommitScanRequest::initial_after(sequence, limit)
        }
    };
    let mut buffer = Vec::new();
    let mut buffer_bytes = 0usize;
    let mut paths = Vec::new();
    let mut replay_bytes = 0u64;
    let mut replay_bytes_exceeded = false;
    let mut replay_backlog_exceeded = false;
    let mut oldest_logical_time = None;
    let mut expected = snapshot_frontier;
    let frontier;
    loop {
        let page = reader
            .scan_commits(request)
            .map_err(|_| ColumnarV2StreamingError::Io)?;
        let upper = page.inclusive_upper();
        let backlog = frontier_distance(snapshot_frontier, upper)?;
        replay_backlog_exceeded |= backlog > limits.backlog();
        for charged in page.records() {
            replay_bytes = replay_bytes
                .checked_add(charged.encoded_content_charge().get() as u64)
                .ok_or(ColumnarV2StreamingError::BoundExceeded)?;
            replay_bytes_exceeded |= replay_bytes > limits.bytes();
            let commit = charged.value();
            if !exact_successor(expected, commit.commit_sequence()) {
                return Err(ColumnarV2StreamingError::Invalid);
            }
            expected = FrontierPosition::AppliedThrough(commit.commit_sequence());
            oldest_logical_time = Some(
                oldest_logical_time.map_or(commit.logical_time(), |oldest: LogicalTime| {
                    oldest.min(commit.logical_time())
                }),
            );
            if replay_bytes_exceeded || replay_backlog_exceeded {
                continue;
            }
            for reference in commit.entity_references() {
                if reference.target().entity_type_id() != definition.entity_type_id() {
                    continue;
                }
                let record = reader
                    .read_entity(reference.target())
                    .map_err(|_| ColumnarV2StreamingError::Io)?
                    .ok_or(ColumnarV2StreamingError::Invalid)?;
                let kind = if reference.matches(&record) {
                    TailObservationKind::Matched(ProjectedRow::from_record(definition, &record)?)
                } else if record.entity_version().get() > reference.entity_version().get() {
                    TailObservationKind::ForwardRace(record.entity_version())
                } else {
                    return Err(ColumnarV2StreamingError::Invalid);
                };
                let observation = TailObservation {
                    key: PrimaryKeyBytes::from_entity_key_bytes(
                        reference.target().key().as_bytes().to_vec(),
                    ),
                    sequence: commit.commit_sequence(),
                    kind,
                };
                let encoded = encode_observation(&observation)?;
                if !buffer.is_empty()
                    && (buffer.len() == MAX_RUN_ROWS
                        || buffer_bytes
                            .checked_add(encoded.len())
                            .is_none_or(|bytes| bytes > MAX_RUN_PAYLOAD_BYTES))
                {
                    paths.push(write_observation_run(
                        scratch,
                        paths.len(),
                        &mut buffer,
                        budget,
                    )?);
                    if hit(controller, ColumnarTestBoundary::AfterV2ScratchRun) {
                        return Err(ColumnarV2StreamingError::Io);
                    }
                    buffer_bytes = 0;
                }
                buffer_bytes = buffer_bytes
                    .checked_add(encoded.len())
                    .ok_or(ColumnarV2StreamingError::BoundExceeded)?;
                buffer.push(observation);
                stats.tail_observations = stats
                    .tail_observations
                    .checked_add(1)
                    .ok_or(ColumnarV2StreamingError::BoundExceeded)?;
                stats.peak_buffered_rows = stats.peak_buffered_rows.max(buffer.len());
                stats.peak_buffered_bytes = stats.peak_buffered_bytes.max(buffer_bytes);
                stats.peak_live_row_bytes = stats.peak_live_row_bytes.max(buffer_bytes);
            }
        }
        match page {
            CommitScanPageV1::Page {
                next_after,
                inclusive_upper,
                ..
            } => {
                if stop_before_next_page() {
                    return Err(ColumnarV2StreamingError::Cancelled);
                }
                let FrontierPosition::AppliedThrough(upper) = inclusive_upper else {
                    return Err(ColumnarV2StreamingError::Invalid);
                };
                request = CommitScanRequest::continuing(next_after, upper, limit)
                    .map_err(|_| ColumnarV2StreamingError::Invalid)?;
            }
            CommitScanPageV1::ExactEnd {
                inclusive_upper, ..
            } => {
                if expected != inclusive_upper {
                    return Err(ColumnarV2StreamingError::Invalid);
                }
                frontier = inclusive_upper;
                break;
            }
        }
    }
    let sampled_utc = sample_utc_after_tail_scan()?;
    let replay_age_exceeded =
        replay_age_exceeded(sampled_utc, oldest_logical_time, limits.age_seconds());
    if let Some(error) = replay_limit_failure(
        replay_age_exceeded,
        replay_bytes_exceeded,
        replay_backlog_exceeded,
    ) {
        return Err(error);
    }
    if !buffer.is_empty() {
        paths.push(write_observation_run(
            scratch,
            paths.len(),
            &mut buffer,
            budget,
        )?);
        if hit(controller, ColumnarTestBoundary::AfterV2ScratchRun) {
            return Err(ColumnarV2StreamingError::Io);
        }
    }
    Ok(ObservationRuns { paths, frontier })
}

fn replay_age_exceeded(
    sampled_utc: Timestamp,
    oldest_retained: Option<LogicalTime>,
    ceiling_seconds: u64,
) -> bool {
    let Some(oldest_retained) = oldest_retained else {
        return false;
    };
    let sampled_nanos = i128::from(sampled_utc.seconds()) * i128::from(NANOS_PER_SECOND)
        + i128::from(sampled_utc.nanoseconds());
    let oldest = oldest_retained.timestamp();
    let oldest_nanos = i128::from(oldest.seconds()) * i128::from(NANOS_PER_SECOND)
        + i128::from(oldest.nanoseconds());
    let age_nanos = sampled_nanos.saturating_sub(oldest_nanos).max(0);
    age_nanos > i128::from(ceiling_seconds) * i128::from(NANOS_PER_SECOND)
}

const fn replay_limit_failure(
    age: bool,
    bytes: bool,
    backlog: bool,
) -> Option<ColumnarV2StreamingError> {
    if age {
        Some(ColumnarV2StreamingError::ReplayAge)
    } else if bytes {
        Some(ColumnarV2StreamingError::ReplayBytes)
    } else if backlog {
        Some(ColumnarV2StreamingError::ReplayBacklog)
    } else {
        None
    }
}

fn frontier_distance(
    lower: FrontierPosition,
    upper: FrontierPosition,
) -> Result<u64, ColumnarV2StreamingError> {
    match (lower, upper) {
        (FrontierPosition::BeforeFirst, FrontierPosition::BeforeFirst) => Ok(0),
        (FrontierPosition::BeforeFirst, FrontierPosition::AppliedThrough(value)) => Ok(value.get()),
        (FrontierPosition::AppliedThrough(_), FrontierPosition::BeforeFirst) => {
            Err(ColumnarV2StreamingError::Invalid)
        }
        (FrontierPosition::AppliedThrough(lower), FrontierPosition::AppliedThrough(upper)) => upper
            .get()
            .checked_sub(lower.get())
            .ok_or(ColumnarV2StreamingError::Invalid),
    }
}

fn exact_successor(frontier: FrontierPosition, sequence: CommitSequence) -> bool {
    match frontier {
        FrontierPosition::BeforeFirst => sequence.get() == 1,
        FrontierPosition::AppliedThrough(previous) => {
            previous.checked_next().is_some_and(|next| next == sequence)
        }
    }
}

fn write_observation_run(
    scratch: &Path,
    ordinal: usize,
    records: &mut Vec<TailObservation>,
    budget: &mut ScratchBudget,
) -> Result<PathBuf, ColumnarV2StreamingError> {
    records.sort();
    if records
        .windows(2)
        .any(|pair| pair[0].key == pair[1].key && pair[0].sequence == pair[1].sequence)
    {
        return Err(ColumnarV2StreamingError::Invalid);
    }
    let path = scratch.join(format!("observation-{ordinal:08x}.run"));
    let encoded = records
        .iter()
        .map(encode_observation)
        .collect::<Result<Vec<_>, _>>()?;
    let maximum = framed_file_bytes(&encoded)?;
    budget.ensure_add(maximum)?;
    let bytes = write_records(&path, OBSERVATION_MAGIC, encoded.into_iter().map(Ok))?;
    budget.add(bytes)?;
    records.clear();
    Ok(path)
}

fn merge_observation_runs(
    scratch: &Path,
    mut paths: Vec<PathBuf>,
    budget: &mut ScratchBudget,
    stats: &mut ColumnarV2StreamingStats,
    controller: Option<&ColumnarTestController>,
) -> Result<PathBuf, ColumnarV2StreamingError> {
    if paths.is_empty() {
        let path = scratch.join("observations-empty.run");
        budget.ensure_add(8)?;
        let bytes = write_records(&path, OBSERVATION_MAGIC, std::iter::empty())?;
        budget.add(bytes)?;
        return Ok(path);
    }
    let mut pass = 0usize;
    while paths.len() > 1 {
        let mut next = Vec::new();
        for (group, chunk) in paths.chunks(MERGE_FAN_IN).enumerate() {
            stats.peak_open_runs = stats.peak_open_runs.max(chunk.len());
            stats.peak_live_row_bytes = stats
                .peak_live_row_bytes
                .max(chunk.len().saturating_mul(MAX_OBSERVATION_PAYLOAD));
            let path = scratch.join(format!("observation-merge-{pass:04x}-{group:08x}.run"));
            budget.ensure_add(sum_file_bytes(chunk)?)?;
            let bytes = merge_observation_group(chunk, &path)?;
            budget.add(bytes)?;
            if hit(controller, ColumnarTestBoundary::AfterV2ScratchMerge) {
                return Err(ColumnarV2StreamingError::Io);
            }
            next.push(path);
        }
        for path in paths {
            remove_budgeted(&path, budget)?;
        }
        paths = next;
        pass = pass
            .checked_add(1)
            .ok_or(ColumnarV2StreamingError::BoundExceeded)?;
    }
    let path = paths.pop().ok_or(ColumnarV2StreamingError::Invalid)?;
    validate_observation_run(&path)?;
    Ok(path)
}

fn validate_observation_run(path: &Path) -> Result<(), ColumnarV2StreamingError> {
    let mut reader = ObservationReader::open(path)?;
    let mut previous: Option<(PrimaryKeyBytes, CommitSequence)> = None;
    while let Some(observation) = reader.next()? {
        let identity = (observation.key, observation.sequence);
        if previous.as_ref().is_some_and(|value| value >= &identity) {
            return Err(ColumnarV2StreamingError::Invalid);
        }
        previous = Some(identity);
    }
    Ok(())
}

#[derive(Eq, PartialEq)]
struct ObservationHeapItem {
    observation: TailObservation,
    reader: usize,
}

impl Ord for ObservationHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.observation
            .cmp(&other.observation)
            .then_with(|| self.reader.cmp(&other.reader))
    }
}

impl PartialOrd for ObservationHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn merge_observation_group(
    inputs: &[PathBuf],
    output: &Path,
) -> Result<u64, ColumnarV2StreamingError> {
    let mut readers = inputs
        .iter()
        .map(|path| ObservationReader::open(path))
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(observation) = reader.next()? {
            heap.push(Reverse(ObservationHeapItem {
                observation,
                reader: index,
            }));
        }
    }
    let mut writer = FramedWriter::create(output, OBSERVATION_MAGIC)?;
    let mut previous: Option<(PrimaryKeyBytes, CommitSequence)> = None;
    while let Some(Reverse(item)) = heap.pop() {
        let identity = (item.observation.key.clone(), item.observation.sequence);
        if previous.as_ref().is_some_and(|value| value >= &identity) {
            return Err(ColumnarV2StreamingError::Invalid);
        }
        previous = Some(identity);
        writer.write(&encode_observation(&item.observation)?)?;
        if let Some(next) = readers[item.reader].next()? {
            heap.push(Reverse(ObservationHeapItem {
                observation: next,
                reader: item.reader,
            }));
        }
    }
    writer.finish()
}

fn build_deferral_event_runs(
    scratch: &Path,
    observations: &Path,
    budget: &mut ScratchBudget,
    stats: &mut ColumnarV2StreamingStats,
) -> Result<Vec<PathBuf>, ColumnarV2StreamingError> {
    let mut reader = ObservationReader::open(observations)?;
    let mut paths = Vec::new();
    let mut events = Vec::new();
    let mut current_key: Option<PrimaryKeyBytes> = None;
    let mut deferred: Option<EntityVersion> = None;
    while let Some(observation) = reader.next()? {
        if current_key.as_ref() != Some(&observation.key) {
            current_key = Some(observation.key.clone());
            deferred = None;
        }
        match observation.kind {
            TailObservationKind::ForwardRace(version) => {
                if deferred.is_none() {
                    push_event_run(
                        scratch,
                        DeferralEvent {
                            sequence: observation.sequence,
                            delta: 1,
                        },
                        &mut events,
                        &mut paths,
                        budget,
                        stats,
                    )?;
                }
                deferred = Some(version);
            }
            TailObservationKind::Matched(row) => {
                if deferred.is_some_and(|version| row.version >= version) {
                    deferred = None;
                    push_event_run(
                        scratch,
                        DeferralEvent {
                            sequence: observation.sequence,
                            delta: -1,
                        },
                        &mut events,
                        &mut paths,
                        budget,
                        stats,
                    )?;
                }
            }
        }
    }
    if !events.is_empty() {
        paths.push(write_event_run(scratch, paths.len(), &mut events, budget)?);
    }
    Ok(paths)
}

#[allow(clippy::too_many_arguments)]
fn push_event_run(
    scratch: &Path,
    event: DeferralEvent,
    events: &mut Vec<DeferralEvent>,
    paths: &mut Vec<PathBuf>,
    budget: &mut ScratchBudget,
    stats: &mut ColumnarV2StreamingStats,
) -> Result<(), ColumnarV2StreamingError> {
    if events.len() == MAX_RUN_ROWS {
        paths.push(write_event_run(scratch, paths.len(), events, budget)?);
    }
    events.push(event);
    stats.peak_buffered_rows = stats.peak_buffered_rows.max(events.len());
    stats.peak_buffered_bytes = stats
        .peak_buffered_bytes
        .max(events.len().saturating_mul(9));
    stats.peak_live_row_bytes = stats
        .peak_live_row_bytes
        .max(events.len().saturating_mul(9));
    Ok(())
}

fn write_event_run(
    scratch: &Path,
    ordinal: usize,
    events: &mut Vec<DeferralEvent>,
    budget: &mut ScratchBudget,
) -> Result<PathBuf, ColumnarV2StreamingError> {
    events.sort();
    let path = scratch.join(format!("deferral-{ordinal:08x}.run"));
    let encoded = events
        .iter()
        .map(encode_event)
        .collect::<Result<Vec<_>, _>>()?;
    budget.ensure_add(framed_file_bytes(&encoded)?)?;
    let bytes = write_records(&path, EVENT_MAGIC, encoded.into_iter().map(Ok))?;
    budget.add(bytes)?;
    events.clear();
    Ok(path)
}

fn resolve_safe_frontier(
    snapshot: FrontierPosition,
    upper: FrontierPosition,
    scratch: &Path,
    event_runs: Vec<PathBuf>,
    budget: &mut ScratchBudget,
    stats: &mut ColumnarV2StreamingStats,
) -> Result<FrontierPosition, ColumnarV2StreamingError> {
    if event_runs.is_empty() {
        return Ok(upper);
    }
    stats.peak_open_runs = stats.peak_open_runs.max(event_runs.len().min(MERGE_FAN_IN));
    let merged = merge_event_runs(scratch, event_runs, budget, stats)?;
    let mut reader = EventReader::open(&merged)?;
    let mut active = 0i64;
    let mut safe = snapshot;
    let mut pending_sequence = None;
    let mut pending_delta = 0i64;
    while let Some(event) = reader.next()? {
        if pending_sequence.is_some_and(|sequence| sequence != event.sequence) {
            active = active
                .checked_add(pending_delta)
                .ok_or(ColumnarV2StreamingError::BoundExceeded)?;
            if active < 0 {
                return Err(ColumnarV2StreamingError::Invalid);
            }
            if active == 0 {
                safe = FrontierPosition::AppliedThrough(
                    pending_sequence.ok_or(ColumnarV2StreamingError::Invalid)?,
                );
            }
            pending_delta = 0;
        }
        pending_sequence = Some(event.sequence);
        pending_delta = pending_delta
            .checked_add(i64::from(event.delta))
            .ok_or(ColumnarV2StreamingError::BoundExceeded)?;
    }
    if let Some(sequence) = pending_sequence {
        active = active
            .checked_add(pending_delta)
            .ok_or(ColumnarV2StreamingError::BoundExceeded)?;
        if active < 0 {
            return Err(ColumnarV2StreamingError::Invalid);
        }
        if active == 0 {
            safe = FrontierPosition::AppliedThrough(sequence);
        }
    }
    if active == 0 {
        safe = upper;
    }
    remove_budgeted(&merged, budget)?;
    Ok(safe)
}

fn merge_event_runs(
    scratch: &Path,
    mut paths: Vec<PathBuf>,
    budget: &mut ScratchBudget,
    stats: &mut ColumnarV2StreamingStats,
) -> Result<PathBuf, ColumnarV2StreamingError> {
    let mut pass = 0usize;
    while paths.len() > 1 {
        let mut next = Vec::new();
        for (group, chunk) in paths.chunks(MERGE_FAN_IN).enumerate() {
            stats.peak_open_runs = stats.peak_open_runs.max(chunk.len());
            stats.peak_live_row_bytes =
                stats.peak_live_row_bytes.max(chunk.len().saturating_mul(9));
            let path = scratch.join(format!("deferral-merge-{pass:04x}-{group:08x}.run"));
            budget.ensure_add(sum_file_bytes(chunk)?)?;
            let bytes = merge_event_group(chunk, &path)?;
            budget.add(bytes)?;
            next.push(path);
        }
        for path in paths {
            remove_budgeted(&path, budget)?;
        }
        paths = next;
        pass = pass
            .checked_add(1)
            .ok_or(ColumnarV2StreamingError::BoundExceeded)?;
    }
    paths.pop().ok_or(ColumnarV2StreamingError::Invalid)
}

#[derive(Eq, PartialEq)]
struct EventHeapItem {
    event: DeferralEvent,
    reader: usize,
}

impl Ord for EventHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.event
            .cmp(&other.event)
            .then_with(|| self.reader.cmp(&other.reader))
    }
}

impl PartialOrd for EventHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn merge_event_group(inputs: &[PathBuf], output: &Path) -> Result<u64, ColumnarV2StreamingError> {
    let mut readers = inputs
        .iter()
        .map(|path| EventReader::open(path))
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(event) = reader.next()? {
            heap.push(Reverse(EventHeapItem {
                event,
                reader: index,
            }));
        }
    }
    let mut writer = FramedWriter::create(output, EVENT_MAGIC)?;
    while let Some(Reverse(item)) = heap.pop() {
        writer.write(&encode_event(&item.event)?)?;
        if let Some(event) = readers[item.reader].next()? {
            heap.push(Reverse(EventHeapItem {
                event,
                reader: item.reader,
            }));
        }
    }
    writer.finish()
}

fn collapse_latest_rows(
    scratch: &Path,
    observations: &Path,
    safe: FrontierPosition,
    budget: &mut ScratchBudget,
) -> Result<PathBuf, ColumnarV2StreamingError> {
    let path = scratch.join("tail-collapsed.run");
    budget.ensure_add(
        fs::metadata(observations)
            .map_err(|_| ColumnarV2StreamingError::Io)?
            .len(),
    )?;
    let mut reader = ObservationReader::open(observations)?;
    let mut writer = FramedWriter::create(&path, COLLAPSED_MAGIC)?;
    let mut pending: Option<ProjectedRow> = None;
    let mut current_key: Option<PrimaryKeyBytes> = None;
    while let Some(observation) = reader.next()? {
        if current_key.as_ref() != Some(&observation.key) {
            if let Some(row) = pending.take() {
                writer.write(&encode_projected_row(&row)?)?;
            }
            current_key = Some(observation.key.clone());
        }
        if FrontierPosition::AppliedThrough(observation.sequence) <= safe
            && let TailObservationKind::Matched(row) = observation.kind
        {
            pending = Some(row);
        }
    }
    if let Some(row) = pending {
        writer.write(&encode_projected_row(&row)?)?;
    }
    let bytes = writer.finish()?;
    budget.add(bytes)?;
    Ok(path)
}

pub(crate) struct ProjectedRowReader {
    reader: FramedReader,
    previous: Option<PrimaryKeyBytes>,
}

pub(crate) fn open_partition_lane(
    path: &Path,
) -> Result<ProjectedRowReader, ColumnarV2StreamingError> {
    ProjectedRowReader::open(path, COLLAPSED_MAGIC)
}

pub(crate) fn remove_partition_lane(path: &Path) -> Result<(), ColumnarV2StreamingError> {
    fs::remove_file(path).map_err(|_| ColumnarV2StreamingError::Io)
}

impl ProjectedRowReader {
    fn open(path: &Path, magic: &[u8; 8]) -> Result<Self, ColumnarV2StreamingError> {
        Ok(Self {
            reader: FramedReader::open(path, magic)?,
            previous: None,
        })
    }

    pub(crate) fn next(&mut self) -> Result<Option<ProjectedRow>, ColumnarV2StreamingError> {
        let Some(payload) = self.reader.next()? else {
            return Ok(None);
        };
        let row = decode_projected_row(&payload)?;
        if self.previous.as_ref().is_some_and(|key| key >= &row.key) {
            return Err(ColumnarV2StreamingError::Invalid);
        }
        self.previous = Some(row.key.clone());
        Ok(Some(row))
    }
}

struct ObservationReader {
    reader: FramedReader,
}

impl ObservationReader {
    fn open(path: &Path) -> Result<Self, ColumnarV2StreamingError> {
        Ok(Self {
            reader: FramedReader::open(path, OBSERVATION_MAGIC)?,
        })
    }

    fn next(&mut self) -> Result<Option<TailObservation>, ColumnarV2StreamingError> {
        self.reader
            .next()?
            .map(|bytes| decode_observation(&bytes))
            .transpose()
    }
}

struct EventReader {
    reader: FramedReader,
}

impl EventReader {
    fn open(path: &Path) -> Result<Self, ColumnarV2StreamingError> {
        Ok(Self {
            reader: FramedReader::open(path, EVENT_MAGIC)?,
        })
    }

    fn next(&mut self) -> Result<Option<DeferralEvent>, ColumnarV2StreamingError> {
        self.reader
            .next()?
            .map(|bytes| decode_event(&bytes))
            .transpose()
    }
}

struct FramedWriter {
    writer: BufWriter<File>,
    bytes: u64,
}

impl FramedWriter {
    fn create(path: &Path, magic: &[u8; 8]) -> Result<Self, ColumnarV2StreamingError> {
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .map_err(|_| ColumnarV2StreamingError::Io)?;
        let mut writer = BufWriter::new(file);
        writer
            .write_all(magic)
            .map_err(|_| ColumnarV2StreamingError::Io)?;
        Ok(Self { writer, bytes: 8 })
    }

    fn write(&mut self, payload: &[u8]) -> Result<(), ColumnarV2StreamingError> {
        if payload.is_empty() || payload.len() > MAX_SCRATCH_FRAME_PAYLOAD {
            return Err(ColumnarV2StreamingError::BoundExceeded);
        }
        let length =
            u32::try_from(payload.len()).map_err(|_| ColumnarV2StreamingError::BoundExceeded)?;
        self.writer
            .write_all(&length.to_be_bytes())
            .and_then(|()| self.writer.write_all(payload))
            .and_then(|()| self.writer.write_all(&checksum_bytes(payload)))
            .map_err(|_| ColumnarV2StreamingError::Io)?;
        self.bytes = self
            .bytes
            .checked_add(4 + payload.len() as u64 + 32)
            .ok_or(ColumnarV2StreamingError::BoundExceeded)?;
        Ok(())
    }

    fn finish(mut self) -> Result<u64, ColumnarV2StreamingError> {
        self.writer
            .flush()
            .map_err(|_| ColumnarV2StreamingError::Io)?;
        self.writer
            .get_ref()
            .sync_all()
            .map_err(|_| ColumnarV2StreamingError::Io)?;
        Ok(self.bytes)
    }
}

struct FramedReader {
    reader: BufReader<File>,
}

impl FramedReader {
    fn open(path: &Path, magic: &[u8; 8]) -> Result<Self, ColumnarV2StreamingError> {
        let file = File::open(path).map_err(|_| ColumnarV2StreamingError::Io)?;
        let mut reader = BufReader::new(file);
        let mut observed = [0u8; 8];
        reader
            .read_exact(&mut observed)
            .map_err(|_| ColumnarV2StreamingError::Invalid)?;
        if &observed != magic {
            return Err(ColumnarV2StreamingError::Invalid);
        }
        Ok(Self { reader })
    }

    fn next(&mut self) -> Result<Option<Vec<u8>>, ColumnarV2StreamingError> {
        let mut length = [0u8; 4];
        let mut read = 0usize;
        while read < length.len() {
            let count = self
                .reader
                .read(&mut length[read..])
                .map_err(|_| ColumnarV2StreamingError::Io)?;
            if count == 0 {
                return if read == 0 {
                    Ok(None)
                } else {
                    Err(ColumnarV2StreamingError::Invalid)
                };
            }
            read += count;
        }
        let length = u32::from_be_bytes(length) as usize;
        if length == 0 || length > MAX_SCRATCH_FRAME_PAYLOAD {
            return Err(ColumnarV2StreamingError::BoundExceeded);
        }
        let mut payload = vec![0u8; length];
        let mut checksum = [0u8; 32];
        self.reader
            .read_exact(&mut payload)
            .and_then(|()| self.reader.read_exact(&mut checksum))
            .map_err(|_| ColumnarV2StreamingError::Invalid)?;
        if checksum_bytes(&payload) != checksum {
            return Err(ColumnarV2StreamingError::Invalid);
        }
        Ok(Some(payload))
    }
}

fn write_records<E>(
    path: &Path,
    magic: &[u8; 8],
    records: impl Iterator<Item = Result<Vec<u8>, E>>,
) -> Result<u64, ColumnarV2StreamingError>
where
    ColumnarV2StreamingError: From<E>,
{
    let mut writer = FramedWriter::create(path, magic)?;
    for record in records {
        writer.write(&record.map_err(ColumnarV2StreamingError::from)?)?;
    }
    writer.finish()
}

fn remove_budgeted(
    path: &Path,
    budget: &mut ScratchBudget,
) -> Result<(), ColumnarV2StreamingError> {
    let bytes = fs::metadata(path)
        .map_err(|_| ColumnarV2StreamingError::Io)?
        .len();
    fs::remove_file(path).map_err(|_| ColumnarV2StreamingError::Io)?;
    budget.remove(bytes)
}

fn framed_file_bytes(records: &[Vec<u8>]) -> Result<u64, ColumnarV2StreamingError> {
    records.iter().try_fold(8u64, |total, record| {
        total
            .checked_add(4 + record.len() as u64 + 32)
            .ok_or(ColumnarV2StreamingError::BoundExceeded)
    })
}

fn sum_file_bytes(paths: &[PathBuf]) -> Result<u64, ColumnarV2StreamingError> {
    paths.iter().try_fold(0u64, |total, path| {
        total
            .checked_add(
                fs::metadata(path)
                    .map_err(|_| ColumnarV2StreamingError::Io)?
                    .len(),
            )
            .ok_or(ColumnarV2StreamingError::BoundExceeded)
    })
}

fn append_frame(path: &Path, payload: &[u8]) -> Result<(), ColumnarV2StreamingError> {
    if payload.is_empty() || payload.len() > MAX_SCRATCH_FRAME_PAYLOAD {
        return Err(ColumnarV2StreamingError::BoundExceeded);
    }
    let mut file = OpenOptions::new()
        .append(true)
        .open(path)
        .map_err(|_| ColumnarV2StreamingError::Io)?;
    let length =
        u32::try_from(payload.len()).map_err(|_| ColumnarV2StreamingError::BoundExceeded)?;
    file.write_all(&length.to_be_bytes())
        .and_then(|()| file.write_all(payload))
        .and_then(|()| file.write_all(&checksum_bytes(payload)))
        .map_err(|_| ColumnarV2StreamingError::Io)
}

fn encode_observation(value: &TailObservation) -> Result<Vec<u8>, ColumnarV2StreamingError> {
    let mut bytes = Vec::new();
    push_bytes(&mut bytes, value.key.as_bytes())?;
    bytes.extend_from_slice(&value.sequence.to_be_bytes());
    match &value.kind {
        TailObservationKind::Matched(row) => {
            bytes.push(1);
            let row = encode_projected_row(row)?;
            push_bytes(&mut bytes, &row)?;
        }
        TailObservationKind::ForwardRace(version) => {
            bytes.push(2);
            bytes.extend_from_slice(&version.to_be_bytes());
        }
    }
    if bytes.len() > MAX_OBSERVATION_PAYLOAD {
        return Err(ColumnarV2StreamingError::BoundExceeded);
    }
    Ok(bytes)
}

fn decode_observation(bytes: &[u8]) -> Result<TailObservation, ColumnarV2StreamingError> {
    let mut cursor = Cursor::new(bytes);
    let key = PrimaryKeyBytes::from_entity_key_bytes(cursor.read_bytes(MAX_KEY_BYTES)?.to_vec());
    let sequence =
        CommitSequence::new(cursor.read_u64()?).ok_or(ColumnarV2StreamingError::Invalid)?;
    let kind = match cursor.read_u8()? {
        1 => {
            let row = decode_projected_row(cursor.read_bytes(MAX_PROJECTED_ROW_PAYLOAD)?)?;
            if row.key != key {
                return Err(ColumnarV2StreamingError::Invalid);
            }
            TailObservationKind::Matched(row)
        }
        2 => TailObservationKind::ForwardRace(
            EntityVersion::new(cursor.read_u64()?).ok_or(ColumnarV2StreamingError::Invalid)?,
        ),
        _ => return Err(ColumnarV2StreamingError::Invalid),
    };
    cursor.finish()?;
    Ok(TailObservation {
        key,
        sequence,
        kind,
    })
}

fn encode_projected_row(row: &ProjectedRow) -> Result<Vec<u8>, ColumnarV2StreamingError> {
    let mut bytes = Vec::new();
    push_bytes(&mut bytes, row.key.as_bytes())?;
    bytes.extend_from_slice(&row.version.to_be_bytes());
    push_bytes(&mut bytes, row.organization.as_bytes())?;
    let count =
        u32::try_from(row.cells.len()).map_err(|_| ColumnarV2StreamingError::BoundExceeded)?;
    bytes.extend_from_slice(&count.to_be_bytes());
    for cell in &row.cells {
        let encoded =
            encode_canonical_value(cell).map_err(|_| ColumnarV2StreamingError::Invalid)?;
        push_bytes(&mut bytes, &encoded)?;
    }
    let state_bytes = row
        .organization
        .as_bytes()
        .len()
        .checked_add(
            row.cells
                .iter()
                .map(|value| encode_canonical_value(value).map(|bytes| bytes.len()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| ColumnarV2StreamingError::Invalid)?
                .into_iter()
                .try_fold(0usize, usize::checked_add)
                .ok_or(ColumnarV2StreamingError::BoundExceeded)?,
        )
        .ok_or(ColumnarV2StreamingError::BoundExceeded)?;
    if state_bytes > MAX_PROVIDER_STATE_BYTES_PER_ROW || bytes.len() > MAX_PROJECTED_ROW_PAYLOAD {
        return Err(ColumnarV2StreamingError::BoundExceeded);
    }
    Ok(bytes)
}

fn decode_projected_row(bytes: &[u8]) -> Result<ProjectedRow, ColumnarV2StreamingError> {
    let mut cursor = Cursor::new(bytes);
    let key = PrimaryKeyBytes::from_entity_key_bytes(cursor.read_bytes(MAX_KEY_BYTES)?.to_vec());
    let version =
        EntityVersion::new(cursor.read_u64()?).ok_or(ColumnarV2StreamingError::Invalid)?;
    let organization = OrgKey::from_encoded_bytes(
        cursor
            .read_bytes(MAX_PROVIDER_STATE_BYTES_PER_ROW)?
            .to_vec(),
    );
    let organization_value = decode_canonical_value(organization.as_bytes())
        .map_err(|_| ColumnarV2StreamingError::Invalid)?;
    if encode_canonical_value(&organization_value).map_err(|_| ColumnarV2StreamingError::Invalid)?
        != organization.as_bytes()
    {
        return Err(ColumnarV2StreamingError::Invalid);
    }
    let count = cursor.read_u32()? as usize;
    if count > 4_096 {
        return Err(ColumnarV2StreamingError::BoundExceeded);
    }
    let mut cells = Vec::with_capacity(count);
    for _ in 0..count {
        let encoded = cursor.read_bytes(MAX_PROVIDER_STATE_BYTES_PER_ROW)?;
        let value =
            decode_canonical_value(encoded).map_err(|_| ColumnarV2StreamingError::Invalid)?;
        if encode_canonical_value(&value).map_err(|_| ColumnarV2StreamingError::Invalid)? != encoded
        {
            return Err(ColumnarV2StreamingError::Invalid);
        }
        cells.push(value);
    }
    cursor.finish()?;
    let row = ProjectedRow {
        key,
        version,
        organization,
        cells,
    };
    if encode_projected_row(&row)? != bytes {
        return Err(ColumnarV2StreamingError::Invalid);
    }
    Ok(row)
}

fn encode_event(event: &DeferralEvent) -> Result<Vec<u8>, ColumnarV2StreamingError> {
    Ok([
        event.sequence.to_be_bytes().as_slice(),
        &[event.delta as u8],
    ]
    .concat())
}

fn decode_event(bytes: &[u8]) -> Result<DeferralEvent, ColumnarV2StreamingError> {
    if bytes.len() != 9 {
        return Err(ColumnarV2StreamingError::Invalid);
    }
    let mut sequence = [0u8; 8];
    sequence.copy_from_slice(&bytes[..8]);
    let delta = bytes[8] as i8;
    if !matches!(delta, -1 | 1) {
        return Err(ColumnarV2StreamingError::Invalid);
    }
    Ok(DeferralEvent {
        sequence: CommitSequence::new(u64::from_be_bytes(sequence))
            .ok_or(ColumnarV2StreamingError::Invalid)?,
        delta,
    })
}

fn push_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<(), ColumnarV2StreamingError> {
    let length = u32::try_from(value.len()).map_err(|_| ColumnarV2StreamingError::BoundExceeded)?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_u8(&mut self) -> Result<u8, ColumnarV2StreamingError> {
        let value = *self
            .bytes
            .get(self.offset)
            .ok_or(ColumnarV2StreamingError::Invalid)?;
        self.offset += 1;
        Ok(value)
    }

    fn read_u32(&mut self) -> Result<u32, ColumnarV2StreamingError> {
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(self.take(4)?);
        Ok(u32::from_be_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64, ColumnarV2StreamingError> {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(self.take(8)?);
        Ok(u64::from_be_bytes(bytes))
    }

    fn read_bytes(&mut self, maximum: usize) -> Result<&'a [u8], ColumnarV2StreamingError> {
        let length = self.read_u32()? as usize;
        if length == 0 || length > maximum {
            return Err(ColumnarV2StreamingError::BoundExceeded);
        }
        self.take(length)
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], ColumnarV2StreamingError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(ColumnarV2StreamingError::BoundExceeded)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(ColumnarV2StreamingError::Invalid)?;
        self.offset = end;
        Ok(value)
    }

    fn finish(self) -> Result<(), ColumnarV2StreamingError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(ColumnarV2StreamingError::Invalid)
        }
    }
}

fn map_columnar_error(error: ColumnarError) -> ColumnarV2StreamingError {
    match error {
        ColumnarError::Storage(_) | ColumnarError::Io(_) => ColumnarV2StreamingError::Io,
        ColumnarError::Definition(_)
        | ColumnarError::Query(_)
        | ColumnarError::Integrity(_)
        | ColumnarError::Projection(_)
        | ColumnarError::Checkpoint(_)
        | ColumnarError::InvalidState(_) => ColumnarV2StreamingError::Invalid,
    }
}

fn hit(controller: Option<&ColumnarTestController>, boundary: ColumnarTestBoundary) -> bool {
    controller.is_some_and(|controller| controller.hit(boundary))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_directory(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "riffdb-columnar-streaming-{label}-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("temporary directory");
        path
    }

    fn row(key: u16, version: u64, org: u64) -> ProjectedRow {
        ProjectedRow {
            key: PrimaryKeyBytes::from_entity_key_bytes(key.to_be_bytes().to_vec()),
            version: EntityVersion::new(version).expect("version"),
            organization: OrgKey::from_value(&CanonicalValue::U64(org)).expect("org"),
            cells: vec![CanonicalValue::U64(version)],
        }
    }

    fn matched(key: u16, sequence: u64, version: u64, org: u64) -> TailObservation {
        TailObservation {
            key: PrimaryKeyBytes::from_entity_key_bytes(key.to_be_bytes().to_vec()),
            sequence: CommitSequence::new(sequence).expect("sequence"),
            kind: TailObservationKind::Matched(row(key, version, org)),
        }
    }

    // req: PRJ-004, PRJ-009, PRJ-010, OQ-020
    #[test]
    fn scratch_budget_is_checked_and_derived_from_replay_and_row_bounds() {
        let limits = ColumnarSpecReplayLimitsV1::new(1, 1_024, 2).expect("limits");
        let budget = ScratchBudget::new(limits).expect("derived ceiling");
        let per_observation = MAX_OBSERVATION_PAYLOAD as u64 + FRAME_OVERHEAD;
        assert_eq!(
            budget.ceiling,
            1_024 + 4 * 2 * MAX_ENTITY_MUTATIONS as u64 * per_observation
        );
        assert_eq!(MAX_ENTITY_MUTATIONS, 4_096);
    }

    // req: PRJ-004, PRJ-009, PRJ-010, OQ-020
    #[test]
    fn replay_age_is_strict_second_precise_and_clamps_future_time_to_zero() {
        let oldest = riffdb_types::LogicalTime::new(
            riffdb_types::Timestamp::new(100, 500_000_000).expect("oldest"),
        );
        assert!(!replay_age_exceeded(
            riffdb_types::Timestamp::new(160, 500_000_000).expect("inclusive boundary"),
            Some(oldest),
            60,
        ));
        assert!(replay_age_exceeded(
            riffdb_types::Timestamp::new(160, 500_000_001).expect("one nanosecond over"),
            Some(oldest),
            60,
        ));
        assert!(!replay_age_exceeded(
            riffdb_types::Timestamp::new(99, 0).expect("clock rollback"),
            Some(oldest),
            0,
        ));
        assert!(!replay_age_exceeded(
            riffdb_types::Timestamp::new(200, 0).expect("empty tail sample"),
            None,
            0,
        ));
    }

    // req: PRJ-004, PRJ-009, PRJ-010, OQ-020
    #[test]
    fn simultaneous_replay_failures_use_age_then_bytes_then_backlog_precedence() {
        assert_eq!(
            replay_limit_failure(true, true, true),
            Some(ColumnarV2StreamingError::ReplayAge)
        );
        assert_eq!(
            replay_limit_failure(false, true, true),
            Some(ColumnarV2StreamingError::ReplayBytes)
        );
        assert_eq!(
            replay_limit_failure(false, false, true),
            Some(ColumnarV2StreamingError::ReplayBacklog)
        );
        assert_eq!(replay_limit_failure(false, false, false), None);
    }

    // req: PRJ-004, PRJ-009, OQ-020
    #[test]
    fn fixed_run_and_fan_in_bounds_are_partition_scale() {
        assert_eq!(MAX_RUN_ROWS, 256);
        assert_eq!(MAX_RUN_PAYLOAD_BYTES, 4 * 1024 * 1024);
        assert_eq!(MERGE_FAN_IN, 8);
        const { assert!(MAX_RUN_ROWS < crate::MAX_SEGMENT_V2_ROWS) };
        assert_eq!(MAX_PARTITION_LANE_ROWS, 4096 * 65536);
        const { assert!(V2_SNAPSHOT_BUILD_MAX_FILES > 4096 * 4096) };
        const { assert!(V2_SNAPSHOT_BUILD_MAX_BYTES > 4096 * 4096 * 64 * 1024 * 1024_u64) };
    }

    // req: PRJ-004, PRJ-009, REP-002
    #[test]
    fn partition_lane_refuses_excess_rows_before_appending_bytes() {
        let directory = temp_directory("lane-bound");
        let path = directory.join("partition.lane");
        write_records(
            &path,
            COLLAPSED_MAGIC,
            std::iter::empty::<Result<Vec<u8>, ColumnarV2StreamingError>>(),
        )
        .expect("empty lane");
        let mut lane = PartitionLane {
            path: path.clone(),
            remaining: 2,
        };
        lane.append(&row(1, 1, 1)).expect("first row");
        lane.append(&row(2, 1, 1)).expect("inclusive ceiling");
        let at_limit = fs::read(&path).expect("bounded lane");
        assert_eq!(
            lane.append(&row(3, 1, 1)),
            Err(ColumnarV2StreamingError::BoundExceeded)
        );
        assert_eq!(fs::read(&path).expect("unchanged lane"), at_limit);
        let mut reader = open_partition_lane(&path).expect("reader");
        assert_eq!(
            reader.next().expect("first").expect("row").key,
            row(1, 1, 1).key
        );
        assert_eq!(
            reader.next().expect("second").expect("row").key,
            row(2, 1, 1).key
        );
        assert!(reader.next().expect("exact end").is_none());
        drop(reader);
        fs::remove_dir_all(directory).expect("cleanup");
    }

    // req: PRJ-002, PRJ-004, PRJ-009
    #[test]
    fn deferral_events_preserve_deferred_frontier_semantics() {
        let one = CommitSequence::new(1).expect("one");
        let two = CommitSequence::new(2).expect("two");
        let three = CommitSequence::new(3).expect("three");
        let mut events = [
            DeferralEvent {
                sequence: one,
                delta: 1,
            },
            DeferralEvent {
                sequence: two,
                delta: 1,
            },
            DeferralEvent {
                sequence: two,
                delta: -1,
            },
            DeferralEvent {
                sequence: three,
                delta: -1,
            },
        ];
        events.sort();
        let mut active = 0i64;
        let mut last_safe = FrontierPosition::BeforeFirst;
        for sequence in [one, two, three] {
            active += events
                .iter()
                .filter(|event| event.sequence == sequence)
                .map(|event| i64::from(event.delta))
                .sum::<i64>();
            if active == 0 {
                last_safe = FrontierPosition::AppliedThrough(sequence);
            }
        }
        assert_eq!(last_safe, FrontierPosition::AppliedThrough(three));
    }

    // req: PRJ-004, PRJ-009, PRJ-010, OQ-020
    #[test]
    fn fixed_fan_in_merge_orders_more_runs_than_can_be_opened_at_once() {
        let directory = temp_directory("fan-in");
        let limits = ColumnarSpecReplayLimitsV1::new(1, 1_024, 32).expect("limits");
        let mut budget = ScratchBudget::new(limits).expect("budget");
        let mut paths = Vec::new();
        for ordinal in 0..(MERGE_FAN_IN * 2 + 1) {
            let key = u16::try_from(MERGE_FAN_IN * 2 - ordinal).expect("key");
            let mut records = vec![matched(key, 1, 1, 1)];
            paths.push(
                write_observation_run(&directory, ordinal, &mut records, &mut budget).expect("run"),
            );
        }
        let mut stats = ColumnarV2StreamingStats::default();
        let merged = merge_observation_runs(&directory, paths, &mut budget, &mut stats, None)
            .expect("fixed fan-in merge");
        let mut reader = ObservationReader::open(&merged).expect("merged reader");
        let mut keys = Vec::new();
        while let Some(observation) = reader.next().expect("record") {
            keys.push(observation.key);
        }
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(keys.len(), MERGE_FAN_IN * 2 + 1);
        assert_eq!(stats.peak_open_runs(), MERGE_FAN_IN);
        assert!(stats.peak_buffered_rows() <= MAX_RUN_ROWS);
        assert!(stats.peak_buffered_bytes() <= MAX_RUN_PAYLOAD_BYTES);
        assert!(stats.peak_live_row_bytes() <= MERGE_FAN_IN * MAX_OBSERVATION_PAYLOAD);
        assert!(budget.peak > 0);
        assert!(budget.peak <= budget.ceiling);
        fs::remove_dir_all(directory).expect("cleanup");
    }

    // req: PRJ-004, PRJ-009, PRJ-010
    #[test]
    fn duplicate_or_out_of_order_observations_refuse_before_collapse() {
        let directory = temp_directory("noncanonical");
        let mut budget = ScratchBudget {
            ceiling: u64::MAX,
            ..ScratchBudget::default()
        };
        let mut duplicates = vec![
            matched(1, 1, 1, 1),
            TailObservation {
                key: PrimaryKeyBytes::from_entity_key_bytes(1u16.to_be_bytes().to_vec()),
                sequence: CommitSequence::new(1).expect("sequence"),
                kind: TailObservationKind::ForwardRace(EntityVersion::new(2).expect("version")),
            },
        ];
        assert_eq!(
            write_observation_run(&directory, 0, &mut duplicates, &mut budget),
            Err(ColumnarV2StreamingError::Invalid)
        );

        let path = directory.join("out-of-order.run");
        let mut writer = FramedWriter::create(&path, OBSERVATION_MAGIC).expect("writer");
        writer
            .write(&encode_observation(&matched(2, 1, 1, 1)).expect("encode"))
            .expect("write");
        writer
            .write(&encode_observation(&matched(1, 1, 1, 1)).expect("encode"))
            .expect("write");
        writer.finish().expect("finish");
        assert_eq!(
            validate_observation_run(&path),
            Err(ColumnarV2StreamingError::Invalid)
        );
        fs::remove_dir_all(directory).expect("cleanup");
    }

    // req: PRJ-004, PRJ-009, OQ-020
    #[test]
    fn scratch_ceiling_refuses_before_creating_another_run() {
        let directory = temp_directory("ceiling");
        let mut budget = ScratchBudget {
            ceiling: 8,
            ..ScratchBudget::default()
        };
        let mut records = vec![matched(1, 1, 1, 1)];
        let path = directory.join("observation-00000000.run");
        assert_eq!(
            write_observation_run(&directory, 0, &mut records, &mut budget),
            Err(ColumnarV2StreamingError::BoundExceeded)
        );
        assert!(!path.exists());
        assert_eq!(budget.live, 0);
        fs::remove_dir_all(directory).expect("cleanup");
    }

    // req: PRJ-002, PRJ-004, PRJ-009
    #[test]
    fn a_to_b_to_c_deferral_collapses_only_at_the_exact_resolving_frontier() {
        let directory = temp_directory("deferred-frontier");
        let observations = directory.join("observations.run");
        let key = PrimaryKeyBytes::from_entity_key_bytes(7u16.to_be_bytes().to_vec());
        let values = [
            TailObservation {
                key: key.clone(),
                sequence: CommitSequence::new(1).expect("one"),
                kind: TailObservationKind::ForwardRace(EntityVersion::new(3).expect("three")),
            },
            TailObservation {
                key: key.clone(),
                sequence: CommitSequence::new(2).expect("two"),
                kind: TailObservationKind::ForwardRace(EntityVersion::new(3).expect("three")),
            },
            matched(7, 3, 3, 30),
        ];
        write_records(
            &observations,
            OBSERVATION_MAGIC,
            values.iter().map(encode_observation),
        )
        .expect("observations");
        let limits = ColumnarSpecReplayLimitsV1::new(1, 1_024, 4).expect("limits");
        let mut budget = ScratchBudget::new(limits).expect("budget");
        budget
            .add(fs::metadata(&observations).expect("metadata").len())
            .expect("charge");
        let mut stats = ColumnarV2StreamingStats::default();
        let events = build_deferral_event_runs(&directory, &observations, &mut budget, &mut stats)
            .expect("events");
        let safe = resolve_safe_frontier(
            FrontierPosition::BeforeFirst,
            FrontierPosition::AppliedThrough(CommitSequence::new(3).expect("three")),
            &directory,
            events,
            &mut budget,
            &mut stats,
        )
        .expect("safe frontier");
        assert_eq!(
            safe,
            FrontierPosition::AppliedThrough(CommitSequence::new(3).expect("three"))
        );
        let collapsed =
            collapse_latest_rows(&directory, &observations, safe, &mut budget).expect("collapsed");
        let mut reader = ProjectedRowReader::open(&collapsed, COLLAPSED_MAGIC).expect("reader");
        let resolved = reader.next().expect("row").expect("resolved row");
        assert_eq!(resolved.organization, row(7, 3, 30).organization);
        assert!(reader.next().expect("end").is_none());
        fs::remove_dir_all(directory).expect("cleanup");
    }

    // req: PRJ-004, PRJ-009
    #[test]
    fn scratch_directory_cleanup_is_exact_on_failure_and_cancellation_drop() {
        let generation = temp_directory("cleanup");
        let scratch_path = generation.join(SCRATCH_DIRECTORY);
        {
            let scratch = ScratchDirectory::create(&generation).expect("scratch");
            fs::write(scratch.path.join("partial"), b"partial").expect("partial run");
        }
        assert!(!scratch_path.exists());
        fs::remove_dir_all(generation).expect("cleanup");
    }
}
