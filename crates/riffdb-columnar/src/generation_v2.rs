//! Immutable filesystem finalization and validate-once open for V2 generations.

use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use riffdb_contract_ir::{ValueType, ValueTypeTag};
use riffdb_types::{
    CanonicalValue, DecimalSpec, FrontierPosition, MAX_DECIMAL_PRECISION, ProjectionGeneration,
};

use crate::checkpoint::checksum_bytes;
use crate::hooks::{ColumnarTestBoundary, ColumnarTestController};
use crate::store::{Segment, SegmentId};
use crate::{
    COLUMNAR_GENERATION_ROOT_FILE_NAME_V1, ColumnarGenerationRootV1, ColumnarGenerationRootV1Entry,
    ColumnarManifestV2, ColumnarManifestV2Entry, ColumnarSnapshot, LiveRow,
    MAX_COLUMNAR_GENERATION_ROOT_V1_BYTES, MAX_COLUMNAR_MANIFEST_V2_BYTES, MAX_SEGMENT_V2_BYTES,
    MAX_SEGMENT_V2_ROWS, OrgKey, PhysicalGenerationFingerprintV1, PrimaryKeyBytes,
    RegisteredDefinition, SegmentV2, SegmentV2Cell, SegmentV2Codec, SegmentV2Column,
    SegmentV2Identity, SegmentV2LogicalType, SegmentV2SegmentId,
};

/// Closed failures while finalizing or opening one immutable V2 generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColumnarV2GenerationError {
    /// A fixed derived-state bound was exceeded.
    BoundExceeded,
    /// Durable bytes or cross-file identities were invalid.
    Invalid,
    /// A filesystem operation failed.
    Io,
    /// Independent logical rows did not equal the reopened candidate.
    LogicalMismatch,
    /// The registered field types are not in the accepted Segment V2 registry.
    UnsupportedDefinition,
}

impl fmt::Display for ColumnarV2GenerationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::BoundExceeded => "columnar V2 generation bound exceeded",
            Self::Invalid => "columnar V2 generation is invalid",
            Self::Io => "columnar V2 generation I/O failed",
            Self::LogicalMismatch => "columnar V2 generation logical equality failed",
            Self::UnsupportedDefinition => "columnar V2 generation definition is unsupported",
        })
    }
}

impl std::error::Error for ColumnarV2GenerationError {}

/// One completely reopened, cross-file validated, immutable V2 generation.
pub struct ValidatedColumnarV2Generation {
    root: ColumnarGenerationRootV1,
    artifact_identity: (u64, [u8; 32]),
    snapshot: Arc<ColumnarSnapshot>,
    directory: PathBuf,
}

impl ValidatedColumnarV2Generation {
    /// Whether the frozen Segment V2 registry can encode every projected field.
    #[must_use]
    pub fn supports_definition(definition: &RegisteredDefinition) -> bool {
        logical_types(definition).is_ok()
    }

    /// Exact private evaluator/build directory for an allocated generation.
    #[doc(hidden)]
    #[must_use]
    pub fn temporary_directory(
        source_directory: &Path,
        generation: ProjectionGeneration,
    ) -> PathBuf {
        source_directory.join(format!("{}.tmp", generation_directory_name(generation)))
    }

    /// Removes only the exact immutable directory for one already unselected
    /// V2 generation. Selection and captured-view fencing are owned by the
    /// server caller; this helper owns only crash-idempotent filesystem work.
    #[doc(hidden)]
    pub fn reclaim(
        source_directory: &Path,
        generation: ProjectionGeneration,
    ) -> Result<(), ColumnarV2GenerationError> {
        Self::reclaim_with_optional_controller(source_directory, generation, None)
    }

    /// Identical reclamation with fixed crash boundaries for process tests.
    #[doc(hidden)]
    pub fn reclaim_with_controller(
        source_directory: &Path,
        generation: ProjectionGeneration,
        controller: &ColumnarTestController,
    ) -> Result<(), ColumnarV2GenerationError> {
        Self::reclaim_with_optional_controller(source_directory, generation, Some(controller))
    }

    fn reclaim_with_optional_controller(
        source_directory: &Path,
        generation: ProjectionGeneration,
        controller: Option<&ColumnarTestController>,
    ) -> Result<(), ColumnarV2GenerationError> {
        let directory = source_directory.join(generation_directory_name(generation));
        if !directory.exists() {
            return Ok(());
        }
        if hit(controller, ColumnarTestBoundary::BeforeV2GenerationReclaim) {
            return Err(ColumnarV2GenerationError::Io);
        }
        fs::remove_dir_all(&directory).map_err(|_| ColumnarV2GenerationError::Io)?;
        if hit(controller, ColumnarTestBoundary::AfterV2GenerationReclaim) {
            return Err(ColumnarV2GenerationError::Io);
        }
        File::open(source_directory)
            .and_then(|parent| parent.sync_all())
            .map_err(|_| ColumnarV2GenerationError::Io)
    }

    /// Writes one disjoint generation and reopens every member before return.
    pub fn prepare(
        source_directory: &Path,
        definition: RegisteredDefinition,
        history_incarnation: u64,
        generation: ProjectionGeneration,
        snapshot_frontier: FrontierPosition,
        expected: &ColumnarSnapshot,
    ) -> Result<Self, ColumnarV2GenerationError> {
        Self::prepare_with_optional_controller(
            source_directory,
            definition,
            history_incarnation,
            generation,
            snapshot_frontier,
            expected,
            None,
        )
    }

    /// Identical finalization with fixed crash boundaries for process tests.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_with_controller(
        source_directory: &Path,
        definition: RegisteredDefinition,
        history_incarnation: u64,
        generation: ProjectionGeneration,
        snapshot_frontier: FrontierPosition,
        expected: &ColumnarSnapshot,
        controller: &ColumnarTestController,
    ) -> Result<Self, ColumnarV2GenerationError> {
        Self::prepare_with_optional_controller(
            source_directory,
            definition,
            history_incarnation,
            generation,
            snapshot_frontier,
            expected,
            Some(controller),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_with_optional_controller(
        source_directory: &Path,
        definition: RegisteredDefinition,
        history_incarnation: u64,
        generation: ProjectionGeneration,
        snapshot_frontier: FrontierPosition,
        expected: &ColumnarSnapshot,
        controller: Option<&ColumnarTestController>,
    ) -> Result<Self, ColumnarV2GenerationError> {
        if snapshot_frontier > expected.visible_frontier {
            return Err(ColumnarV2GenerationError::Invalid);
        }
        // The independent rebuild evaluator deliberately remains in its
        // canonical row map. Accepting layered V1 segments here would require
        // materializing an unbounded second merged partition merely to encode
        // and compare it. Production rebuilds always satisfy this invariant.
        if !expected.segments.is_empty() {
            return Err(ColumnarV2GenerationError::Invalid);
        }
        let logical_types = logical_types(&definition)?;
        fs::create_dir_all(source_directory).map_err(|_| ColumnarV2GenerationError::Io)?;
        let directory_name = generation_directory_name(generation);
        let final_directory = source_directory.join(&directory_name);
        if final_directory.exists() {
            let opened = Self::open_unchecked_artifact(
                source_directory,
                &definition,
                history_incarnation,
                generation,
                expected.visible_frontier,
                Some(expected),
            )?;
            return Ok(opened);
        }

        let temporary_directory = Self::temporary_directory(source_directory, generation);
        if temporary_directory.exists() {
            fs::remove_dir_all(&temporary_directory).map_err(|_| ColumnarV2GenerationError::Io)?;
        }
        fs::create_dir(&temporary_directory).map_err(|_| ColumnarV2GenerationError::Io)?;

        let organizations = expected
            .delta
            .iter()
            .filter(|(_, rows)| !rows.is_empty())
            .map(|(organization, _)| organization.clone());
        let mut root_entries = Vec::with_capacity(expected.delta.len());
        let mut total_segments = 0u64;
        let mut total_rows = 0u64;
        let mut next_segment = 1u64;
        for organization in organizations {
            let rows = expected
                .delta
                .get(&organization)
                .ok_or(ColumnarV2GenerationError::LogicalMismatch)?;
            let mut manifest_entries = Vec::new();
            let mut row_iterator = rows.iter();
            loop {
                let chunk = row_iterator
                    .by_ref()
                    .take(MAX_SEGMENT_V2_ROWS)
                    .collect::<Vec<_>>();
                if chunk.is_empty() {
                    break;
                }
                let segment_id = segment_id(generation, next_segment);
                next_segment = next_segment
                    .checked_add(1)
                    .ok_or(ColumnarV2GenerationError::BoundExceeded)?;
                let segment = build_segment(
                    &definition,
                    &logical_types,
                    history_incarnation,
                    generation,
                    organization.clone(),
                    segment_id,
                    snapshot_frontier,
                    expected.visible_frontier,
                    &chunk,
                )?;
                let bytes = SegmentV2Codec::encode(&segment)
                    .map_err(|_| ColumnarV2GenerationError::Invalid)?;
                let checksum = checksum_bytes(&bytes);
                let entry = ColumnarManifestV2Entry::new(
                    segment_id,
                    snapshot_frontier,
                    expected.visible_frontier,
                    chunk.len(),
                    bytes.len(),
                    checksum,
                )
                .map_err(|_| ColumnarV2GenerationError::Invalid)?;
                write_immutable_member(
                    &temporary_directory,
                    &entry.file_name(),
                    &bytes,
                    controller,
                    Some(ColumnarTestBoundary::BeforeV2SegmentSync),
                    Some(ColumnarTestBoundary::AfterV2SegmentRename),
                )?;
                manifest_entries.push(entry);
                total_segments = total_segments
                    .checked_add(1)
                    .ok_or(ColumnarV2GenerationError::BoundExceeded)?;
                total_rows = total_rows
                    .checked_add(chunk.len() as u64)
                    .ok_or(ColumnarV2GenerationError::BoundExceeded)?;
            }
            let manifest = ColumnarManifestV2::new(
                definition.fingerprint(),
                history_incarnation,
                generation,
                organization.clone(),
                expected.visible_frontier,
                manifest_entries,
            )
            .map_err(|_| ColumnarV2GenerationError::Invalid)?;
            let bytes = manifest
                .encode()
                .map_err(|_| ColumnarV2GenerationError::Invalid)?;
            let checksum = checksum_bytes(&bytes);
            let root_entry =
                ColumnarGenerationRootV1Entry::new(organization, bytes.len() as u64, checksum)
                    .map_err(|_| ColumnarV2GenerationError::Invalid)?;
            write_immutable_member(
                &temporary_directory,
                &root_entry.file_name(),
                &bytes,
                controller,
                None,
                Some(ColumnarTestBoundary::AfterV2ManifestRename),
            )?;
            root_entries.push(root_entry);
        }

        let root = ColumnarGenerationRootV1::new(
            definition.fingerprint(),
            history_incarnation,
            generation,
            expected.visible_frontier,
            total_segments,
            total_rows,
            root_entries,
        )
        .map_err(|_| ColumnarV2GenerationError::Invalid)?;
        let root_bytes = root
            .encode()
            .map_err(|_| ColumnarV2GenerationError::Invalid)?;
        write_immutable_member(
            &temporary_directory,
            COLUMNAR_GENERATION_ROOT_FILE_NAME_V1,
            &root_bytes,
            controller,
            None,
            Some(ColumnarTestBoundary::AfterV2RootRename),
        )?;
        File::open(&temporary_directory)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| ColumnarV2GenerationError::Io)?;
        fs::rename(&temporary_directory, &final_directory)
            .map_err(|_| ColumnarV2GenerationError::Io)?;
        if hit(controller, ColumnarTestBoundary::AfterV2GenerationRename) {
            return Err(ColumnarV2GenerationError::Io);
        }
        File::open(source_directory)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| ColumnarV2GenerationError::Io)?;

        let opened = Self::open_unchecked_artifact(
            source_directory,
            &definition,
            history_incarnation,
            generation,
            expected.visible_frontier,
            Some(expected),
        )?;
        Ok(opened)
    }

    /// Opens only the exact root artifact selected by durable control.
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        source_directory: &Path,
        definition: RegisteredDefinition,
        history_incarnation: u64,
        generation: ProjectionGeneration,
        frontier: FrontierPosition,
        artifact_identity: (u64, [u8; 32]),
        physical_generation_fingerprint: PhysicalGenerationFingerprintV1,
    ) -> Result<Self, ColumnarV2GenerationError> {
        let opened = Self::open_unchecked_artifact(
            source_directory,
            &definition,
            history_incarnation,
            generation,
            frontier,
            None,
        )?;
        if opened.artifact_identity != artifact_identity
            || opened.root.physical_generation_fingerprint() != physical_generation_fingerprint
        {
            return Err(ColumnarV2GenerationError::Invalid);
        }
        Ok(opened)
    }

    fn open_unchecked_artifact(
        source_directory: &Path,
        definition: &RegisteredDefinition,
        history_incarnation: u64,
        generation: ProjectionGeneration,
        frontier: FrontierPosition,
        expected: Option<&ColumnarSnapshot>,
    ) -> Result<Self, ColumnarV2GenerationError> {
        let directory = source_directory.join(generation_directory_name(generation));
        let root_bytes = read_bounded(
            &directory.join(COLUMNAR_GENERATION_ROOT_FILE_NAME_V1),
            MAX_COLUMNAR_GENERATION_ROOT_V1_BYTES,
        )?;
        let root = ColumnarGenerationRootV1::decode(&root_bytes)
            .map_err(|_| ColumnarV2GenerationError::Invalid)?;
        if root.definition_fingerprint() != definition.fingerprint()
            || root.history_incarnation() != history_incarnation
            || root.generation() != generation
            || root.frontier() != frontier
        {
            return Err(ColumnarV2GenerationError::Invalid);
        }
        let logical_types = logical_types(definition)?;
        let mut segments = Vec::new();
        let mut total_segments = 0u64;
        let mut total_rows = 0u64;
        let expected_organizations = expected.map(|snapshot| {
            snapshot
                .delta
                .iter()
                .filter(|(_, rows)| !rows.is_empty())
                .map(|(organization, _)| organization)
                .collect::<Vec<_>>()
        });
        if let Some(organizations) = &expected_organizations
            && (expected.is_some_and(|snapshot| !snapshot.segments.is_empty())
                || organizations.len() != root.partitions().len()
                || root
                    .partitions()
                    .iter()
                    .zip(organizations)
                    .any(|(entry, organization)| entry.organization() != *organization))
        {
            return Err(ColumnarV2GenerationError::LogicalMismatch);
        }
        for root_entry in root.partitions() {
            let mut previous_primary_key: Option<PrimaryKeyBytes> = None;
            let mut expected_rows = expected
                .and_then(|snapshot| snapshot.delta.get(root_entry.organization()))
                .map(BTreeMap::iter);
            let manifest_name = root_entry.file_name();
            let manifest_bytes = read_bounded(
                &directory.join(&manifest_name),
                MAX_COLUMNAR_MANIFEST_V2_BYTES,
            )?;
            if manifest_bytes.len() as u64 != root_entry.manifest_length()
                || checksum_bytes(&manifest_bytes) != *root_entry.manifest_checksum()
            {
                return Err(ColumnarV2GenerationError::Invalid);
            }
            let manifest = ColumnarManifestV2::decode(&manifest_bytes)
                .map_err(|_| ColumnarV2GenerationError::Invalid)?;
            if manifest.definition_fingerprint() != definition.fingerprint()
                || manifest.history_incarnation() != history_incarnation
                || manifest.generation() != generation
                || manifest.organization() != root_entry.organization()
                || manifest.durable_frontier() != frontier
            {
                return Err(ColumnarV2GenerationError::Invalid);
            }
            for entry in manifest.segments() {
                let file_name = entry.file_name();
                let bytes = read_bounded(&directory.join(&file_name), MAX_SEGMENT_V2_BYTES)?;
                if bytes.len() != entry.file_length() || checksum_bytes(&bytes) != *entry.checksum()
                {
                    return Err(ColumnarV2GenerationError::Invalid);
                }
                let (segment, pruning) = SegmentV2Codec::decode_with_pruning(&bytes)
                    .map_err(|_| ColumnarV2GenerationError::Invalid)?;
                validate_segment(
                    &segment,
                    definition,
                    &logical_types,
                    history_incarnation,
                    generation,
                    root_entry.organization(),
                    entry,
                )?;
                let rows = decode_rows(&segment, definition)?;
                if previous_primary_key.as_ref().is_some_and(|previous| {
                    rows.keys().next().is_some_and(|first| previous >= first)
                }) {
                    return Err(ColumnarV2GenerationError::Invalid);
                }
                previous_primary_key = rows.keys().next_back().cloned();
                if let Some(expected_rows) = &mut expected_rows {
                    for (key, row) in &rows {
                        let Some((expected_key, expected_row)) = expected_rows.next() else {
                            return Err(ColumnarV2GenerationError::LogicalMismatch);
                        };
                        if key != expected_key || row != expected_row {
                            return Err(ColumnarV2GenerationError::LogicalMismatch);
                        }
                    }
                }
                total_segments = total_segments
                    .checked_add(1)
                    .ok_or(ColumnarV2GenerationError::BoundExceeded)?;
                total_rows = total_rows
                    .checked_add(rows.len() as u64)
                    .ok_or(ColumnarV2GenerationError::BoundExceeded)?;
                segments.push(Arc::new(Segment {
                    id: SegmentId::from_file_name(file_name),
                    org: root_entry.organization().clone(),
                    rows,
                    checksum: *entry.checksum(),
                    pruning: Some(pruning),
                }));
            }
            if expected_rows.is_some_and(|mut rows| rows.next().is_some()) {
                return Err(ColumnarV2GenerationError::LogicalMismatch);
            }
        }
        if total_segments != root.total_segments() || total_rows != root.total_rows() {
            return Err(ColumnarV2GenerationError::Invalid);
        }
        validate_exact_inventory(&directory, &root)?;
        if expected.is_some_and(|snapshot| snapshot.visible_frontier != frontier) {
            return Err(ColumnarV2GenerationError::LogicalMismatch);
        }
        let snapshot = Arc::new(ColumnarSnapshot {
            segments,
            delta: BTreeMap::new(),
            visible_frontier: frontier,
        });
        Ok(Self {
            root,
            artifact_identity: (root_bytes.len() as u64, checksum_bytes(&root_bytes)),
            snapshot,
            directory,
        })
    }

    /// Complete validated generation root.
    #[must_use]
    pub const fn root(&self) -> &ColumnarGenerationRootV1 {
        &self.root
    }

    /// Positive root length and checksum installed into common control.
    #[must_use]
    pub const fn artifact_identity(&self) -> (u64, [u8; 32]) {
        self.artifact_identity
    }

    /// Immutable decoded view used by queries without further durable reads.
    #[must_use]
    pub fn snapshot(&self) -> &Arc<ColumnarSnapshot> {
        &self.snapshot
    }

    /// Exact immutable generation directory.
    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.directory
    }
}

fn logical_types(
    definition: &RegisteredDefinition,
) -> Result<Vec<SegmentV2LogicalType>, ColumnarV2GenerationError> {
    definition
        .projected_types()
        .iter()
        .map(logical_type)
        .collect()
}

fn logical_type(value_type: &ValueType) -> Result<SegmentV2LogicalType, ColumnarV2GenerationError> {
    let value_type = value_type.optional_inner().unwrap_or(value_type);
    match value_type.tag() {
        ValueTypeTag::Bool => Ok(SegmentV2LogicalType::Bool),
        ValueTypeTag::I64 => Ok(SegmentV2LogicalType::I64),
        ValueTypeTag::U64 => Ok(SegmentV2LogicalType::U64),
        ValueTypeTag::String => Ok(SegmentV2LogicalType::String),
        ValueTypeTag::Bytes => Ok(SegmentV2LogicalType::Bytes),
        ValueTypeTag::Timestamp => Ok(SegmentV2LogicalType::Timestamp),
        ValueTypeTag::Date => Ok(SegmentV2LogicalType::Date),
        ValueTypeTag::Uuid => Ok(SegmentV2LogicalType::Uuid),
        ValueTypeTag::Enum => value_type
            .enum_type_id()
            .map(SegmentV2LogicalType::Enum)
            .ok_or(ColumnarV2GenerationError::UnsupportedDefinition),
        ValueTypeTag::Decimal => value_type
            .decimal_spec()
            .map(SegmentV2LogicalType::Decimal)
            .ok_or(ColumnarV2GenerationError::UnsupportedDefinition),
        ValueTypeTag::Money => Ok(SegmentV2LogicalType::Money {
            currency: value_type
                .currency()
                .ok_or(ColumnarV2GenerationError::UnsupportedDefinition)?,
            amount: DecimalSpec::new(MAX_DECIMAL_PRECISION, 2)
                .map_err(|_| ColumnarV2GenerationError::UnsupportedDefinition)?,
        }),
        ValueTypeTag::Optional
        | ValueTypeTag::List
        | ValueTypeTag::Record
        | ValueTypeTag::Vector => Err(ColumnarV2GenerationError::UnsupportedDefinition),
    }
}

#[allow(clippy::too_many_arguments)]
fn build_segment(
    definition: &RegisteredDefinition,
    logical_types: &[SegmentV2LogicalType],
    history_incarnation: u64,
    generation: ProjectionGeneration,
    organization: OrgKey,
    segment_id: SegmentV2SegmentId,
    snapshot_frontier: FrontierPosition,
    applied_frontier: FrontierPosition,
    rows: &[(&PrimaryKeyBytes, &LiveRow)],
) -> Result<SegmentV2, ColumnarV2GenerationError> {
    let primary_keys = rows.iter().map(|(key, _)| (*key).clone()).collect();
    let versions = rows.iter().map(|(_, row)| row.entity_version).collect();
    let mut columns = Vec::with_capacity(definition.projected_fields().len());
    for (position, (field, logical_type)) in definition
        .projected_fields()
        .iter()
        .zip(logical_types)
        .enumerate()
    {
        let cells = rows
            .iter()
            .map(|(_, row)| {
                row.cells
                    .get(position)
                    .cloned()
                    .map(|value| {
                        if value == CanonicalValue::Null {
                            SegmentV2Cell::Null
                        } else {
                            SegmentV2Cell::Value(value)
                        }
                    })
                    .ok_or(ColumnarV2GenerationError::LogicalMismatch)
            })
            .collect::<Result<Vec<_>, _>>()?;
        columns.push(
            SegmentV2Column::new(*field, logical_type.clone(), cells)
                .map_err(|_| ColumnarV2GenerationError::LogicalMismatch)?,
        );
    }
    let identity = SegmentV2Identity::new(
        definition.fingerprint(),
        history_incarnation,
        generation,
        organization,
        segment_id,
        snapshot_frontier,
        applied_frontier,
    )
    .map_err(|_| ColumnarV2GenerationError::Invalid)?;
    SegmentV2::new(identity, primary_keys, versions, columns)
        .map_err(|_| ColumnarV2GenerationError::Invalid)
}

fn validate_segment(
    segment: &SegmentV2,
    definition: &RegisteredDefinition,
    logical_types: &[SegmentV2LogicalType],
    history_incarnation: u64,
    generation: ProjectionGeneration,
    organization: &OrgKey,
    entry: &ColumnarManifestV2Entry,
) -> Result<(), ColumnarV2GenerationError> {
    let identity = segment.identity();
    if identity.definition_fingerprint() != definition.fingerprint()
        || identity.history_incarnation() != history_incarnation
        || identity.generation() != generation
        || identity.organization() != organization
        || identity.segment_id() != entry.segment_id()
        || identity.frontier_start() != entry.frontier_start()
        || identity.frontier_end() != entry.frontier_end()
        || segment.primary_keys().len() != entry.row_count()
        || segment.columns().len() != definition.projected_fields().len()
    {
        return Err(ColumnarV2GenerationError::Invalid);
    }
    for column in segment.columns() {
        let Some(position) = definition
            .projected_fields()
            .iter()
            .position(|field| *field == column.field_id())
        else {
            return Err(ColumnarV2GenerationError::Invalid);
        };
        if column.logical_type() != &logical_types[position] {
            return Err(ColumnarV2GenerationError::Invalid);
        }
    }
    Ok(())
}

fn decode_rows(
    segment: &SegmentV2,
    definition: &RegisteredDefinition,
) -> Result<BTreeMap<PrimaryKeyBytes, LiveRow>, ColumnarV2GenerationError> {
    let mut columns = BTreeMap::new();
    for column in segment.columns() {
        columns.insert(column.field_id(), column);
    }
    let mut rows = BTreeMap::new();
    for row_index in 0..segment.primary_keys().len() {
        let mut cells = Vec::with_capacity(definition.projected_fields().len());
        for field in definition.projected_fields() {
            let cell = columns
                .get(field)
                .and_then(|column| column.cells().get(row_index))
                .ok_or(ColumnarV2GenerationError::Invalid)?;
            let value = match cell {
                SegmentV2Cell::Value(value) => value.clone(),
                SegmentV2Cell::Null => CanonicalValue::Null,
                SegmentV2Cell::Missing => return Err(ColumnarV2GenerationError::Invalid),
            };
            let position = definition
                .projected_fields()
                .iter()
                .position(|projected| projected == field)
                .ok_or(ColumnarV2GenerationError::Invalid)?;
            definition.projected_types()[position]
                .validate_value(&value)
                .map_err(|_| ColumnarV2GenerationError::Invalid)?;
            cells.push(value);
        }
        rows.insert(
            segment.primary_keys()[row_index].clone(),
            LiveRow {
                entity_version: segment.entity_versions()[row_index],
                cells,
            },
        );
    }
    Ok(rows)
}

fn write_immutable_member(
    directory: &Path,
    file_name: &str,
    bytes: &[u8],
    controller: Option<&ColumnarTestController>,
    before_sync: Option<ColumnarTestBoundary>,
    after_rename: Option<ColumnarTestBoundary>,
) -> Result<(), ColumnarV2GenerationError> {
    let final_path = directory.join(file_name);
    let temporary_path = directory.join(format!("{file_name}.tmp"));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary_path)
        .map_err(|_| ColumnarV2GenerationError::Io)?;
    file.write_all(bytes)
        .map_err(|_| ColumnarV2GenerationError::Io)?;
    if let Some(boundary) = before_sync
        && hit(controller, boundary)
    {
        return Err(ColumnarV2GenerationError::Io);
    }
    file.sync_all().map_err(|_| ColumnarV2GenerationError::Io)?;
    fs::rename(&temporary_path, &final_path).map_err(|_| ColumnarV2GenerationError::Io)?;
    if let Some(boundary) = after_rename
        && hit(controller, boundary)
    {
        return Err(ColumnarV2GenerationError::Io);
    }
    File::open(directory)
        .and_then(|parent| parent.sync_all())
        .map_err(|_| ColumnarV2GenerationError::Io)
}

fn hit(controller: Option<&ColumnarTestController>, boundary: ColumnarTestBoundary) -> bool {
    if let Some(controller) = controller {
        controller.hit(boundary)
    } else {
        false
    }
}

fn read_bounded(path: &Path, maximum: usize) -> Result<Vec<u8>, ColumnarV2GenerationError> {
    let file = File::open(path).map_err(|_| ColumnarV2GenerationError::Io)?;
    read_bounded_from(file, maximum)
}

fn read_bounded_from(
    reader: impl Read,
    maximum: usize,
) -> Result<Vec<u8>, ColumnarV2GenerationError> {
    let read_limit = maximum
        .checked_add(1)
        .ok_or(ColumnarV2GenerationError::BoundExceeded)?;
    let mut bytes = Vec::with_capacity(maximum.min(8 * 1024));
    reader
        .take(read_limit as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| ColumnarV2GenerationError::Io)?;
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(ColumnarV2GenerationError::BoundExceeded);
    }
    Ok(bytes)
}

fn validate_exact_inventory(
    directory: &Path,
    root: &ColumnarGenerationRootV1,
) -> Result<(), ColumnarV2GenerationError> {
    let expected_count = 1u64
        .checked_add(root.partitions().len() as u64)
        .and_then(|value| value.checked_add(root.total_segments()))
        .ok_or(ColumnarV2GenerationError::BoundExceeded)?;
    validate_inventory_count(directory, expected_count, || {})
}

fn validate_inventory_count(
    directory: &Path,
    expected_count: u64,
    mut observed_entry: impl FnMut(),
) -> Result<(), ColumnarV2GenerationError> {
    let mut observed_count = 0u64;
    for entry in fs::read_dir(directory).map_err(|_| ColumnarV2GenerationError::Io)? {
        let entry = entry.map_err(|_| ColumnarV2GenerationError::Io)?;
        observed_entry();
        observed_count = observed_count
            .checked_add(1)
            .ok_or(ColumnarV2GenerationError::BoundExceeded)?;
        if observed_count > expected_count {
            return Err(ColumnarV2GenerationError::Invalid);
        }
        if !entry
            .file_type()
            .map_err(|_| ColumnarV2GenerationError::Io)?
            .is_file()
        {
            return Err(ColumnarV2GenerationError::Invalid);
        }
    }
    if observed_count == expected_count {
        Ok(())
    } else {
        Err(ColumnarV2GenerationError::Invalid)
    }
}

fn generation_directory_name(generation: ProjectionGeneration) -> String {
    format!("generation-{:016x}", generation.get())
}

fn segment_id(generation: ProjectionGeneration, ordinal: u64) -> SegmentV2SegmentId {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&generation.to_be_bytes());
    bytes[8..].copy_from_slice(&ordinal.to_be_bytes());
    SegmentV2SegmentId::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct GrowingReader {
        remaining: usize,
        served: Arc<AtomicUsize>,
    }

    impl Read for GrowingReader {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            let length = buffer.len().min(self.remaining);
            buffer[..length].fill(0x5a);
            self.remaining -= length;
            self.served.fetch_add(length, Ordering::Relaxed);
            Ok(length)
        }
    }

    // req: PRJ-006, PRJ-009, PRJ-010, OQ-020, OQ-022
    #[test]
    fn bounded_reader_refuses_size_growth_after_the_limit_without_overread() {
        const MAXIMUM: usize = 4_096;
        let served = Arc::new(AtomicUsize::new(0));
        let result = read_bounded_from(
            GrowingReader {
                remaining: MAXIMUM * 4,
                served: Arc::clone(&served),
            },
            MAXIMUM,
        );
        assert_eq!(result, Err(ColumnarV2GenerationError::BoundExceeded));
        assert_eq!(
            served.load(Ordering::Relaxed),
            MAXIMUM + 1,
            "a concurrently growing member is read only through the refusal byte"
        );
    }

    // req: PRJ-006, PRJ-009, PRJ-010, OQ-020, OQ-022
    #[test]
    fn excessive_inventory_refuses_at_the_first_unexpected_entry_without_member_reads() {
        static NEXT_DIRECTORY: AtomicUsize = AtomicUsize::new(0);
        let ordinal = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let directory = std::env::temp_dir().join(format!(
            "riffdb-columnar-inventory-{}-{ordinal}",
            std::process::id()
        ));
        fs::create_dir(&directory).expect("create inventory directory");
        for index in 0..16 {
            fs::write(
                directory.join(format!("unexpected-{index}")),
                b"not decoded",
            )
            .expect("write unexpected member");
        }
        let inspected = AtomicUsize::new(0);
        let result = validate_inventory_count(&directory, 2, || {
            inspected.fetch_add(1, Ordering::Relaxed);
        });
        fs::remove_dir_all(&directory).expect("remove inventory directory");

        assert_eq!(result, Err(ColumnarV2GenerationError::Invalid));
        assert_eq!(
            inspected.load(Ordering::Relaxed),
            3,
            "inventory work is capped at expected count plus one"
        );
    }
}
