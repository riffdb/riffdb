//! Durable manifest, segment files, and crash-safe open/recover (D5).

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use riffdb_types::{
    CommitSequence, EntityVersion, FrontierPosition, HashDomain, decode_canonical_value,
    encode_canonical_value, hash,
};

use crate::definition::{DefinitionFingerprint, LAYOUT_VERSION};
use crate::hooks::{ColumnarTestBoundary, ColumnarTestController};
use crate::store::{
    LiveRow, OrgDelta, OrgKey, PrimaryKeyBytes, RowState, Segment, SegmentId, WorkingState,
};

/// Checkpoint / segment durability failures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CheckpointError {
    /// Manifest could not be parsed.
    CorruptManifest(&'static str),
    /// Fingerprint does not match the registered definition.
    FingerprintMismatch {
        /// Expected fingerprint from the registered definition.
        expected: DefinitionFingerprint,
        /// Fingerprint recorded in the manifest.
        found: DefinitionFingerprint,
    },
    /// Segment file missing or checksum mismatch.
    SegmentIntegrity(&'static str),
    /// Filesystem operation failed.
    Io(String),
}

impl std::fmt::Display for CheckpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CorruptManifest(message) => write!(f, "corrupt manifest: {message}"),
            Self::FingerprintMismatch { expected, found } => {
                write!(
                    f,
                    "fingerprint mismatch: expected {expected}, found {found}"
                )
            }
            Self::SegmentIntegrity(message) => write!(f, "segment integrity: {message}"),
            Self::Io(message) => write!(f, "checkpoint I/O: {message}"),
        }
    }
}

impl std::error::Error for CheckpointError {}

impl From<std::io::Error> for CheckpointError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.to_string())
    }
}

/// One segment listed in the manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentInventoryEntry {
    /// Relative file name.
    pub file_name: String,
    /// Encoded org key.
    pub org: OrgKey,
    /// SHA-256 of file bytes.
    pub checksum: [u8; 32],
}

/// Durable MANIFEST contents (layout version, fingerprint, frontier, inventory).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestV1 {
    /// Layout version constant.
    pub layout_version: u32,
    /// Definition fingerprint at checkpoint time.
    pub fingerprint: DefinitionFingerprint,
    /// Durable / recoverable frontier (visible frontier at checkpoint).
    pub durable_frontier: FrontierPosition,
    /// Segment inventory with checksums.
    pub segments: Vec<SegmentInventoryEntry>,
}

const MANIFEST_NAME: &str = "MANIFEST";
const MAGIC: &[u8; 4] = b"RCOL";

impl ManifestV1 {
    /// Encodes the manifest to stable bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&self.layout_version.to_be_bytes());
        out.extend_from_slice(self.fingerprint.as_bytes());
        encode_frontier(&mut out, self.durable_frontier);
        out.extend_from_slice(&(self.segments.len() as u32).to_be_bytes());
        for segment in &self.segments {
            encode_bytes(&mut out, segment.file_name.as_bytes());
            encode_bytes(&mut out, segment.org.as_bytes());
            out.extend_from_slice(&segment.checksum);
        }
        out
    }

    /// Decodes a manifest from bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, CheckpointError> {
        let mut offset = 0;
        if bytes.len() < 4 || &bytes[0..4] != MAGIC {
            return Err(CheckpointError::CorruptManifest("bad magic"));
        }
        offset += 4;
        let layout_version = read_u32(bytes, &mut offset)?;
        if layout_version != LAYOUT_VERSION {
            return Err(CheckpointError::CorruptManifest("layout version"));
        }
        let mut fingerprint_bytes = [0u8; 32];
        fingerprint_bytes.copy_from_slice(read_exact(bytes, &mut offset, 32)?);
        let fingerprint = DefinitionFingerprint::from_bytes(fingerprint_bytes);
        let durable_frontier = decode_frontier(bytes, &mut offset)?;
        let count = read_u32(bytes, &mut offset)? as usize;
        let mut segments = Vec::with_capacity(count);
        for _ in 0..count {
            let file_name = String::from_utf8(read_bytes(bytes, &mut offset)?)
                .map_err(|_| CheckpointError::CorruptManifest("file name utf8"))?;
            let org = OrgKey(read_bytes(bytes, &mut offset)?);
            let mut checksum = [0u8; 32];
            checksum.copy_from_slice(read_exact(bytes, &mut offset, 32)?);
            segments.push(SegmentInventoryEntry {
                file_name,
                org,
                checksum,
            });
        }
        if offset != bytes.len() {
            return Err(CheckpointError::CorruptManifest("trailing bytes"));
        }
        Ok(Self {
            layout_version,
            fingerprint,
            durable_frontier,
            segments,
        })
    }
}

fn encode_frontier(out: &mut Vec<u8>, frontier: FrontierPosition) {
    match frontier {
        FrontierPosition::BeforeFirst => out.push(0),
        FrontierPosition::AppliedThrough(sequence) => {
            out.push(1);
            out.extend_from_slice(&sequence.get().to_be_bytes());
        }
    }
}

fn decode_frontier(bytes: &[u8], offset: &mut usize) -> Result<FrontierPosition, CheckpointError> {
    let tag = *read_exact(bytes, offset, 1)?
        .first()
        .ok_or(CheckpointError::CorruptManifest("frontier tag"))?;
    match tag {
        0 => Ok(FrontierPosition::BeforeFirst),
        1 => {
            let raw = read_u64(bytes, offset)?;
            let sequence = CommitSequence::new(raw)
                .ok_or(CheckpointError::CorruptManifest("frontier sequence"))?;
            Ok(FrontierPosition::AppliedThrough(sequence))
        }
        _ => Err(CheckpointError::CorruptManifest("frontier tag value")),
    }
}

fn encode_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

fn read_exact<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
    len: usize,
) -> Result<&'a [u8], CheckpointError> {
    let end = offset
        .checked_add(len)
        .ok_or(CheckpointError::CorruptManifest("overflow"))?;
    if end > bytes.len() {
        return Err(CheckpointError::CorruptManifest("truncated"));
    }
    let slice = &bytes[*offset..end];
    *offset = end;
    Ok(slice)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32, CheckpointError> {
    let raw = read_exact(bytes, offset, 4)?;
    Ok(u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> Result<u64, CheckpointError> {
    let raw = read_exact(bytes, offset, 8)?;
    Ok(u64::from_be_bytes([
        raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
    ]))
}

fn read_bytes(bytes: &[u8], offset: &mut usize) -> Result<Vec<u8>, CheckpointError> {
    let len = read_u32(bytes, offset)? as usize;
    Ok(read_exact(bytes, offset, len)?.to_vec())
}

/// Writes segment file bytes (length-prefixed rows).
pub(crate) fn encode_segment_rows(
    rows: &BTreeMap<PrimaryKeyBytes, LiveRow>,
) -> Result<Vec<u8>, CheckpointError> {
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&LAYOUT_VERSION.to_be_bytes());
    out.extend_from_slice(&(rows.len() as u32).to_be_bytes());
    for (key, row) in rows {
        encode_bytes(&mut out, key.as_bytes());
        out.extend_from_slice(&row.entity_version.get().to_be_bytes());
        out.extend_from_slice(&(row.cells.len() as u32).to_be_bytes());
        for cell in &row.cells {
            let encoded = encode_canonical_value(cell)
                .map_err(|_| CheckpointError::SegmentIntegrity("cell encode"))?;
            encode_bytes(&mut out, &encoded);
        }
    }
    Ok(out)
}

/// Reads segment rows from file bytes.
pub(crate) fn decode_segment_rows(
    bytes: &[u8],
) -> Result<BTreeMap<PrimaryKeyBytes, LiveRow>, CheckpointError> {
    let mut offset = 0;
    if bytes.len() < 4 || &bytes[0..4] != MAGIC {
        return Err(CheckpointError::SegmentIntegrity("bad magic"));
    }
    offset += 4;
    let layout = read_u32(bytes, &mut offset)?;
    if layout != LAYOUT_VERSION {
        return Err(CheckpointError::SegmentIntegrity("layout"));
    }
    let count = read_u32(bytes, &mut offset)? as usize;
    let mut rows = BTreeMap::new();
    for _ in 0..count {
        let key = PrimaryKeyBytes::from_entity_key_bytes(read_bytes(bytes, &mut offset)?);
        let version_raw = read_u64(bytes, &mut offset)?;
        let entity_version = EntityVersion::new(version_raw)
            .ok_or(CheckpointError::SegmentIntegrity("entity version"))?;
        let cell_count = read_u32(bytes, &mut offset)? as usize;
        let mut cells = Vec::with_capacity(cell_count);
        for _ in 0..cell_count {
            let encoded = read_bytes(bytes, &mut offset)?;
            let value = decode_canonical_value(&encoded)
                .map_err(|_| CheckpointError::SegmentIntegrity("cell decode"))?;
            cells.push(value);
        }
        rows.insert(
            key,
            LiveRow {
                entity_version,
                cells,
            },
        );
    }
    Ok(rows)
}

pub(crate) fn checksum_bytes(bytes: &[u8]) -> [u8; 32] {
    *hash(HashDomain::CanonicalValue, bytes).as_bytes()
}

/// Checkpoint directory operations.
pub(crate) struct CheckpointDir {
    root: PathBuf,
    controller: Option<ColumnarTestController>,
    /// Test-only: advance manifest frontier before segment sync (overclaim neuter).
    pub(crate) test_manifest_before_segment_sync: bool,
}

impl CheckpointDir {
    pub(crate) fn new(root: PathBuf) -> Result<Self, CheckpointError> {
        fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            controller: None,
            test_manifest_before_segment_sync: false,
        })
    }

    pub(crate) fn install_controller(&mut self, controller: ColumnarTestController) {
        self.controller = Some(controller);
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    fn hit(&self, boundary: ColumnarTestBoundary) {
        if let Some(controller) = &self.controller {
            controller.hit(boundary);
        }
    }

    /// Loads manifest if present; `None` means empty projection directory.
    pub(crate) fn load_manifest(&self) -> Result<Option<ManifestV1>, CheckpointError> {
        let path = self.root.join(MANIFEST_NAME);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(&path)?;
        Ok(Some(ManifestV1::decode(&bytes)?))
    }

    /// Opens segments listed in the manifest and builds working state.
    pub(crate) fn open_from_manifest(
        &self,
        manifest: &ManifestV1,
        expected_fingerprint: DefinitionFingerprint,
    ) -> Result<WorkingState, CheckpointError> {
        if manifest.fingerprint != expected_fingerprint {
            return Err(CheckpointError::FingerprintMismatch {
                expected: expected_fingerprint,
                found: manifest.fingerprint,
            });
        }
        let mut working = WorkingState {
            processed: manifest.durable_frontier,
            ..WorkingState::default()
        };
        let mut referenced = BTreeSet::new();
        for entry in &manifest.segments {
            referenced.insert(entry.file_name.clone());
            let path = self.root.join(&entry.file_name);
            let bytes = fs::read(&path)
                .map_err(|_| CheckpointError::SegmentIntegrity("segment file missing"))?;
            let checksum = checksum_bytes(&bytes);
            if checksum != entry.checksum {
                return Err(CheckpointError::SegmentIntegrity("checksum mismatch"));
            }
            let rows = decode_segment_rows(&bytes)?;
            working.segments.push(std::sync::Arc::new(Segment {
                id: SegmentId(entry.file_name.clone()),
                org: entry.org.clone(),
                rows,
                checksum,
            }));
        }
        self.sweep_unreferenced(&referenced)?;
        Ok(working)
    }

    fn sweep_unreferenced(&self, referenced: &BTreeSet<String>) -> Result<(), CheckpointError> {
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name == MANIFEST_NAME || name.starts_with('.') {
                continue;
            }
            if name.starts_with("seg-") && !referenced.contains(name.as_ref()) {
                let _ = fs::remove_file(entry.path());
            }
        }
        Ok(())
    }

    /// Flushes working delta into new segment files and publishes a new manifest
    /// at `durable_frontier` (must be the race-free visible frontier).
    pub(crate) fn checkpoint(
        &self,
        working: &mut WorkingState,
        fingerprint: DefinitionFingerprint,
        durable_frontier: FrontierPosition,
    ) -> Result<ManifestV1, CheckpointError> {
        // Build new segments from current delta (one segment per org with delta rows).
        let mut new_segments: Vec<std::sync::Arc<Segment>> = working.segments.clone();
        let mut next_ordinal = next_segment_ordinal(&new_segments);

        if self.test_manifest_before_segment_sync {
            // Neuter path (falsifiability): claim durable_frontier while leaving
            // delta undurable and segment inventory pre-delta — classic overclaim.
            let bogus = ManifestV1 {
                layout_version: LAYOUT_VERSION,
                fingerprint,
                durable_frontier,
                segments: inventory_from_segments(&new_segments),
            };
            self.write_manifest_atomic(&bogus)?;
            // Do not flush delta to segments; leave working.delta as-is for the
            // in-process engine, but durable reopen will see an empty projection
            // at a non-zero frontier.
            return Ok(bogus);
        }

        let delta = std::mem::take(&mut working.delta);
        for (org, org_delta) in delta {
            let rows = materialize_org_rows(&new_segments, &org, &org_delta);
            if rows.is_empty() {
                continue;
            }
            let file_name = format!("seg-{next_ordinal:08}.col");
            next_ordinal += 1;
            let bytes = encode_segment_rows(&rows)?;
            let checksum = checksum_bytes(&bytes);
            let path = self.root.join(&file_name);

            // write-temp → sync → rename is for the manifest; segment files are
            // written then synced before the manifest names them.
            {
                let mut file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path)?;
                file.write_all(&bytes)?;
                self.hit(ColumnarTestBoundary::BeforeSegmentSync);
                file.sync_all()?;
            }
            self.hit(ColumnarTestBoundary::AfterSegmentSync);

            new_segments.push(std::sync::Arc::new(Segment {
                id: SegmentId(file_name),
                org,
                rows,
                checksum,
            }));
        }

        working.segments = new_segments;
        working.delta = BTreeMap::new();

        let manifest = ManifestV1 {
            layout_version: LAYOUT_VERSION,
            fingerprint,
            durable_frontier,
            segments: inventory_from_segments(&working.segments),
        };
        self.write_manifest_atomic(&manifest)?;
        Ok(manifest)
    }

    fn write_manifest_atomic(&self, manifest: &ManifestV1) -> Result<(), CheckpointError> {
        let bytes = manifest.encode();
        let digest = checksum_bytes(&bytes);
        let hex = hex32(&digest);
        let final_path = self.root.join(MANIFEST_NAME);
        let mut temporary = final_path.as_os_str().to_owned();
        temporary.push(".");
        temporary.push(&hex);
        temporary.push(".tmp");
        let temporary = PathBuf::from(temporary);

        {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
        }

        self.hit(ColumnarTestBoundary::BeforeManifestRename);
        fs::rename(&temporary, &final_path)?;
        self.hit(ColumnarTestBoundary::AfterManifestRename);

        // Parent directory fsync so the rename is durable.
        let dir = File::open(&self.root)?;
        dir.sync_all()?;
        Ok(())
    }
}

fn inventory_from_segments(segments: &[std::sync::Arc<Segment>]) -> Vec<SegmentInventoryEntry> {
    segments
        .iter()
        .map(|segment| SegmentInventoryEntry {
            file_name: segment.id.file_name().to_string(),
            org: segment.org.clone(),
            checksum: segment.checksum,
        })
        .collect()
}

fn next_segment_ordinal(segments: &[std::sync::Arc<Segment>]) -> u64 {
    segments
        .iter()
        .filter_map(|segment| {
            segment
                .id
                .file_name()
                .strip_prefix("seg-")
                .and_then(|rest| rest.strip_suffix(".col"))
                .and_then(|digits| digits.parse::<u64>().ok())
        })
        .max()
        .map(|value| value + 1)
        .unwrap_or(1)
}

/// Materializes full org state: merge prior segments with delta, emit live rows.
fn materialize_org_rows(
    segments: &[std::sync::Arc<Segment>],
    org: &OrgKey,
    delta: &OrgDelta,
) -> BTreeMap<PrimaryKeyBytes, LiveRow> {
    let mut merged: BTreeMap<PrimaryKeyBytes, LiveRow> = BTreeMap::new();
    for segment in segments {
        if &segment.org != org {
            continue;
        }
        for (key, row) in &segment.rows {
            merged.insert(key.clone(), row.clone());
        }
    }
    for (key, state) in delta {
        match state {
            RowState::Live(row) => {
                let replace = match merged.get(key) {
                    None => true,
                    Some(existing) => crate::store::supersession_should_replace(
                        existing.entity_version.get(),
                        row.entity_version.get(),
                    ),
                };
                if replace {
                    merged.insert(key.clone(), row.clone());
                }
            }
            RowState::Tombstone { entity_version } => {
                if let Some(existing) = merged.get(key)
                    && existing.entity_version.get() <= entity_version.get()
                {
                    merged.remove(key);
                }
            }
        }
    }
    merged
}

fn hex32(bytes: &[u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}
