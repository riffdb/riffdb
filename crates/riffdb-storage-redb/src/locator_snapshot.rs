//! Checkpointed command-locator snapshot sidecar (`<db>.locator.riffsnapshot`, ADR-0335).
//!
//! This derived sidecar records the validated `LocatorGeneration` segment directory
//! through a checkpointed `database.redb` frontier `C`. On bounded clean or dirty
//! startup, `activate_operational_ports` verifies the snapshot's CRC32C and lineage
//! binding against the open database, loads the checkpointed segments up to `C`,
//! folds only the uncheckpointed suffix `(C, P]` from `COMMITS`, and publishes
//! `LocatorCapability::Complete` immediately at readiness. Any missing, truncated,
//! corrupt, or mismatched snapshot is logged and ignored so startup falls back to
//! background segment reconstruction without failing open.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use redb::ReadableDatabase;
use riffdb_storage_api::{
    CommandDerivedIndexKindV1, CommandDerivedMemberV1, CommandSegmentDigestV1, StorageError,
    StorageErrorKind, StoredEventRouteV1,
    proto_codec::{
        decode_database_identity_v1, decode_history_incarnation_v1, decode_record_registry_digest,
    },
};
use riffdb_types::{
    AdministrationSequence, CommitSequence, DatabaseId, EventHash, EventId, EventTypeId,
    PartitionKeyHash, SchemaHash,
};

use crate::error::{precommit_storage_error, storage_error, table_error, transaction_error};
use crate::journal::JournalTable;
use crate::layout::{META, META_DATABASE_ID, META_HISTORY_INCARNATION, META_RECORD_REGISTRY};
use crate::locator_memory::LocatorAllocationClass;
use crate::store::{RedbReadAccess, SharedRedb, read_administration_tail, read_commit_tail};
use crate::transient::{TransientIndexState, TransientIndexes};

const SNAPSHOT_MAGIC: [u8; 8] = *b"RIFFLOC1";
const SNAPSHOT_VERSION_V1: u16 = 1;
const MAX_SNAPSHOT_BYTES: usize = 128 * 1024 * 1024;
const MAX_SNAPSHOT_SEGMENTS: usize = 1_000_000;
const MAX_SEGMENT_MANIFEST_ENTRIES: usize = 16_384;
const MAX_SEGMENT_EVENTS: usize = 16_384;
const MAX_EXACT_KEY_BYTES: usize = 2_048;

const MIN_ENCODED_SEGMENT_BYTES: usize = 8 + 8 + 32 + 16 + 8 + 4 + 4;
const MIN_ENCODED_MANIFEST_ENTRY_BYTES: usize = 1 + 2 + 8 + 2 + 2 + 1;
const ENCODED_EVENT_BYTES: usize = 32 + 12 + 4 + 32;

/// Sibling file that holds the checkpointed command-locator snapshot (`<db>.locator.riffsnapshot`).
#[must_use]
pub fn locator_snapshot_path(database: &Path) -> PathBuf {
    let mut name = database
        .file_name()
        .unwrap_or_else(|| std::ffi::OsStr::new("db"))
        .to_os_string();
    name.push(".locator.riffsnapshot");
    database.with_file_name(name)
}

pub(crate) fn locator_snapshot_temp_path(database: &Path) -> PathBuf {
    let mut name = database
        .file_name()
        .unwrap_or_else(|| std::ffi::OsStr::new("db"))
        .to_os_string();
    name.push(".locator.riffsnapshot.tmp");
    database.with_file_name(name)
}

pub(crate) fn discard_locator_snapshot_files(database: &Path) {
    let _ = std::fs::remove_file(locator_snapshot_path(database));
    let _ = std::fs::remove_file(locator_snapshot_temp_path(database));
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SnapshotManifestEntryV1 {
    pub(crate) kind: CommandDerivedIndexKindV1,
    pub(crate) exact_key: Vec<u8>,
    pub(crate) segment_first: CommitSequence,
    pub(crate) command_ordinal: u16,
    pub(crate) member_ordinal: u16,
    pub(crate) member: CommandDerivedMemberV1,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SnapshotSegmentV1 {
    pub(crate) first: CommitSequence,
    pub(crate) last: CommitSequence,
    pub(crate) digest: CommandSegmentDigestV1,
    pub(crate) database: DatabaseId,
    pub(crate) history: u64,
    pub(crate) manifest_entries: Vec<SnapshotManifestEntryV1>,
    pub(crate) events: Vec<(PartitionKeyHash, StoredEventRouteV1)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RiffLocatorSnapshotV1 {
    pub(crate) database_id: DatabaseId,
    pub(crate) history_incarnation: u64,
    pub(crate) registry_digest: SchemaHash,
    pub(crate) retention_watermark: Option<CommitSequence>,
    pub(crate) checkpoint_application_sequence: Option<CommitSequence>,
    pub(crate) checkpoint_administration_sequence: Option<AdministrationSequence>,
    pub(crate) tail_segment: Option<(CommitSequence, CommandSegmentDigestV1)>,
    pub(crate) segments: Vec<SnapshotSegmentV1>,
}

const CRC32C_TABLE: [u32; 256] = {
    let mut table = [0u32; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            if (crc & 1) != 0 {
                crc = (crc >> 1) ^ 0x82F6_3B78;
            } else {
                crc >>= 1;
            }
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in bytes {
        let idx = ((crc ^ u32::from(byte)) & 0xFF) as usize;
        crc = (crc >> 8) ^ CRC32C_TABLE[idx];
    }
    !crc
}

impl RiffLocatorSnapshotV1 {
    pub(crate) fn encode(&self) -> Result<Vec<u8>, StorageError> {
        if self.segments.len() > MAX_SNAPSHOT_SEGMENTS {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let mut out = Vec::with_capacity(256);
        out.extend_from_slice(&SNAPSHOT_MAGIC);
        out.extend_from_slice(&SNAPSHOT_VERSION_V1.to_le_bytes());
        out.extend_from_slice(&self.database_id.into_bytes());
        out.extend_from_slice(&self.history_incarnation.to_le_bytes());
        out.extend_from_slice(self.registry_digest.as_bytes());
        encode_opt_commit_sequence(&mut out, self.retention_watermark);
        encode_opt_commit_sequence(&mut out, self.checkpoint_application_sequence);
        encode_opt_administration_sequence(&mut out, self.checkpoint_administration_sequence);
        match self.tail_segment {
            None => out.push(0),
            Some((first, digest)) => {
                out.push(1);
                out.extend_from_slice(&first.get().to_le_bytes());
                out.extend_from_slice(digest.as_bytes());
            }
        }
        let segment_count = u32::try_from(self.segments.len())
            .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        out.extend_from_slice(&segment_count.to_le_bytes());
        for segment in &self.segments {
            if segment.manifest_entries.len() > MAX_SEGMENT_MANIFEST_ENTRIES
                || segment.events.len() > MAX_SEGMENT_EVENTS
            {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
            out.extend_from_slice(&segment.first.get().to_le_bytes());
            out.extend_from_slice(&segment.last.get().to_le_bytes());
            out.extend_from_slice(segment.digest.as_bytes());
            out.extend_from_slice(&segment.database.into_bytes());
            out.extend_from_slice(&segment.history.to_le_bytes());
            let entry_count = u32::try_from(segment.manifest_entries.len())
                .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
            out.extend_from_slice(&entry_count.to_le_bytes());
            for entry in &segment.manifest_entries {
                if entry.exact_key.len() > MAX_EXACT_KEY_BYTES {
                    return Err(storage_error(StorageErrorKind::LimitExceeded));
                }
                out.push(encode_index_kind(entry.kind));
                let key_len = u16::try_from(entry.exact_key.len())
                    .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
                out.extend_from_slice(&key_len.to_le_bytes());
                out.extend_from_slice(&entry.exact_key);
                out.extend_from_slice(&entry.segment_first.get().to_le_bytes());
                out.extend_from_slice(&entry.command_ordinal.to_le_bytes());
                out.extend_from_slice(&entry.member_ordinal.to_le_bytes());
                out.push(encode_member(entry.member));
            }
            let event_count = u32::try_from(segment.events.len())
                .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
            out.extend_from_slice(&event_count.to_le_bytes());
            for (partition, route) in &segment.events {
                out.extend_from_slice(partition.as_bytes());
                out.extend_from_slice(&route.event_id().to_be_bytes());
                out.extend_from_slice(&route.event_type_id().get().to_le_bytes());
                out.extend_from_slice(route.event_hash().as_bytes());
            }
            if out.len() > MAX_SNAPSHOT_BYTES {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
        }
        let checksum = crc32c(&out);
        out.extend_from_slice(&checksum.to_le_bytes());
        Ok(out)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.len() < 8 + 2 + 16 + 8 + 32 + 1 + 1 + 1 + 1 + 4 + 4
            || bytes.len() > MAX_SNAPSHOT_BYTES
        {
            return Err("snapshot_length_invalid");
        }
        let (payload, checksum_bytes) = bytes.split_at(bytes.len() - 4);
        let expected_crc = u32::from_le_bytes([
            checksum_bytes[0],
            checksum_bytes[1],
            checksum_bytes[2],
            checksum_bytes[3],
        ]);
        if crc32c(payload) != expected_crc {
            return Err("crc32c_mismatch");
        }
        let mut cursor = SliceReader::new(payload);
        if cursor.read_array::<8>()? != SNAPSHOT_MAGIC {
            return Err("magic_mismatch");
        }
        let version = cursor.read_u16()?;
        if version != SNAPSHOT_VERSION_V1 {
            return Err("unsupported_version");
        }
        let database_id = DatabaseId::from_bytes(cursor.read_array::<16>()?)
            .map_err(|_| "invalid_database_id")?;
        let history_incarnation = cursor.read_u64()?;
        if history_incarnation == 0 {
            return Err("zero_history_incarnation");
        }
        let registry_digest = SchemaHash::from_bytes(cursor.read_array::<32>()?);
        let retention_watermark = cursor.read_opt_commit_sequence()?;
        let checkpoint_application_sequence = cursor.read_opt_commit_sequence()?;
        let checkpoint_administration_sequence = cursor.read_opt_administration_sequence()?;
        let tail_segment = match cursor.read_u8()? {
            0 => None,
            1 => {
                let first = CommitSequence::new(cursor.read_u64()?)
                    .ok_or("invalid_tail_commit_sequence")?;
                let digest = CommandSegmentDigestV1::from_bytes(cursor.read_array::<32>()?);
                Some((first, digest))
            }
            _ => return Err("invalid_tail_tag"),
        };
        let segment_count = usize::try_from(cursor.read_u32()?).map_err(|_| "segment_count")?;
        if segment_count > MAX_SNAPSHOT_SEGMENTS
            || segment_count > cursor.remaining() / MIN_ENCODED_SEGMENT_BYTES
        {
            return Err("segment_count_exceeds_bounds");
        }
        let mut segments = Vec::with_capacity(segment_count);
        for _ in 0..segment_count {
            let first =
                CommitSequence::new(cursor.read_u64()?).ok_or("invalid_segment_first_seq")?;
            let last = CommitSequence::new(cursor.read_u64()?).ok_or("invalid_segment_last_seq")?;
            if first > last {
                return Err("segment_sequence_order");
            }
            let digest = CommandSegmentDigestV1::from_bytes(cursor.read_array::<32>()?);
            let database = DatabaseId::from_bytes(cursor.read_array::<16>()?)
                .map_err(|_| "invalid_segment_database_id")?;
            let history = cursor.read_u64()?;
            if database != database_id || history == 0 || history > history_incarnation {
                return Err("segment_lineage_mismatch");
            }
            let entry_count = usize::try_from(cursor.read_u32()?).map_err(|_| "entry_count")?;
            if entry_count > MAX_SEGMENT_MANIFEST_ENTRIES
                || entry_count > cursor.remaining() / MIN_ENCODED_MANIFEST_ENTRY_BYTES
            {
                return Err("entry_count_exceeds_bounds");
            }
            let mut manifest_entries = Vec::with_capacity(entry_count);
            for _ in 0..entry_count {
                let kind = decode_index_kind(cursor.read_u8()?)?;
                let key_len = usize::from(cursor.read_u16()?);
                if key_len == 0 || key_len > MAX_EXACT_KEY_BYTES {
                    return Err("invalid_exact_key_length");
                }
                let exact_key = cursor.read_slice(key_len)?.to_vec();
                let segment_first = CommitSequence::new(cursor.read_u64()?)
                    .ok_or("invalid_manifest_segment_first")?;
                if segment_first != first {
                    return Err("manifest_segment_first_mismatch");
                }
                let command_ordinal = cursor.read_u16()?;
                let member_ordinal = cursor.read_u16()?;
                let member = decode_member(cursor.read_u8()?)?;
                manifest_entries.push(SnapshotManifestEntryV1 {
                    kind,
                    exact_key,
                    segment_first,
                    command_ordinal,
                    member_ordinal,
                    member,
                });
            }
            let event_count = usize::try_from(cursor.read_u32()?).map_err(|_| "event_count")?;
            if event_count > MAX_SEGMENT_EVENTS
                || event_count > cursor.remaining() / ENCODED_EVENT_BYTES
            {
                return Err("event_count_exceeds_bounds");
            }
            let mut events = Vec::with_capacity(event_count);
            for _ in 0..event_count {
                let partition = PartitionKeyHash::from_bytes(cursor.read_array::<32>()?);
                let event_id =
                    EventId::from_be_bytes(cursor.read_array::<12>()?).ok_or("invalid_event_id")?;
                let event_type_id =
                    EventTypeId::new(cursor.read_u32()?).ok_or("invalid_event_type_id")?;
                let event_hash = EventHash::from_bytes(cursor.read_array::<32>()?);
                events.push((
                    partition,
                    StoredEventRouteV1::new(event_id, event_type_id, event_hash),
                ));
            }
            segments.push(SnapshotSegmentV1 {
                first,
                last,
                digest,
                database,
                history,
                manifest_entries,
                events,
            });
        }
        if cursor.remaining() != 0 {
            return Err("trailing_bytes");
        }
        Ok(Self {
            database_id,
            history_incarnation,
            registry_digest,
            retention_watermark,
            checkpoint_application_sequence,
            checkpoint_administration_sequence,
            tail_segment,
            segments,
        })
    }
}

struct SliceReader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> SliceReader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    const fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    fn read_slice(&mut self, len: usize) -> Result<&'a [u8], &'static str> {
        let end = self.pos.checked_add(len).ok_or("truncated_snapshot")?;
        let slice = self.bytes.get(self.pos..end).ok_or("truncated_snapshot")?;
        self.pos = end;
        Ok(slice)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], &'static str> {
        let slice = self.read_slice(N)?;
        let mut array = [0u8; N];
        array.copy_from_slice(slice);
        Ok(array)
    }

    fn read_u8(&mut self) -> Result<u8, &'static str> {
        Ok(self.read_array::<1>()?[0])
    }

    fn read_u16(&mut self) -> Result<u16, &'static str> {
        Ok(u16::from_le_bytes(self.read_array::<2>()?))
    }

    fn read_u32(&mut self) -> Result<u32, &'static str> {
        Ok(u32::from_le_bytes(self.read_array::<4>()?))
    }

    fn read_u64(&mut self) -> Result<u64, &'static str> {
        Ok(u64::from_le_bytes(self.read_array::<8>()?))
    }

    fn read_opt_commit_sequence(&mut self) -> Result<Option<CommitSequence>, &'static str> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => CommitSequence::new(self.read_u64()?)
                .map(Some)
                .ok_or("invalid_commit_sequence"),
            _ => Err("invalid_option_tag"),
        }
    }

    fn read_opt_administration_sequence(
        &mut self,
    ) -> Result<Option<AdministrationSequence>, &'static str> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => AdministrationSequence::new(self.read_u64()?)
                .map(Some)
                .ok_or("invalid_administration_sequence"),
            _ => Err("invalid_option_tag"),
        }
    }
}

fn encode_opt_commit_sequence(out: &mut Vec<u8>, sequence: Option<CommitSequence>) {
    match sequence {
        None => out.push(0),
        Some(seq) => {
            out.push(1);
            out.extend_from_slice(&seq.get().to_le_bytes());
        }
    }
}

fn encode_opt_administration_sequence(out: &mut Vec<u8>, sequence: Option<AdministrationSequence>) {
    match sequence {
        None => out.push(0),
        Some(seq) => {
            out.push(1);
            out.extend_from_slice(&seq.get().to_le_bytes());
        }
    }
}

const fn encode_index_kind(kind: CommandDerivedIndexKindV1) -> u8 {
    match kind {
        CommandDerivedIndexKindV1::Idempotency => 1,
        CommandDerivedIndexKindV1::Provenance => 2,
        CommandDerivedIndexKindV1::AuditSequence => 3,
        CommandDerivedIndexKindV1::AuditRequest => 4,
        CommandDerivedIndexKindV1::EventRoute => 5,
        CommandDerivedIndexKindV1::PendingOutbox => 6,
    }
}

const fn decode_index_kind(tag: u8) -> Result<CommandDerivedIndexKindV1, &'static str> {
    match tag {
        1 => Ok(CommandDerivedIndexKindV1::Idempotency),
        2 => Ok(CommandDerivedIndexKindV1::Provenance),
        3 => Ok(CommandDerivedIndexKindV1::AuditSequence),
        4 => Ok(CommandDerivedIndexKindV1::AuditRequest),
        5 => Ok(CommandDerivedIndexKindV1::EventRoute),
        6 => Ok(CommandDerivedIndexKindV1::PendingOutbox),
        _ => Err("invalid_index_kind"),
    }
}

const fn encode_member(member: CommandDerivedMemberV1) -> u8 {
    match member {
        CommandDerivedMemberV1::Command => 1,
        CommandDerivedMemberV1::AuditStarted => 2,
        CommandDerivedMemberV1::AuditTerminal => 3,
        CommandDerivedMemberV1::Event => 4,
    }
}

const fn decode_member(tag: u8) -> Result<CommandDerivedMemberV1, &'static str> {
    match tag {
        1 => Ok(CommandDerivedMemberV1::Command),
        2 => Ok(CommandDerivedMemberV1::AuditStarted),
        3 => Ok(CommandDerivedMemberV1::AuditTerminal),
        4 => Ok(CommandDerivedMemberV1::Event),
        _ => Err("invalid_member_kind"),
    }
}

fn write_locator_snapshot_atomic(
    database: &Path,
    snapshot: &RiffLocatorSnapshotV1,
) -> Result<(), StorageError> {
    let encoded = snapshot.encode()?;
    let temp_path = locator_snapshot_temp_path(database);
    let final_path = locator_snapshot_path(database);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp_path)
        .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
    if file.write_all(&encoded).is_err() || file.sync_all().is_err() {
        drop(file);
        let _ = std::fs::remove_file(&temp_path);
        return Err(storage_error(StorageErrorKind::Unavailable));
    }
    drop(file);
    if std::fs::rename(&temp_path, &final_path).is_err() {
        let _ = std::fs::remove_file(&temp_path);
        return Err(storage_error(StorageErrorKind::Unavailable));
    }
    if let Some(parent) = final_path.parent()
        && !parent.as_os_str().is_empty()
        && let Ok(dir) = std::fs::File::open(parent)
    {
        let _ = dir.sync_all();
    }
    Ok(())
}

/// Best-effort persistence of `<db>.locator.riffsnapshot` for the current
/// checkpointed `database.redb` frontier when `TransientIndexes` are `Ready`.
pub(crate) fn persist_for_shared(shared: &SharedRedb) {
    let _ = try_persist_for_shared(shared);
}

fn try_persist_for_shared(shared: &SharedRedb) -> Result<(), StorageError> {
    if !shared.is_native_file_backend_for_snapshot() {
        return Ok(());
    }
    let generation = {
        let state = shared
            .transient_indexes_read_for_snapshot()
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        let TransientIndexState::Ready(indexes) = &*state else {
            return Ok(());
        };
        indexes.locator_generation()?
    };
    let read = shared.database.begin_read().map_err(transaction_error)?;
    let (database_id, history_incarnation, registry_digest) = {
        let meta = read.open_table(META).map_err(table_error)?;
        let db_bytes = meta
            .get(META_DATABASE_ID)
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        let history_bytes = meta
            .get(META_HISTORY_INCARNATION)
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        let registry_bytes = meta
            .get(META_RECORD_REGISTRY)
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        (
            *decode_database_identity_v1(db_bytes.value())
                .map_err(crate::error::codec_error)?
                .value(),
            *decode_history_incarnation_v1(history_bytes.value())
                .map_err(crate::error::codec_error)?
                .value(),
            *decode_record_registry_digest(registry_bytes.value())
                .map_err(crate::error::codec_error)?
                .value(),
        )
    };
    generation.validate_database_incarnation(database_id, history_incarnation)?;
    let retention_watermark = crate::retention::load_watermark(&read)?
        .and_then(|watermark| CommitSequence::new(watermark.watermark_sequence()));
    let checkpoint_application_sequence = read_commit_tail(&read)?;
    let checkpoint_administration_sequence = read_administration_tail(&read)?;
    drop(read);
    if let Some(c_seq) = checkpoint_application_sequence {
        let covered_by_retention = retention_watermark.is_some_and(|w| w >= c_seq);
        let covered_by_generation = generation.coverage().is_some_and(|cov| cov >= c_seq);
        if !covered_by_retention && !covered_by_generation {
            return Ok(());
        }
    }
    let (tail_segment, segments) =
        generation.snapshot_segments_through(checkpoint_application_sequence)?;
    if let Some(last) = segments.last() {
        if Some(last.last) != checkpoint_application_sequence {
            return Ok(());
        }
    } else if let Some(c_seq) = checkpoint_application_sequence
        && !retention_watermark.is_some_and(|w| w >= c_seq)
    {
        return Ok(());
    }
    let snapshot = RiffLocatorSnapshotV1 {
        database_id,
        history_incarnation,
        registry_digest,
        retention_watermark,
        checkpoint_application_sequence,
        checkpoint_administration_sequence,
        tail_segment,
        segments,
    };
    write_locator_snapshot_atomic(shared.database_path_for_snapshot(), &snapshot)
}

fn record_snapshot_fallback(_reason: &'static str) {}

/// Attempts to populate `LocatorCapability::Complete` and `TransientIndexes`
/// synchronously at startup by loading `<db>.locator.riffsnapshot` up to `C`
/// and folding only the uncheckpointed `COMMITS` suffix `(C, P]`.
///
/// Any missing, truncated, CRC32C-invalid, or lineage-mismatched sidecar is
/// ignored (`Ok(false)`), allowing the caller to fall back to the admitted
/// background `locator_builder`.
pub(crate) fn try_complete_startup_from_snapshot(
    shared: &Arc<SharedRedb>,
    access: &RedbReadAccess,
) -> Result<bool, StorageError> {
    if !shared.is_native_file_backend_for_snapshot() {
        return Ok(false);
    }
    let path = locator_snapshot_path(shared.database_path_for_snapshot());
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(_) => {
            record_snapshot_fallback("io_read_failed");
            return Ok(false);
        }
    };
    let snapshot = match RiffLocatorSnapshotV1::decode(&bytes) {
        Ok(snapshot) => snapshot,
        Err(reason) => {
            record_snapshot_fallback(reason);
            return Ok(false);
        }
    };
    let (database_id, history_incarnation, registry_digest) =
        access.locator_capability().source_binding()?;
    let current_watermark = CommitSequence::new(shared.retention_watermark());
    let application_frontier = access.application_frontier()?;
    let administration_frontier = access.administration_frontier()?;
    if snapshot.database_id != database_id
        || snapshot.history_incarnation != history_incarnation
        || snapshot.registry_digest != registry_digest
        || snapshot.retention_watermark != current_watermark
        || snapshot.checkpoint_application_sequence > application_frontier
        || snapshot.checkpoint_administration_sequence > administration_frontier
    {
        record_snapshot_fallback("lineage_or_frontier_mismatch");
        return Ok(false);
    }
    if let Err(reason) = verify_snapshot_tail_against_access(&snapshot, access) {
        record_snapshot_fallback(reason);
        return Ok(false);
    }
    let capability = access.locator_capability();
    let owner = capability.construction_owner()?;
    let (indexes, _suffix_rows) =
        match TransientIndexes::rebuild_from_snapshot_and_suffix_with_owner(
            access,
            &snapshot,
            owner,
            LocatorAllocationClass::Ordinary,
            || false,
        ) {
            Ok(built) => built,
            Err(_) => {
                record_snapshot_fallback("suffix_fold_failed");
                return Ok(false);
            }
        };
    let generation = indexes.locator_generation()?;
    if generation.validate_source(access).is_err() {
        record_snapshot_fallback("source_validation_failed");
        return Ok(false);
    }
    capability.install_generation(generation)?;
    shared.install_snapshot_completed_indexes(access, capability, indexes)?;
    if snapshot.checkpoint_application_sequence != application_frontier
        || snapshot.checkpoint_administration_sequence != administration_frontier
    {
        persist_for_shared(shared);
    }
    Ok(true)
}

fn verify_snapshot_tail_against_access(
    snapshot: &RiffLocatorSnapshotV1,
    access: &RedbReadAccess,
) -> Result<(), &'static str> {
    match snapshot.segments.last() {
        Some(last) => {
            if snapshot.tail_segment != Some((last.first, last.digest)) {
                return Err("tail_segment_mismatch");
            }
            if Some(last.last) != snapshot.checkpoint_application_sequence {
                return Err("tail_sequence_mismatch");
            }
            let key = crate::keys::encode_application_sequence_key(last.first);
            let Some(bytes) = access
                .read_value(JournalTable::Commits, key.as_slice())
                .map_err(|_| "tail_commit_lookup_failed")?
            else {
                return Err("tail_commit_row_missing");
            };
            match crate::history_stub::segment_summary(&bytes)
                .map_err(|_| "tail_stub_decode_failed")?
            {
                Some(summary) => {
                    if summary.first_commit_sequence != Some(last.first)
                        || summary.last_commit_sequence != Some(last.last)
                        || summary.segment_digest != *last.digest.as_bytes()
                    {
                        return Err("tail_stub_summary_mismatch");
                    }
                }
                None => {
                    let decoded = crate::history_stub::decode_segment_value(&bytes)
                        .map_err(|_| "tail_segment_decode_failed")?;
                    let segment = decoded.value();
                    if segment.first_commit_sequence() != last.first
                        || segment.last_commit_sequence() != last.last
                        || segment.segment_digest() != last.digest
                    {
                        return Err("tail_segment_header_mismatch");
                    }
                }
            }
            Ok(())
        }
        None => {
            if snapshot.tail_segment.is_some() {
                return Err("unexpected_tail_segment");
            }
            if let Some(c_seq) = snapshot.checkpoint_application_sequence
                && !snapshot.retention_watermark.is_some_and(|w| w >= c_seq)
            {
                return Err("missing_segments_for_checkpoint_frontier");
            }
            Ok(())
        }
    }
}
