//! Versioned local durability-frame encoding for the standard application profile.
#![allow(
    dead_code,
    reason = "WP-478 lands the closed frame and lane boundary before production coordinator wiring"
)]
#![expect(
    clippy::expect_used,
    clippy::unreachable,
    reason = "validated journal frames have closed tags, bounded widths, and complete segment payloads"
)]

#[path = "journal_offline_integrity.rs"]
mod offline_integrity;
#[path = "journal_recovery_memory.rs"]
mod recovery_memory;
#[path = "journal_recovery_stream.rs"]
mod recovery_stream;

use crate::history_stub::StubLocator;
use std::collections::BTreeMap;
use std::fs::File;
use std::io;
#[cfg(not(unix))]
use std::io::{Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

use redb::{
    Database, Durability, ReadTransaction, ReadableDatabase, ReadableTable, TableDefinition,
};

use riffdb_storage_api::{
    DURABILITY_JOURNAL_EXTENT_VERSION as EXTENT_FORMAT_VERSION,
    DURABILITY_JOURNAL_FRAME_VERSION as FORMAT_VERSION,
    DURABILITY_JOURNAL_FRAME_VERSION_V3 as FRAME_VERSION_V3,
};
use riffdb_types::{AdministrationSequence, CommitSequence, DatabaseId};

use crate::media::{JournalMedia, MediaFile, RealJournalMedia};

use crate::keys::{decode_application_sequence_key, decode_audit_key};
use crate::layout::{
    AUDIT, AUDIT_BY_REQUEST, COMMITS, ENTITIES, ENTITY_CHAIN_HEADS, EVENT_ROUTES, EVENTS,
    IDEMPOTENCY, IDEMPOTENCY_PENDING, INDEX_EPOCHS, META, PROVENANCE, SECONDARY_INDEXES,
    VECTOR_EVIDENCE, VECTOR_EVIDENCE_INDEX, VECTOR_OBSERVATIONS,
};

const FILE_MAGIC: [u8; 8] = *b"RDBJRN01";
const FRAME_MAGIC: [u8; 8] = *b"RDBFRM01";
const FOOTER_MAGIC: [u8; 8] = *b"RDBEND01";
const EXTENT_MAGIC: [u8; 8] = *b"RDBJEX03";
const EXTENT_FRAME_MAGIC: [u8; 8] = *b"RDBJPF03";
const EXTENT_FRAME_FOOTER_MAGIC: [u8; 8] = *b"RDBJPE03";
/// Physical frame format inside an extent. It is independent of the extent
/// header format: header version 4 changed only the header slots.
const EXTENT_FRAME_VERSION: u16 = 3;
/// Header version 3: no revision and no anchor; every frame is suffix.
const EXTENT_HEADER_VERSION_V3: u16 = 3;
/// Magic of a sealed extent's footer, written at its anchor (ADR-0263).
const SEALED_FOOTER_MAGIC: [u8; 8] = *b"RDBJSF04";
const EXTENT_HEADER_SLOT_BYTES: usize = 4 * 1024;
const EXTENT_HEADER_SLOT_COUNT: usize = 2;
const EXTENT_DATA_OFFSET: usize = EXTENT_HEADER_SLOT_BYTES * EXTENT_HEADER_SLOT_COUNT;
/// Data capacity of every extent written so far. Each extent stamps its own
/// capacity, so recovery reads it whatever the configured size, and the open
/// path replaces a recovered extent of another size with one of the configured
/// capacity (`normalize_extent_capacity_with_media`).
pub(crate) const LEGACY_EXTENT_DATA_BYTES: usize = 40 * 1024 * 1024;
/// ADR-0263 Decision 7's extent capacity, configured together with the raised
/// suffix ceilings now that command history has left redb.
pub(crate) const PRODUCTION_EXTENT_DATA_BYTES: usize = 320 * 1024 * 1024;
/// Configured extent data capacity. Test builds keep the legacy capacity:
/// every test database zero-fills an active and a spare extent, and the
/// physical ceiling then selects checkpoints before the suffix ceilings do.
#[cfg(not(any(test, feature = "test-fixtures")))]
pub(crate) const EXTENT_DATA_BYTES: usize = PRODUCTION_EXTENT_DATA_BYTES;
#[cfg(any(test, feature = "test-fixtures"))]
pub(crate) const EXTENT_DATA_BYTES: usize = LEGACY_EXTENT_DATA_BYTES;
const EXTENT_FILE_BYTES: usize = EXTENT_DATA_OFFSET + EXTENT_DATA_BYTES;

const fn extent_file_bytes(data_bytes: usize) -> usize {
    EXTENT_DATA_OFFSET + data_bytes
}

/// Where a frame lands in its extent when `used_bytes` data bytes precede it.
pub(crate) fn frame_placement_after(
    used_bytes: usize,
    frame: &EncodedJournalFrame,
) -> Option<FramePlacement> {
    Some(FramePlacement {
        position: EXTENT_DATA_OFFSET.checked_add(used_bytes)?,
        padded_len: extent_frame_bytes(frame.as_bytes().len())?,
        frame_hash: frame.frame_hash(),
    })
}

/// Data bytes an extent already holds when its complete tail is at
/// `complete_bytes` (zero for an absent extent).
pub(crate) fn extent_used_bytes(complete_bytes: usize) -> usize {
    complete_bytes.saturating_sub(EXTENT_DATA_OFFSET)
}

fn known_extent_data_bytes(data_bytes: usize) -> bool {
    data_bytes == EXTENT_DATA_BYTES
        || data_bytes == LEGACY_EXTENT_DATA_BYTES
        || data_bytes == PRODUCTION_EXTENT_DATA_BYTES
}
/// Most zero-fill bytes an extent preparation leaves unsynced.
///
/// Preparation runs beside the live journal on the same device. Buffering a
/// whole 320 MiB extent and syncing it once queues the entire extent ahead of
/// the next journal `fdatasync`, which then waits behind it: on the
/// development host a concurrent 136 KiB journal sync stalled for up to 177 ms
/// in isolation (WP-814 attributes the remaining ~0.7 s writer maximum to this
/// fill). Syncing every 2 MiB bounds that queue to one chunk (worst concurrent
/// sync 7.8 ms there) for about 10% longer preparation, which runs off the
/// writer path. Durability is unchanged: each chunk sync only precedes the
/// existing final sync.
const EXTENT_ZERO_FILL_SYNC_BYTES: usize = 2 * 1024 * 1024;
const EXTENT_FRAME_ALIGNMENT: usize = 4 * 1024;
const EXTENT_FRAME_HEADER_BYTES: usize = 128;
const EXTENT_FRAME_FOOTER_BYTES: usize = 64;
const EXTENT_HEADER_CHECKSUM_OFFSET: usize = EXTENT_HEADER_SLOT_BYTES - HASH_BYTES;
const HASH_BYTES: usize = 32;
const FILE_HEADER_BYTES: usize = 8 + 2 + 16 + 8 + 8 + HASH_BYTES + HASH_BYTES;
/// Fixed header of a version 2 logical frame (and every legacy journal frame).
const FRAME_HEADER_BYTES_V2: usize =
    8 + 2 + 16 + 1 + 1 + 8 + 8 + 8 + 8 + 2 + 2 + 2 + 2 + 4 + 4 + HASH_BYTES;
/// Version 3 appends the frame's changelog binding (ADR-0263 Amendment 2 B4):
/// presence flag and padding, history incarnation, predecessor and assigned
/// changelog sequences, prior history hash, and catalog digest.
const FRAME_BINDING_BYTES: usize = 8 + 8 + 8 + 8 + HASH_BYTES + HASH_BYTES;
/// Fixed header of the current (version 3) logical frame.
const FRAME_HEADER_BYTES: usize = FRAME_HEADER_BYTES_V2 + FRAME_BINDING_BYTES;
const FRAME_VERSION_V2: u16 = 2;
const FRAME_VERSION_V4: u16 = 4;
const FRAME_VERSION_V5: u16 = 5;
const FRAME_FOOTER_BYTES: usize = 8 + 8 + HASH_BYTES;
const MUTATION_HEADER_BYTES: usize = 1 + 1 + 1 + 1 + 4 + 4 + HASH_BYTES;

pub(crate) const MAX_JOURNAL_TRANSITIONS: usize = 256;
pub(crate) const MAX_JOURNAL_COMMANDS: usize = MAX_JOURNAL_TRANSITIONS;
/// Maximum complete published-plus-unpublished recovery suffix retained before
/// redb checkpoint reclamation. The tighter 256-transition limit above still
/// governs one frame flush and all unpublished work; this independent cap
/// prevents already-published small frames from forcing byte-inefficient redb
/// checkpoints while keeping restart CPU bounded.
pub(crate) const MAX_JOURNAL_SUFFIX_TRANSITIONS: usize = 65_536;
pub(crate) const MAX_JOURNAL_FRAME_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_JOURNAL_SUFFIX_BYTES: usize = 256 * 1024 * 1024;

mod source_suffix_scan;
pub(crate) use recovery_memory::admit_native_frame as admit_native_recovery_frame;
pub(crate) use source_suffix_scan::{
    SOURCE_SUFFIX_BYTES, SOURCE_SUFFIX_TRANSITIONS, scan_online_source_suffix_with_media,
};
const DELETE_VALUE_LENGTH: u32 = u32::MAX;

static COMMAND_FRAME_COUNT: AtomicU64 = AtomicU64::new(0);
static COMMAND_FRAME_COMMANDS: AtomicU64 = AtomicU64::new(0);
static COMMAND_FRAME_SELECTED_BYTES: AtomicU64 = AtomicU64::new(0);
static COMMAND_FRAME_RAW_EQUIVALENT_BYTES: AtomicU64 = AtomicU64::new(0);
static COMMAND_SEGMENT_SELECTED_BYTES: AtomicU64 = AtomicU64::new(0);
static COMMAND_SEGMENT_RAW_BYTES: AtomicU64 = AtomicU64::new(0);
static COMMAND_FLUSH_COUNT: AtomicU64 = AtomicU64::new(0);
static COMMAND_FLUSH_FRAMES: AtomicU64 = AtomicU64::new(0);
static COMMAND_FLUSH_COMMANDS: AtomicU64 = AtomicU64::new(0);
static COMMAND_FLUSH_BYTES: AtomicU64 = AtomicU64::new(0);
static COMMAND_FLUSH_MAX_FRAMES: AtomicU64 = AtomicU64::new(0);
static COMMAND_FLUSH_IO_MICROS: AtomicU64 = AtomicU64::new(0);
static COMMAND_FLUSH_MAX_IO_MICROS: AtomicU64 = AtomicU64::new(0);
static COMMAND_QUEUE_OBSERVATIONS: AtomicU64 = AtomicU64::new(0);
static COMMAND_QUEUE_MICROS: AtomicU64 = AtomicU64::new(0);
static COMMAND_QUEUE_MAX_MICROS: AtomicU64 = AtomicU64::new(0);
static COMMAND_ENCODE_MICROS: AtomicU64 = AtomicU64::new(0);
static COMMAND_ENCODE_MAX_MICROS: AtomicU64 = AtomicU64::new(0);
static COMMAND_WRITE_MICROS: AtomicU64 = AtomicU64::new(0);
static COMMAND_WRITE_MAX_MICROS: AtomicU64 = AtomicU64::new(0);
static COMMAND_SYNC_MICROS: AtomicU64 = AtomicU64::new(0);
static COMMAND_SYNC_MAX_MICROS: AtomicU64 = AtomicU64::new(0);

fn saturating_atomic_add(target: &AtomicU64, value: usize) {
    let value = u64::try_from(value).unwrap_or(u64::MAX);
    let _ = target.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

fn record_duration(sum: &AtomicU64, maximum: &AtomicU64, elapsed: Duration) {
    let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
    let _ = sum.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(micros))
    });
    let _ = maximum.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.max(micros))
    });
}

fn record_command_frame_census(frame: &EncodedJournalFrame) {
    if frame.command_count() == 0 {
        return;
    }
    saturating_atomic_add(&COMMAND_FRAME_COUNT, 1);
    saturating_atomic_add(&COMMAND_FRAME_COMMANDS, usize::from(frame.command_count()));
    saturating_atomic_add(&COMMAND_FRAME_SELECTED_BYTES, frame.as_bytes().len());
    saturating_atomic_add(
        &COMMAND_FRAME_RAW_EQUIVALENT_BYTES,
        frame.raw_equivalent_frame_bytes(),
    );
    saturating_atomic_add(
        &COMMAND_SEGMENT_SELECTED_BYTES,
        frame.selected_command_segment_bytes(),
    );
    saturating_atomic_add(
        &COMMAND_SEGMENT_RAW_BYTES,
        frame.raw_command_segment_bytes(),
    );
}

pub(crate) fn command_frame_census() -> [u64; 6] {
    [
        COMMAND_FRAME_COUNT.load(Ordering::Relaxed),
        COMMAND_FRAME_COMMANDS.load(Ordering::Relaxed),
        COMMAND_FRAME_SELECTED_BYTES.load(Ordering::Relaxed),
        COMMAND_FRAME_RAW_EQUIVALENT_BYTES.load(Ordering::Relaxed),
        COMMAND_SEGMENT_SELECTED_BYTES.load(Ordering::Relaxed),
        COMMAND_SEGMENT_RAW_BYTES.load(Ordering::Relaxed),
    ]
}

fn record_command_flush_census(
    batch: &[JournalSubmission],
    encode_started: Instant,
    encode_elapsed: Duration,
    write_elapsed: Duration,
    sync_elapsed: Duration,
) {
    let mut frames = 0_usize;
    let mut commands = 0_usize;
    let mut bytes = 0_usize;
    for submission in batch {
        let command_count = usize::from(submission.frame.command_count());
        if command_count == 0 {
            continue;
        }
        frames = frames.saturating_add(1);
        commands = commands.saturating_add(command_count);
        bytes = bytes.saturating_add(submission.frame.as_bytes().len());
    }
    if frames == 0 {
        return;
    }
    for submission in batch
        .iter()
        .filter(|submission| submission.frame.command_count() > 0)
    {
        saturating_atomic_add(&COMMAND_QUEUE_OBSERVATIONS, 1);
        record_duration(
            &COMMAND_QUEUE_MICROS,
            &COMMAND_QUEUE_MAX_MICROS,
            encode_started
                .checked_duration_since(submission.submitted_at)
                .unwrap_or_default(),
        );
    }
    saturating_atomic_add(&COMMAND_FLUSH_COUNT, 1);
    saturating_atomic_add(&COMMAND_FLUSH_FRAMES, frames);
    saturating_atomic_add(&COMMAND_FLUSH_COMMANDS, commands);
    saturating_atomic_add(&COMMAND_FLUSH_BYTES, bytes);
    let frames = u64::try_from(frames).unwrap_or(u64::MAX);
    let _ =
        COMMAND_FLUSH_MAX_FRAMES.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.max(frames))
        });
    let io_elapsed = encode_elapsed
        .saturating_add(write_elapsed)
        .saturating_add(sync_elapsed);
    let io_micros = u64::try_from(io_elapsed.as_micros()).unwrap_or(u64::MAX);
    let _ = COMMAND_FLUSH_IO_MICROS.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(io_micros))
    });
    let _ =
        COMMAND_FLUSH_MAX_IO_MICROS.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.max(io_micros))
        });
    record_duration(
        &COMMAND_ENCODE_MICROS,
        &COMMAND_ENCODE_MAX_MICROS,
        encode_elapsed,
    );
    record_duration(
        &COMMAND_WRITE_MICROS,
        &COMMAND_WRITE_MAX_MICROS,
        write_elapsed,
    );
    record_duration(&COMMAND_SYNC_MICROS, &COMMAND_SYNC_MAX_MICROS, sync_elapsed);
}

pub(crate) fn command_flush_census() -> [u64; 7] {
    [
        COMMAND_FLUSH_COUNT.load(Ordering::Relaxed),
        COMMAND_FLUSH_FRAMES.load(Ordering::Relaxed),
        COMMAND_FLUSH_COMMANDS.load(Ordering::Relaxed),
        COMMAND_FLUSH_BYTES.load(Ordering::Relaxed),
        COMMAND_FLUSH_MAX_FRAMES.load(Ordering::Relaxed),
        COMMAND_FLUSH_IO_MICROS.load(Ordering::Relaxed),
        COMMAND_FLUSH_MAX_IO_MICROS.load(Ordering::Relaxed),
    ]
}

pub(crate) fn command_journal_stage_census() -> [u64; 10] {
    [
        COMMAND_QUEUE_OBSERVATIONS.load(Ordering::Relaxed),
        COMMAND_QUEUE_MICROS.load(Ordering::Relaxed),
        COMMAND_QUEUE_MAX_MICROS.load(Ordering::Relaxed),
        COMMAND_FLUSH_COUNT.load(Ordering::Relaxed),
        COMMAND_ENCODE_MICROS.load(Ordering::Relaxed),
        COMMAND_ENCODE_MAX_MICROS.load(Ordering::Relaxed),
        COMMAND_WRITE_MICROS.load(Ordering::Relaxed),
        COMMAND_WRITE_MAX_MICROS.load(Ordering::Relaxed),
        COMMAND_SYNC_MICROS.load(Ordering::Relaxed),
        COMMAND_SYNC_MAX_MICROS.load(Ordering::Relaxed),
    ]
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
#[repr(u8)]
pub(crate) enum JournalTable {
    Meta = 1,
    Entities = 2,
    SecondaryIndexes = 3,
    IndexEpochs = 4,
    Idempotency = 5,
    IdempotencyPending = 6,
    Events = 7,
    EventRoutes = 8,
    Outbox = 9,
    Provenance = 10,
    Commits = 11,
    Audit = 12,
    AuditByRequest = 13,
    EntityChainHeads = 14,
    VectorEvidence = 15,
    VectorObservations = 16,
    VectorEvidenceIndex = 17,
    /// ADR-0165 command-derived locator tables. Additive tags: an older journal
    /// never carries them, so existing extents decode unchanged.
    IdempotencyLocators = 18,
    ProvenanceLocators = 19,
    AuditByRequestLocators = 20,
    HistoryTombstones = 21,
    OutboxStatus = 22,
}

impl JournalTable {
    pub(crate) const ALL: [Self; 22] = [
        Self::Meta,
        Self::Entities,
        Self::SecondaryIndexes,
        Self::IndexEpochs,
        Self::Idempotency,
        Self::IdempotencyPending,
        Self::Events,
        Self::EventRoutes,
        Self::Outbox,
        Self::Provenance,
        Self::Commits,
        Self::Audit,
        Self::AuditByRequest,
        Self::EntityChainHeads,
        Self::VectorEvidence,
        Self::VectorObservations,
        Self::VectorEvidenceIndex,
        Self::IdempotencyLocators,
        Self::ProvenanceLocators,
        Self::AuditByRequestLocators,
        Self::HistoryTombstones,
        Self::OutboxStatus,
    ];

    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Meta => "meta",
            Self::Entities => "entities",
            Self::SecondaryIndexes => "secondary_indexes",
            Self::IndexEpochs => "index_epochs",
            Self::Idempotency => "idempotency",
            Self::IdempotencyPending => "idempotency_pending",
            Self::Events => "events",
            Self::EventRoutes => "event_routes",
            Self::Outbox => "outbox",
            Self::Provenance => "provenance",
            Self::Commits => "commits",
            Self::Audit => "audit",
            Self::AuditByRequest => "audit_by_request",
            Self::EntityChainHeads => "entity_chain_heads",
            Self::VectorEvidence => "vector_evidence",
            Self::VectorObservations => "vector_observations",
            Self::VectorEvidenceIndex => "vector_evidence_index",
            Self::IdempotencyLocators => "idempotency_locators",
            Self::ProvenanceLocators => "provenance_locators",
            Self::AuditByRequestLocators => "audit_by_request_locators",
            Self::HistoryTombstones => "history_tombstones",
            Self::OutboxStatus => "outbox_status",
        }
    }

    fn decode(value: u8) -> Result<Self, JournalCodecError> {
        match value {
            1 => Ok(Self::Meta),
            2 => Ok(Self::Entities),
            3 => Ok(Self::SecondaryIndexes),
            4 => Ok(Self::IndexEpochs),
            5 => Ok(Self::Idempotency),
            6 => Ok(Self::IdempotencyPending),
            7 => Ok(Self::Events),
            8 => Ok(Self::EventRoutes),
            9 => Ok(Self::Outbox),
            10 => Ok(Self::Provenance),
            11 => Ok(Self::Commits),
            12 => Ok(Self::Audit),
            13 => Ok(Self::AuditByRequest),
            14 => Ok(Self::EntityChainHeads),
            15 => Ok(Self::VectorEvidence),
            16 => Ok(Self::VectorObservations),
            17 => Ok(Self::VectorEvidenceIndex),
            18 => Ok(Self::IdempotencyLocators),
            19 => Ok(Self::ProvenanceLocators),
            20 => Ok(Self::AuditByRequestLocators),
            21 => Ok(Self::HistoryTombstones),
            22 => Ok(Self::OutboxStatus),
            _ => Err(JournalCodecError::UnknownTable),
        }
    }

    pub(crate) const fn composite(self) -> riffdb_storage_api::CompositeTableV1 {
        match self {
            Self::HistoryTombstones => riffdb_storage_api::CompositeTableV1::HistoryTombstones,
            Self::OutboxStatus => riffdb_storage_api::CompositeTableV1::OutboxStatus,
            Self::Meta => riffdb_storage_api::CompositeTableV1::Meta,
            Self::Entities => riffdb_storage_api::CompositeTableV1::Entities,
            Self::SecondaryIndexes => riffdb_storage_api::CompositeTableV1::SecondaryIndexes,
            Self::IndexEpochs => riffdb_storage_api::CompositeTableV1::IndexEpochs,
            Self::Idempotency => riffdb_storage_api::CompositeTableV1::Idempotency,
            Self::IdempotencyPending => riffdb_storage_api::CompositeTableV1::IdempotencyPending,
            Self::Events => riffdb_storage_api::CompositeTableV1::Events,
            Self::EventRoutes => riffdb_storage_api::CompositeTableV1::EventRoutes,
            Self::Outbox => riffdb_storage_api::CompositeTableV1::Outbox,
            Self::Provenance => riffdb_storage_api::CompositeTableV1::Provenance,
            Self::Commits => riffdb_storage_api::CompositeTableV1::Commits,
            Self::Audit => riffdb_storage_api::CompositeTableV1::Audit,
            Self::AuditByRequest => riffdb_storage_api::CompositeTableV1::AuditByRequest,
            Self::EntityChainHeads => riffdb_storage_api::CompositeTableV1::EntityChainHeads,
            Self::VectorEvidence => riffdb_storage_api::CompositeTableV1::VectorEvidence,
            Self::VectorObservations => riffdb_storage_api::CompositeTableV1::VectorObservations,
            Self::VectorEvidenceIndex => riffdb_storage_api::CompositeTableV1::VectorEvidenceIndex,
            Self::IdempotencyLocators => riffdb_storage_api::CompositeTableV1::IdempotencyLocators,
            Self::ProvenanceLocators => riffdb_storage_api::CompositeTableV1::ProvenanceLocators,
            Self::AuditByRequestLocators => {
                riffdb_storage_api::CompositeTableV1::AuditByRequestLocators
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum JournalFrameKind {
    Command = 1,
    ServiceAudit = 2,
    RetentionPrune = 3,
    SourceReclamation = 4,
}

impl JournalFrameKind {
    fn decode(value: u8) -> Result<Self, JournalCodecError> {
        match value {
            1 => Ok(Self::Command),
            2 => Ok(Self::ServiceAudit),
            3 => Ok(Self::RetentionPrune),
            4 => Ok(Self::SourceReclamation),
            _ => Err(JournalCodecError::UnknownVersion),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum JournalMutation {
    Put {
        table: JournalTable,
        key: Box<[u8]>,
        expected_hash: Option<[u8; HASH_BYTES]>,
        value: Box<[u8]>,
    },
    Delete {
        table: JournalTable,
        key: Box<[u8]>,
        expected_hash: [u8; HASH_BYTES],
    },
}

impl JournalMutation {
    pub(crate) fn put(
        table: JournalTable,
        key: impl Into<Box<[u8]>>,
        value: impl Into<Box<[u8]>>,
    ) -> Result<Self, JournalCodecError> {
        let key = key.into();
        let value = value.into();
        validate_component(&key)?;
        validate_component(&value)?;
        Ok(Self::Put {
            table,
            key,
            expected_hash: None,
            value,
        })
    }

    pub(crate) fn replace(
        table: JournalTable,
        key: impl Into<Box<[u8]>>,
        expected: &[u8],
        value: impl Into<Box<[u8]>>,
    ) -> Result<Self, JournalCodecError> {
        let key = key.into();
        let value = value.into();
        validate_component(&key)?;
        validate_component(expected)?;
        validate_component(&value)?;
        Ok(Self::Put {
            table,
            key,
            expected_hash: Some(digest(expected)),
            value,
        })
    }

    pub(crate) fn delete_matching(
        table: JournalTable,
        key: impl Into<Box<[u8]>>,
        expected: &[u8],
    ) -> Result<Self, JournalCodecError> {
        let key = key.into();
        validate_component(&key)?;
        validate_component(expected)?;
        Ok(Self::Delete {
            table,
            key,
            expected_hash: digest(expected),
        })
    }

    pub(crate) fn table(&self) -> JournalTable {
        match self {
            Self::Put { table, .. } | Self::Delete { table, .. } => *table,
        }
    }

    pub(crate) fn key(&self) -> &[u8] {
        match self {
            Self::Put { key, .. } | Self::Delete { key, .. } => key,
        }
    }

    pub(crate) fn value(&self) -> Option<&[u8]> {
        match self {
            Self::Put { value, .. } => Some(value),
            Self::Delete { .. } => None,
        }
    }

    pub(crate) fn expected_hash(&self) -> Option<[u8; HASH_BYTES]> {
        match self {
            Self::Put { expected_hash, .. } => *expected_hash,
            Self::Delete { expected_hash, .. } => Some(*expected_hash),
        }
    }

    pub(crate) fn composite(
        &self,
    ) -> Result<riffdb_storage_api::CompositeMutationV1, riffdb_storage_api::StorageValueError>
    {
        match self {
            Self::Put {
                table,
                key,
                expected_hash,
                value,
            } => riffdb_storage_api::CompositeMutationV1::put_checked(
                table.composite(),
                key.clone(),
                *expected_hash,
                value.clone(),
            ),
            Self::Delete {
                table,
                key,
                expected_hash,
            } => riffdb_storage_api::CompositeMutationV1::delete_checked(
                table.composite(),
                key.clone(),
                *expected_hash,
            ),
        }
    }
}

/// A frame-sized mutation encoder used by the live writer path.
///
/// The buffer reserves the durable frame header up front, so record staging
/// writes each key and value directly into its final journal allocation. This
/// avoids retaining one heap object per table mutation and avoids the sizing
/// plus second encoding walk at epoch seal. Decoding deliberately continues to
/// materialize [`JournalMutation`] values because recovery is off the request
/// hot path and benefits from the closed semantic representation.
#[derive(Debug)]
pub(crate) struct JournalMutationBuffer {
    bytes: Vec<u8>,
    mutation_count: u32,
    logical_command_put_count: u16,
    command_audit_count: u16,
    audit_put_count: u16,
    service_audit_closed: bool,
    retention_prune_shape: retention_frame::RetentionPruneShape,
    selected_command_segment_bytes: usize,
    raw_command_segment_bytes: usize,
}

impl Default for JournalMutationBuffer {
    fn default() -> Self {
        let mut bytes = Vec::with_capacity(64 * 1024);
        bytes.resize(FRAME_HEADER_BYTES, 0);
        Self {
            bytes,
            mutation_count: 0,
            logical_command_put_count: 0,
            command_audit_count: 0,
            audit_put_count: 0,
            service_audit_closed: true,
            retention_prune_shape: retention_frame::RetentionPruneShape::default(),
            selected_command_segment_bytes: 0,
            raw_command_segment_bytes: 0,
        }
    }
}

impl JournalMutationBuffer {
    pub(crate) const fn mutation_count(&self) -> u32 {
        self.mutation_count
    }

    /// Returns the exact logical frame length after the fixed footer is sealed.
    pub(crate) fn encoded_frame_len(&self) -> Result<usize, JournalCodecError> {
        self.bytes
            .len()
            .checked_add(FRAME_FOOTER_BYTES)
            .filter(|size| *size <= MAX_JOURNAL_FRAME_BYTES)
            .ok_or(JournalCodecError::LimitExceeded)
    }

    pub(crate) fn extend(
        &mut self,
        mutations: impl IntoIterator<Item = JournalMutation>,
    ) -> Result<(), JournalCodecError> {
        for mutation in mutations {
            self.push(&mutation)?;
        }
        Ok(())
    }

    /// Appends a newly sealed command segment without decoding its canonical
    /// bytes again merely to recover counts already proven by the typed
    /// staging path.
    ///
    /// The mutation encoding is byte-identical to a normal `Commits` put. The
    /// supplied count remains checked against the frame's commit and
    /// administration frontiers at seal, while startup recovery independently
    /// decodes the segment and validates the same counts before replay.
    pub(crate) fn push_command_segment(
        &mut self,
        key: impl Into<Box<[u8]>>,
        value: impl Into<Box<[u8]>>,
        command_count: usize,
        raw_envelope_bytes: usize,
    ) -> Result<(), JournalCodecError> {
        let command_count = u16::try_from(command_count)
            .ok()
            .filter(|count| *count != 0)
            .ok_or(JournalCodecError::LimitExceeded)?;
        let audit_count = command_count
            .checked_mul(2)
            .ok_or(JournalCodecError::LimitExceeded)?;
        let mutation = JournalMutation::put(JournalTable::Commits, key, value)?;
        let selected_bytes = mutation
            .value()
            .map(<[u8]>::len)
            .ok_or(JournalCodecError::InvalidValue)?;
        if raw_envelope_bytes < selected_bytes {
            return Err(JournalCodecError::InvalidValue);
        }
        self.push_with_command_counts(&mutation, Some((command_count, audit_count)))?;
        self.selected_command_segment_bytes = self
            .selected_command_segment_bytes
            .checked_add(selected_bytes)
            .ok_or(JournalCodecError::LimitExceeded)?;
        self.raw_command_segment_bytes = self
            .raw_command_segment_bytes
            .checked_add(raw_envelope_bytes)
            .ok_or(JournalCodecError::LimitExceeded)?;
        Ok(())
    }

    fn push(&mut self, mutation: &JournalMutation) -> Result<(), JournalCodecError> {
        self.push_with_command_counts(mutation, None)
    }

    fn push_with_command_counts(
        &mut self,
        mutation: &JournalMutation,
        proven_command_counts: Option<(u16, u16)>,
    ) -> Result<(), JournalCodecError> {
        let key = mutation.key();
        let value = mutation.value();
        validate_component(key)?;
        if let Some(value) = value {
            validate_component(value)?;
        }
        let added = MUTATION_HEADER_BYTES
            .checked_add(key.len())
            .and_then(|size| size.checked_add(value.map_or(0, <[u8]>::len)))
            .ok_or(JournalCodecError::LimitExceeded)?;
        if self
            .bytes
            .len()
            .checked_add(added)
            .and_then(|size| size.checked_add(FRAME_FOOTER_BYTES))
            .is_none_or(|size| size > MAX_JOURNAL_FRAME_BYTES)
        {
            return Err(JournalCodecError::LimitExceeded);
        }
        self.mutation_count = self
            .mutation_count
            .checked_add(1)
            .ok_or(JournalCodecError::LimitExceeded)?;
        if mutation.table() == JournalTable::Commits
            && let Some(value) = value
        {
            let (logical_commands, command_audits) = match proven_command_counts {
                Some(counts) => counts,
                None => (
                    logical_commands_in_commit_value(value)?,
                    logical_command_audits_in_commit_value(value)?,
                ),
            };
            self.logical_command_put_count = self
                .logical_command_put_count
                .checked_add(logical_commands)
                .ok_or(JournalCodecError::LimitExceeded)?;
            self.command_audit_count = self
                .command_audit_count
                .checked_add(command_audits)
                .ok_or(JournalCodecError::LimitExceeded)?;
        }
        if value.is_some() && mutation.table() == JournalTable::Audit {
            self.audit_put_count = self
                .audit_put_count
                .checked_add(1)
                .ok_or(JournalCodecError::LimitExceeded)?;
        }
        self.service_audit_closed &= service_audit_mutation_is_closed(mutation);
        self.retention_prune_shape.observe(mutation);
        self.bytes.push(mutation.table() as u8);
        self.bytes.push(u8::from(value.is_none()));
        self.bytes
            .push(u8::from(mutation.expected_hash().is_some()));
        self.bytes.push(0);
        self.bytes.extend_from_slice(
            &u32::try_from(key.len())
                .map_err(|_| JournalCodecError::LimitExceeded)?
                .to_be_bytes(),
        );
        self.bytes.extend_from_slice(
            &value
                .map(|bytes| u32::try_from(bytes.len()))
                .transpose()
                .map_err(|_| JournalCodecError::LimitExceeded)?
                .unwrap_or(DELETE_VALUE_LENGTH)
                .to_be_bytes(),
        );
        self.bytes
            .extend_from_slice(&mutation.expected_hash().unwrap_or([0; HASH_BYTES]));
        self.bytes.extend_from_slice(key);
        if let Some(value) = value {
            self.bytes.extend_from_slice(value);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn finish(
        mut self,
        database_id: DatabaseId,
        kind: JournalFrameKind,
        predecessor_sequence: Option<CommitSequence>,
        covered_sequence: Option<CommitSequence>,
        predecessor_administration_sequence: Option<AdministrationSequence>,
        covered_administration_sequence: Option<AdministrationSequence>,
        transition_count: u16,
        command_count: u16,
        audit_count: u16,
        previous_hash: [u8; HASH_BYTES],
        changelog: Option<FrameChangelogBinding>,
    ) -> Result<EncodedJournalFrame, JournalCodecError> {
        let payload_len = self
            .bytes
            .len()
            .checked_sub(FRAME_HEADER_BYTES)
            .ok_or(JournalCodecError::InvalidValue)?;
        let total_len = self
            .bytes
            .len()
            .checked_add(FRAME_FOOTER_BYTES)
            .ok_or(JournalCodecError::LimitExceeded)?;
        if transition_count == 0
            || usize::from(transition_count) > MAX_JOURNAL_TRANSITIONS
            || self.mutation_count == 0
            || total_len > MAX_JOURNAL_FRAME_BYTES
            || sequence_delta(
                predecessor_sequence.map(CommitSequence::get),
                covered_sequence.map(CommitSequence::get),
            ) != Some(u64::from(command_count))
            || sequence_delta(
                predecessor_administration_sequence.map(AdministrationSequence::get),
                covered_administration_sequence.map(AdministrationSequence::get),
            ) != Some(u64::from(audit_count))
            || self.logical_command_put_count != command_count
            || match kind {
                JournalFrameKind::Command => {
                    transition_count != command_count
                        || command_count == 0
                        || (self.audit_put_count != audit_count
                            && self.command_audit_count != audit_count)
                }
                JournalFrameKind::RetentionPrune => {
                    transition_count != 1
                        || command_count != 0
                        || audit_count != 0
                        || !self.retention_prune_shape.valid(covered_sequence)
                }
                JournalFrameKind::SourceReclamation => true,
                JournalFrameKind::ServiceAudit => {
                    transition_count != audit_count
                        || command_count != 0
                        || self.audit_put_count != audit_count
                        || !self.service_audit_closed
                }
            }
        {
            return Err(JournalCodecError::InvalidValue);
        }
        if changelog.is_some() {
            let count = usize::try_from(self.mutation_count)
                .map_err(|_| JournalCodecError::LimitExceeded)?;
            let payload = payload_len
                .checked_sub(
                    count
                        .checked_mul(MUTATION_HEADER_BYTES)
                        .ok_or(JournalCodecError::LimitExceeded)?,
                )
                .ok_or(JournalCodecError::InvalidValue)?;
            recovery_memory::admit_native_layout(count, payload)?;
        }
        let header = &mut self.bytes[..FRAME_HEADER_BYTES];
        header[0..8].copy_from_slice(&FRAME_MAGIC);
        header[8..10].copy_from_slice(&FORMAT_VERSION.to_be_bytes());
        header[10..26].copy_from_slice(database_id.as_bytes());
        header[26] = kind as u8;
        header[27] = 0;
        header[28..36].copy_from_slice(&encode_commit_frontier(predecessor_sequence));
        header[36..44].copy_from_slice(&encode_commit_frontier(covered_sequence));
        header[44..52].copy_from_slice(&encode_administration_frontier(
            predecessor_administration_sequence,
        ));
        header[52..60].copy_from_slice(&encode_administration_frontier(
            covered_administration_sequence,
        ));
        header[60..62].copy_from_slice(&transition_count.to_be_bytes());
        header[62..64].copy_from_slice(&command_count.to_be_bytes());
        header[64..66].copy_from_slice(&audit_count.to_be_bytes());
        header[66..68].copy_from_slice(&0_u16.to_be_bytes());
        header[68..72].copy_from_slice(&self.mutation_count.to_be_bytes());
        header[72..76].copy_from_slice(
            &u32::try_from(payload_len)
                .map_err(|_| JournalCodecError::LimitExceeded)?
                .to_be_bytes(),
        );
        header[76..108].copy_from_slice(&previous_hash);
        header[FRAME_HEADER_BYTES_V2..FRAME_HEADER_BYTES]
            .copy_from_slice(&FrameChangelogBinding::encode(changelog.as_ref()));
        let frame_hash = digest(&self.bytes);
        self.bytes.extend_from_slice(&FOOTER_MAGIC);
        self.bytes.extend_from_slice(
            &u64::try_from(total_len)
                .map_err(|_| JournalCodecError::LimitExceeded)?
                .to_be_bytes(),
        );
        self.bytes.extend_from_slice(&frame_hash);
        Ok(EncodedJournalFrame {
            kind,
            bytes: Arc::from(self.bytes),
            frame_hash,
            transition_count,
            command_count,
            audit_count,
            covered_sequence,
            covered_administration_sequence,
            selected_command_segment_bytes: self.selected_command_segment_bytes,
            raw_command_segment_bytes: self.raw_command_segment_bytes,
            raw_equivalent_frame_bytes: total_len
                .checked_sub(self.selected_command_segment_bytes)
                .and_then(|bytes| bytes.checked_add(self.raw_command_segment_bytes))
                .ok_or(JournalCodecError::LimitExceeded)?,
            records_changelog: changelog.is_some(),
        })
    }
}

fn mutation_put_count(
    mutations: &[JournalMutation],
    table: JournalTable,
) -> Result<u16, JournalCodecError> {
    u16::try_from(
        mutations
            .iter()
            .filter(|mutation| mutation.table() == table && mutation.value().is_some())
            .count(),
    )
    .map_err(|_| JournalCodecError::LimitExceeded)
}

fn logical_command_put_count(mutations: &[JournalMutation]) -> Result<u16, JournalCodecError> {
    mutations
        .iter()
        .filter(|mutation| mutation.table() == JournalTable::Commits)
        .filter_map(JournalMutation::value)
        .try_fold(0_u16, |count, value| {
            count
                .checked_add(logical_commands_in_commit_value(value)?)
                .ok_or(JournalCodecError::LimitExceeded)
        })
}

fn logical_command_audit_count(mutations: &[JournalMutation]) -> Result<u16, JournalCodecError> {
    mutations
        .iter()
        .filter(|mutation| mutation.table() == JournalTable::Commits)
        .filter_map(JournalMutation::value)
        .try_fold(0_u16, |count, value| {
            count
                .checked_add(logical_command_audits_in_commit_value(value)?)
                .ok_or(JournalCodecError::LimitExceeded)
        })
}

/// Counts the logical commands represented by one authoritative commit put.
/// Historical commit and capsule revisions occupy one row per command. A V1
/// segment occupies one row for a bounded, contiguous command group.
fn logical_commands_in_commit_value(value: &[u8]) -> Result<u16, JournalCodecError> {
    match riffdb_storage_api::decode_command_segment_v1(value) {
        Ok(segment) => u16::try_from(segment.value().commands().len())
            .map_err(|_| JournalCodecError::LimitExceeded),
        // Every pre-segment authority revision represents exactly one logical
        // command. This layer deliberately does not attempt all historical
        // decoders; startup recovery performs complete record validation after
        // the frame checksum and mutation bounds have been proven.
        Err(_) => Ok(1),
    }
}

/// Counts the command-owned service-audit members carried by a segment.
/// Historical commit rows carry no embedded audit authority.
fn logical_command_audits_in_commit_value(value: &[u8]) -> Result<u16, JournalCodecError> {
    match riffdb_storage_api::decode_command_segment_v1(value) {
        Ok(segment) => u16::try_from(segment.value().commands().len())
            .ok()
            .and_then(|commands| commands.checked_mul(2))
            .ok_or(JournalCodecError::LimitExceeded),
        Err(_) => Ok(0),
    }
}

#[path = "journal/retention_frame.rs"]
mod retention_frame;

mod source_reclamation;
pub(crate) use source_reclamation::{SourceReceiptPreimage, SourceReclamationProof};

fn service_audit_mutation_is_closed(mutation: &JournalMutation) -> bool {
    match mutation.table() {
        JournalTable::Audit | JournalTable::AuditByRequest => mutation.value().is_some(),
        JournalTable::Meta => {
            (mutation.value().is_some()
                && mutation.key() == crate::layout::META_ADMINISTRATION_SEQUENCE.as_bytes())
                // ADR-0186 adds exactly this checked physical allocator domain.
                // No journal field, table tag, or existing metadata meaning changes.
                // Successful shape validation does not authorize replay into an
                // inactive database: the checkpoint owner must validate V3 roots.
                || crate::changelog_v3::journal_allocator_assignment(mutation).is_ok()
        }
        JournalTable::Entities
        | JournalTable::SecondaryIndexes
        | JournalTable::IndexEpochs
        | JournalTable::Idempotency
        | JournalTable::IdempotencyPending
        | JournalTable::IdempotencyLocators
        | JournalTable::ProvenanceLocators
        | JournalTable::AuditByRequestLocators
        | JournalTable::Events
        | JournalTable::EventRoutes
        | JournalTable::Outbox
        | JournalTable::Provenance
        | JournalTable::Commits
        | JournalTable::EntityChainHeads
        | JournalTable::VectorEvidence
        | JournalTable::VectorObservations
        | JournalTable::VectorEvidenceIndex
        | JournalTable::HistoryTombstones
        | JournalTable::OutboxStatus => false,
    }
}

fn sequence_delta(previous: Option<u64>, covered: Option<u64>) -> Option<u64> {
    match (previous, covered) {
        (None, None) => Some(0),
        (None, Some(covered)) => Some(covered),
        (Some(previous), Some(covered)) => covered.checked_sub(previous),
        (Some(_), None) => None,
    }
}

fn encode_commit_frontier(sequence: Option<CommitSequence>) -> [u8; 8] {
    sequence.map_or(0, CommitSequence::get).to_be_bytes()
}

fn encode_administration_frontier(sequence: Option<AdministrationSequence>) -> [u8; 8] {
    sequence
        .map_or(0, AdministrationSequence::get)
        .to_be_bytes()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JournalFileHeader {
    database_id: DatabaseId,
    checkpoint_sequence: Option<CommitSequence>,
    checkpoint_administration_sequence: Option<AdministrationSequence>,
    checkpoint_frame_hash: [u8; HASH_BYTES],
}

impl JournalFileHeader {
    pub(crate) fn new(
        database_id: DatabaseId,
        checkpoint_sequence: Option<CommitSequence>,
        checkpoint_frame_hash: [u8; HASH_BYTES],
    ) -> Self {
        Self::with_frontiers(
            database_id,
            checkpoint_sequence,
            None,
            checkpoint_frame_hash,
        )
    }

    pub(crate) fn with_frontiers(
        database_id: DatabaseId,
        checkpoint_sequence: Option<CommitSequence>,
        checkpoint_administration_sequence: Option<AdministrationSequence>,
        checkpoint_frame_hash: [u8; HASH_BYTES],
    ) -> Self {
        Self {
            database_id,
            checkpoint_sequence,
            checkpoint_administration_sequence,
            checkpoint_frame_hash,
        }
    }

    pub(crate) fn encode(&self) -> [u8; FILE_HEADER_BYTES] {
        let mut bytes = [0_u8; FILE_HEADER_BYTES];
        bytes[..8].copy_from_slice(&FILE_MAGIC);
        bytes[8..10].copy_from_slice(&FORMAT_VERSION.to_be_bytes());
        bytes[10..26].copy_from_slice(self.database_id.as_bytes());
        bytes[26..34].copy_from_slice(
            &self
                .checkpoint_sequence
                .map_or(0, riffdb_types::CommitSequence::get)
                .to_be_bytes(),
        );
        bytes[34..42].copy_from_slice(
            &self
                .checkpoint_administration_sequence
                .map_or(0, riffdb_types::AdministrationSequence::get)
                .to_be_bytes(),
        );
        bytes[42..74].copy_from_slice(&self.checkpoint_frame_hash);
        let checksum = digest(&bytes[..74]);
        bytes[74..].copy_from_slice(&checksum);
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, JournalCodecError> {
        if bytes.len() != FILE_HEADER_BYTES {
            return Err(JournalCodecError::Truncated);
        }
        if bytes[..8] != FILE_MAGIC
            || !matches!(
                read_u16(bytes, 8)?,
                FRAME_VERSION_V3 | FRAME_VERSION_V4 | FRAME_VERSION_V5
            )
        {
            return Err(JournalCodecError::UnknownVersion);
        }
        if digest(&bytes[..74]).as_slice() != &bytes[74..] {
            return Err(JournalCodecError::Checksum);
        }
        let database_id = DatabaseId::from_bytes(read_array::<16>(bytes, 10)?)
            .map_err(|_| JournalCodecError::InvalidValue)?;
        let checkpoint = read_u64(bytes, 26)?;
        let checkpoint_sequence = if checkpoint == 0 {
            None
        } else {
            CommitSequence::new(checkpoint)
                .ok_or(JournalCodecError::InvalidValue)?
                .into()
        };
        let checkpoint_administration_sequence = AdministrationSequence::new(read_u64(bytes, 34)?);
        Ok(Self {
            database_id,
            checkpoint_sequence,
            checkpoint_administration_sequence,
            checkpoint_frame_hash: read_array::<HASH_BYTES>(bytes, 42)?,
        })
    }

    pub(crate) fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    pub(crate) fn checkpoint_sequence(&self) -> Option<CommitSequence> {
        self.checkpoint_sequence
    }

    pub(crate) fn checkpoint_administration_sequence(&self) -> Option<AdministrationSequence> {
        self.checkpoint_administration_sequence
    }

    pub(crate) fn checkpoint_frame_hash(&self) -> [u8; HASH_BYTES] {
        self.checkpoint_frame_hash
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExtentHeader {
    logical: JournalFileHeader,
    generation: u64,
    slot: u16,
    /// Stamped data capacity: the configured size, or the legacy size of an
    /// extent written before ADR-0263 that recovery must still read.
    data_bytes: usize,
    /// Header rewrite count within one generation. The selected slot is the
    /// highest (generation, revision); a crash while writing the other slot
    /// falls back to the previous header.
    revision: u64,
    /// Physical position of the first frame after `logical`'s checkpoint. The
    /// frames before it are materialized history that the extent retains
    /// (ADR-0263); recovery scans from here.
    anchor_position: usize,
}

impl ExtentHeader {
    fn initial(logical: JournalFileHeader) -> Self {
        Self {
            logical,
            generation: 1,
            slot: 0,
            data_bytes: EXTENT_DATA_BYTES,
            revision: 0,
            anchor_position: EXTENT_DATA_OFFSET,
        }
    }

    /// The same generation with its checkpoint anchor advanced to
    /// `anchor_position`, written to the other slot at the next revision.
    /// Frames before the anchor stay in place as retained history.
    fn anchored(
        &self,
        logical: JournalFileHeader,
        anchor_position: usize,
    ) -> Result<Self, JournalIoError> {
        if anchor_position < self.anchor_position
            || anchor_position > extent_file_bytes(self.data_bytes)
            || !anchor_position.is_multiple_of(EXTENT_FRAME_ALIGNMENT)
            || logical.database_id() != self.logical.database_id()
        {
            return Err(JournalIoError::Corrupt);
        }
        Ok(Self {
            logical,
            generation: self.generation,
            slot: if self.slot == 0 { 1 } else { 0 },
            data_bytes: self.data_bytes,
            revision: self
                .revision
                .checked_add(1)
                .ok_or(JournalIoError::Corrupt)?,
            anchor_position,
        })
    }

    fn successor(&self, logical: JournalFileHeader) -> Result<Self, JournalIoError> {
        Ok(Self {
            logical,
            generation: self
                .generation
                .checked_add(1)
                .ok_or(JournalIoError::Corrupt)?,
            slot: if self.slot == 0 { 1 } else { 0 },
            data_bytes: self.data_bytes,
            revision: 0,
            anchor_position: EXTENT_DATA_OFFSET,
        })
    }

    fn encode(&self) -> [u8; EXTENT_HEADER_SLOT_BYTES] {
        let mut bytes = [0_u8; EXTENT_HEADER_SLOT_BYTES];
        bytes[..8].copy_from_slice(&EXTENT_MAGIC);
        bytes[8..10].copy_from_slice(&EXTENT_FORMAT_VERSION.to_be_bytes());
        bytes[104..112].copy_from_slice(&self.revision.to_be_bytes());
        bytes[112..120].copy_from_slice(
            &u64::try_from(self.anchor_position)
                .expect("extent anchor is bounded")
                .to_be_bytes(),
        );
        bytes[10..12].copy_from_slice(&self.slot.to_be_bytes());
        bytes[12..16].copy_from_slice(
            &u32::try_from(EXTENT_HEADER_SLOT_BYTES)
                .expect("extent header is bounded")
                .to_be_bytes(),
        );
        bytes[16..32].copy_from_slice(self.logical.database_id().as_bytes());
        bytes[32..40].copy_from_slice(&self.generation.to_be_bytes());
        bytes[40..48].copy_from_slice(
            &self
                .logical
                .checkpoint_sequence()
                .map_or(0, CommitSequence::get)
                .to_be_bytes(),
        );
        bytes[48..56].copy_from_slice(
            &self
                .logical
                .checkpoint_administration_sequence()
                .map_or(0, AdministrationSequence::get)
                .to_be_bytes(),
        );
        bytes[56..88].copy_from_slice(&self.logical.checkpoint_frame_hash());
        bytes[88..96].copy_from_slice(
            &u64::try_from(EXTENT_DATA_OFFSET)
                .expect("extent data offset is bounded")
                .to_be_bytes(),
        );
        bytes[96..104].copy_from_slice(
            &u64::try_from(self.data_bytes)
                .expect("extent data capacity is bounded")
                .to_be_bytes(),
        );
        let checksum = digest(&bytes[..EXTENT_HEADER_CHECKSUM_OFFSET]);
        bytes[EXTENT_HEADER_CHECKSUM_OFFSET..].copy_from_slice(&checksum);
        bytes
    }

    fn decode(bytes: &[u8], expected_slot: u16) -> Result<Self, JournalCodecError> {
        if bytes.len() != EXTENT_HEADER_SLOT_BYTES {
            return Err(JournalCodecError::Truncated);
        }
        let version = read_u16(bytes, 8)?;
        if bytes[..8] != EXTENT_MAGIC
            || (version != EXTENT_FORMAT_VERSION && version != EXTENT_HEADER_VERSION_V3)
            || read_u16(bytes, 10)? != expected_slot
            || usize::try_from(read_u32(bytes, 12)?).ok() != Some(EXTENT_HEADER_SLOT_BYTES)
            || usize::try_from(read_u64(bytes, 88)?).ok() != Some(EXTENT_DATA_OFFSET)
            || !usize::try_from(read_u64(bytes, 96)?).is_ok_and(known_extent_data_bytes)
        {
            return Err(JournalCodecError::UnknownVersion);
        }
        if digest(&bytes[..EXTENT_HEADER_CHECKSUM_OFFSET]).as_slice()
            != &bytes[EXTENT_HEADER_CHECKSUM_OFFSET..]
        {
            return Err(JournalCodecError::Checksum);
        }
        let reserved_from = if version == EXTENT_HEADER_VERSION_V3 {
            104
        } else {
            120
        };
        if bytes[reserved_from..EXTENT_HEADER_CHECKSUM_OFFSET]
            .iter()
            .any(|byte| *byte != 0)
        {
            return Err(JournalCodecError::InvalidValue);
        }
        let data_bytes =
            usize::try_from(read_u64(bytes, 96)?).map_err(|_| JournalCodecError::InvalidValue)?;
        let (revision, anchor_position) = if version == EXTENT_HEADER_VERSION_V3 {
            (0, EXTENT_DATA_OFFSET)
        } else {
            let anchor_position = usize::try_from(read_u64(bytes, 112)?)
                .map_err(|_| JournalCodecError::InvalidValue)?;
            if anchor_position < EXTENT_DATA_OFFSET
                || anchor_position > extent_file_bytes(data_bytes)
                || !anchor_position.is_multiple_of(EXTENT_FRAME_ALIGNMENT)
            {
                return Err(JournalCodecError::InvalidValue);
            }
            (read_u64(bytes, 104)?, anchor_position)
        };
        let database_id = DatabaseId::from_bytes(read_array::<16>(bytes, 16)?)
            .map_err(|_| JournalCodecError::InvalidValue)?;
        let generation = read_u64(bytes, 32)?;
        if generation == 0 {
            return Err(JournalCodecError::InvalidValue);
        }
        let checkpoint = read_u64(bytes, 40)?;
        let checkpoint_sequence = if checkpoint == 0 {
            None
        } else {
            Some(CommitSequence::new(checkpoint).ok_or(JournalCodecError::InvalidValue)?)
        };
        let checkpoint_administration_sequence = AdministrationSequence::new(read_u64(bytes, 48)?);
        Ok(Self {
            logical: JournalFileHeader::with_frontiers(
                database_id,
                checkpoint_sequence,
                checkpoint_administration_sequence,
                read_array::<HASH_BYTES>(bytes, 56)?,
            ),
            generation,
            slot: expected_slot,
            data_bytes,
            revision,
            anchor_position,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExtentState {
    header: ExtentHeader,
    next_position: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JournalFrame {
    format_version: u16,
    database_id: DatabaseId,
    kind: JournalFrameKind,
    predecessor_sequence: Option<CommitSequence>,
    covered_sequence: Option<CommitSequence>,
    predecessor_administration_sequence: Option<AdministrationSequence>,
    covered_administration_sequence: Option<AdministrationSequence>,
    transition_count: u16,
    command_count: u16,
    audit_count: u16,
    previous_hash: [u8; HASH_BYTES],
    mutations: Vec<JournalMutation>,
    changelog: Option<FrameChangelogBinding>,
    source_reclamation: Option<SourceReclamationProof>,
}

/// The V3 changelog binding a journaled frame's receipt is derived with, beyond
/// the database identity and frontiers the frame header already carries. It is
/// recorded in the frame so a retained extent re-derives its receipts without
/// the history state that existed when it was written.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FrameChangelogBinding {
    history_incarnation: u64,
    predecessor: Option<riffdb_storage_api::ChangelogTransactionSequence>,
    sequence: riffdb_storage_api::ChangelogTransactionSequence,
    prior_history_hash: [u8; HASH_BYTES],
    catalog_digest: [u8; HASH_BYTES],
}

impl FrameChangelogBinding {
    pub(crate) fn new(
        binding: &riffdb_storage_api::AuthoritativeTransactionBindingV3,
        catalog_digest: [u8; HASH_BYTES],
    ) -> Self {
        Self {
            history_incarnation: binding.history_incarnation,
            predecessor: binding.predecessor,
            sequence: binding.sequence,
            prior_history_hash: binding.prior_history_hash,
            catalog_digest,
        }
    }

    pub(crate) const fn assigned_sequence(
        &self,
    ) -> riffdb_storage_api::ChangelogTransactionSequence {
        self.sequence
    }

    /// The complete binding, with identity and frontiers taken from the frame.
    pub(crate) fn binding(
        &self,
        frame: &JournalFrame,
    ) -> riffdb_storage_api::AuthoritativeTransactionBindingV3 {
        riffdb_storage_api::AuthoritativeTransactionBindingV3 {
            database_id: frame.database_id,
            history_incarnation: self.history_incarnation,
            predecessor: self.predecessor,
            sequence: self.sequence,
            predecessor_frontier: riffdb_types::DualFrontier::new(
                frame.predecessor_sequence,
                frame.predecessor_administration_sequence,
            ),
            covered_frontier: riffdb_types::DualFrontier::new(
                frame.covered_sequence,
                frame.covered_administration_sequence,
            ),
            prior_history_hash: self.prior_history_hash,
        }
    }

    pub(crate) const fn catalog_digest(&self) -> [u8; HASH_BYTES] {
        self.catalog_digest
    }

    fn encode(binding: Option<&Self>) -> [u8; FRAME_BINDING_BYTES] {
        let mut bytes = [0_u8; FRAME_BINDING_BYTES];
        if let Some(binding) = binding {
            bytes[0] = 1;
            bytes[8..16].copy_from_slice(&binding.history_incarnation.to_be_bytes());
            bytes[16..24].copy_from_slice(
                &binding
                    .predecessor
                    .map_or(0, riffdb_storage_api::ChangelogTransactionSequence::get)
                    .to_be_bytes(),
            );
            bytes[24..32].copy_from_slice(&binding.sequence.get().to_be_bytes());
            bytes[32..64].copy_from_slice(&binding.prior_history_hash);
            bytes[64..96].copy_from_slice(&binding.catalog_digest);
        }
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Option<Self>, JournalCodecError> {
        let bytes = bytes
            .get(..FRAME_BINDING_BYTES)
            .ok_or(JournalCodecError::Truncated)?;
        if bytes[1..8].iter().any(|byte| *byte != 0) {
            return Err(JournalCodecError::InvalidValue);
        }
        match bytes[0] {
            0 if bytes.iter().all(|byte| *byte == 0) => Ok(None),
            1 => {
                let history_incarnation = read_u64(bytes, 8)?;
                let sequence =
                    riffdb_storage_api::ChangelogTransactionSequence::new(read_u64(bytes, 24)?)
                        .ok_or(JournalCodecError::InvalidValue)?;
                let predecessor =
                    riffdb_storage_api::ChangelogTransactionSequence::new(read_u64(bytes, 16)?);
                if history_incarnation == 0
                    || predecessor
                        .map(riffdb_storage_api::ChangelogTransactionSequence::get)
                        .and_then(|value| value.checked_add(1))
                        .is_some_and(|next| next != sequence.get())
                {
                    return Err(JournalCodecError::InvalidValue);
                }
                Ok(Some(Self {
                    history_incarnation,
                    predecessor,
                    sequence,
                    prior_history_hash: read_array::<HASH_BYTES>(bytes, 32)?,
                    catalog_digest: read_array::<HASH_BYTES>(bytes, 64)?,
                }))
            }
            _ => Err(JournalCodecError::InvalidValue),
        }
    }
}

impl JournalFrame {
    /// Records a changelog binding on a constructed frame, for fixtures that
    /// build frames outside the ordered writer.
    #[cfg(test)]
    pub(crate) fn with_changelog(self, changelog: Option<FrameChangelogBinding>) -> Self {
        Self { changelog, ..self }
    }

    /// The recorded changelog binding, absent for a version 2 frame or a frame
    /// written before the source adopted V3 history.
    pub(crate) const fn changelog(&self) -> Option<&FrameChangelogBinding> {
        self.changelog.as_ref()
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn encode_buffered_command(
        database_id: DatabaseId,
        predecessor_sequence: Option<CommitSequence>,
        covered_sequence: Option<CommitSequence>,
        predecessor_administration_sequence: Option<AdministrationSequence>,
        covered_administration_sequence: Option<AdministrationSequence>,
        command_count: u16,
        previous_hash: [u8; HASH_BYTES],
        mutations: JournalMutationBuffer,
        changelog: Option<FrameChangelogBinding>,
    ) -> Result<EncodedJournalFrame, JournalCodecError> {
        let audit_count = sequence_delta(
            predecessor_administration_sequence.map(AdministrationSequence::get),
            covered_administration_sequence.map(AdministrationSequence::get),
        )
        .and_then(|count| u16::try_from(count).ok())
        .ok_or(JournalCodecError::LimitExceeded)?;
        mutations.finish(
            database_id,
            JournalFrameKind::Command,
            predecessor_sequence,
            covered_sequence,
            predecessor_administration_sequence,
            covered_administration_sequence,
            command_count,
            command_count,
            audit_count,
            previous_hash,
            changelog,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn encode_buffered_service_audit(
        database_id: DatabaseId,
        application_sequence: Option<CommitSequence>,
        predecessor_administration_sequence: Option<AdministrationSequence>,
        covered_administration_sequence: Option<AdministrationSequence>,
        audit_count: u16,
        previous_hash: [u8; HASH_BYTES],
        mutations: JournalMutationBuffer,
        changelog: Option<FrameChangelogBinding>,
    ) -> Result<EncodedJournalFrame, JournalCodecError> {
        mutations.finish(
            database_id,
            JournalFrameKind::ServiceAudit,
            application_sequence,
            application_sequence,
            predecessor_administration_sequence,
            covered_administration_sequence,
            audit_count,
            0,
            audit_count,
            previous_hash,
            changelog,
        )
    }

    pub(crate) fn new(
        database_id: DatabaseId,
        first_sequence: CommitSequence,
        last_sequence: CommitSequence,
        command_count: u16,
        previous_hash: [u8; HASH_BYTES],
        mutations: Vec<JournalMutation>,
    ) -> Result<Self, JournalCodecError> {
        let predecessor_sequence = if first_sequence == CommitSequence::first() {
            None
        } else {
            CommitSequence::new(first_sequence.get().saturating_sub(1))
        };
        Self::command(
            database_id,
            predecessor_sequence,
            Some(last_sequence),
            None,
            None,
            command_count,
            previous_hash,
            mutations,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn command(
        database_id: DatabaseId,
        predecessor_sequence: Option<CommitSequence>,
        covered_sequence: Option<CommitSequence>,
        predecessor_administration_sequence: Option<AdministrationSequence>,
        covered_administration_sequence: Option<AdministrationSequence>,
        command_count: u16,
        previous_hash: [u8; HASH_BYTES],
        mutations: Vec<JournalMutation>,
    ) -> Result<Self, JournalCodecError> {
        let audit_count = sequence_delta(
            predecessor_administration_sequence.map(AdministrationSequence::get),
            covered_administration_sequence.map(AdministrationSequence::get),
        )
        .and_then(|count| u16::try_from(count).ok())
        .ok_or(JournalCodecError::LimitExceeded)?;
        Self::checked(
            database_id,
            JournalFrameKind::Command,
            predecessor_sequence,
            covered_sequence,
            predecessor_administration_sequence,
            covered_administration_sequence,
            command_count,
            command_count,
            audit_count,
            previous_hash,
            mutations,
        )
    }

    pub(crate) fn service_audit(
        database_id: DatabaseId,
        application_sequence: Option<CommitSequence>,
        predecessor_administration_sequence: Option<AdministrationSequence>,
        covered_administration_sequence: Option<AdministrationSequence>,
        audit_count: u16,
        previous_hash: [u8; HASH_BYTES],
        mutations: Vec<JournalMutation>,
    ) -> Result<Self, JournalCodecError> {
        Self::checked(
            database_id,
            JournalFrameKind::ServiceAudit,
            application_sequence,
            application_sequence,
            predecessor_administration_sequence,
            covered_administration_sequence,
            audit_count,
            0,
            audit_count,
            previous_hash,
            mutations,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn checked(
        database_id: DatabaseId,
        kind: JournalFrameKind,
        predecessor_sequence: Option<CommitSequence>,
        covered_sequence: Option<CommitSequence>,
        predecessor_administration_sequence: Option<AdministrationSequence>,
        covered_administration_sequence: Option<AdministrationSequence>,
        transition_count: u16,
        command_count: u16,
        audit_count: u16,
        previous_hash: [u8; HASH_BYTES],
        mutations: Vec<JournalMutation>,
    ) -> Result<Self, JournalCodecError> {
        Self::checked_with(
            database_id,
            kind,
            predecessor_sequence,
            covered_sequence,
            predecessor_administration_sequence,
            covered_administration_sequence,
            transition_count,
            command_count,
            audit_count,
            previous_hash,
            mutations,
            true,
        )
    }

    /// `derive_counts` re-derives the command and command-audit counts from
    /// every command segment. Only a frame whose hash equals one recorded from
    /// an already validated frame may skip it (see [`Self::decode_proven`]).
    #[allow(clippy::too_many_arguments)]
    fn checked_with(
        database_id: DatabaseId,
        kind: JournalFrameKind,
        predecessor_sequence: Option<CommitSequence>,
        covered_sequence: Option<CommitSequence>,
        predecessor_administration_sequence: Option<AdministrationSequence>,
        covered_administration_sequence: Option<AdministrationSequence>,
        transition_count: u16,
        command_count: u16,
        audit_count: u16,
        previous_hash: [u8; HASH_BYTES],
        mutations: Vec<JournalMutation>,
        derive_counts: bool,
    ) -> Result<Self, JournalCodecError> {
        if transition_count == 0
            || usize::from(transition_count) > MAX_JOURNAL_TRANSITIONS
            || mutations.is_empty()
            || sequence_delta(
                predecessor_sequence.map(CommitSequence::get),
                covered_sequence.map(CommitSequence::get),
            ) != Some(u64::from(command_count))
            || sequence_delta(
                predecessor_administration_sequence.map(AdministrationSequence::get),
                covered_administration_sequence.map(AdministrationSequence::get),
            ) != Some(u64::from(audit_count))
            || (derive_counts && logical_command_put_count(&mutations)? != command_count)
            || match kind {
                JournalFrameKind::Command => {
                    transition_count != command_count
                        || command_count == 0
                        || (derive_counts
                            && mutation_put_count(&mutations, JournalTable::Audit)? != audit_count
                            && logical_command_audit_count(&mutations)? != audit_count)
                }
                JournalFrameKind::RetentionPrune => {
                    transition_count != 1
                        || command_count != 0
                        || audit_count != 0
                        || !retention_frame::retention_prune_shape_is_valid(
                            &mutations,
                            covered_sequence,
                        )
                }
                JournalFrameKind::SourceReclamation => {
                    transition_count != 1
                        || command_count != 0
                        || audit_count != 0
                        || mutations.len() != 1
                        || crate::changelog_v3::journal_allocator_assignment(&mutations[0]).is_err()
                }
                JournalFrameKind::ServiceAudit => {
                    transition_count != audit_count
                        || command_count != 0
                        || mutation_put_count(&mutations, JournalTable::Audit)? != audit_count
                        || !mutations.iter().all(service_audit_mutation_is_closed)
                }
            }
        {
            return Err(JournalCodecError::InvalidValue);
        }
        let frame = Self {
            format_version: FORMAT_VERSION,
            database_id,
            kind,
            predecessor_sequence,
            covered_sequence,
            predecessor_administration_sequence,
            covered_administration_sequence,
            transition_count,
            command_count,
            audit_count,
            previous_hash,
            mutations,
            changelog: None,
            source_reclamation: None,
        };
        if frame.encoded_len()? > MAX_JOURNAL_FRAME_BYTES {
            return Err(JournalCodecError::LimitExceeded);
        }
        Ok(frame)
    }

    pub(crate) fn encode(&self) -> Result<EncodedJournalFrame, JournalCodecError> {
        self.encode_as(self.format_version)
    }

    /// A version 2 encoding, as every legacy journal and pre-ADR-0263 extent
    /// holds. Recovery still reads these; only fixtures still write them.
    #[cfg(test)]
    pub(crate) fn encode_version_2(&self) -> Result<EncodedJournalFrame, JournalCodecError> {
        if self.changelog.is_some() {
            return Err(JournalCodecError::InvalidValue);
        }
        self.encode_as(FRAME_VERSION_V2)
    }

    fn encode_as(&self, version: u16) -> Result<EncodedJournalFrame, JournalCodecError> {
        self.validate_source_reclamation()?;
        if self.kind == JournalFrameKind::SourceReclamation && version < FRAME_VERSION_V5 {
            return Err(JournalCodecError::UnknownVersion);
        }
        let header_bytes = match version {
            FRAME_VERSION_V3 | FRAME_VERSION_V4 | FRAME_VERSION_V5 => FRAME_HEADER_BYTES,
            FRAME_VERSION_V2 => FRAME_HEADER_BYTES_V2,
            _ => return Err(JournalCodecError::UnknownVersion),
        };
        if version < FRAME_VERSION_V4
            && (self.kind == JournalFrameKind::RetentionPrune
                || self.mutations.iter().any(|m| {
                    matches!(
                        m.table(),
                        JournalTable::HistoryTombstones | JournalTable::OutboxStatus
                    )
                }))
        {
            return Err(JournalCodecError::UnknownVersion);
        }
        let payload_len = self.payload_len()?;
        let total_len = header_bytes
            .checked_add(payload_len)
            .and_then(|value| value.checked_add(FRAME_FOOTER_BYTES))
            .ok_or(JournalCodecError::LimitExceeded)?;
        if total_len > MAX_JOURNAL_FRAME_BYTES {
            return Err(JournalCodecError::LimitExceeded);
        }
        let mut bytes = Vec::with_capacity(total_len);
        bytes.extend_from_slice(&FRAME_MAGIC);
        bytes.extend_from_slice(&version.to_be_bytes());
        bytes.extend_from_slice(self.database_id.as_bytes());
        bytes.push(self.kind as u8);
        bytes.push(0);
        bytes.extend_from_slice(&encode_commit_frontier(self.predecessor_sequence));
        bytes.extend_from_slice(&encode_commit_frontier(self.covered_sequence));
        bytes.extend_from_slice(&encode_administration_frontier(
            self.predecessor_administration_sequence,
        ));
        bytes.extend_from_slice(&encode_administration_frontier(
            self.covered_administration_sequence,
        ));
        bytes.extend_from_slice(&self.transition_count.to_be_bytes());
        bytes.extend_from_slice(&self.command_count.to_be_bytes());
        bytes.extend_from_slice(&self.audit_count.to_be_bytes());
        bytes.extend_from_slice(&0_u16.to_be_bytes());
        bytes.extend_from_slice(
            &u32::try_from(self.mutations.len())
                .map_err(|_| JournalCodecError::LimitExceeded)?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(
            &u32::try_from(payload_len)
                .map_err(|_| JournalCodecError::LimitExceeded)?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(&self.previous_hash);
        if header_bytes == FRAME_HEADER_BYTES {
            bytes.extend_from_slice(&FrameChangelogBinding::encode(self.changelog.as_ref()));
        }
        if let Some(proof) = &self.source_reclamation {
            let proof = proof.encode()?;
            bytes.extend_from_slice(
                &u32::try_from(proof.len())
                    .map_err(|_| JournalCodecError::LimitExceeded)?
                    .to_be_bytes(),
            );
            bytes.extend_from_slice(&proof);
        }
        for mutation in &self.mutations {
            bytes.push(mutation.table() as u8);
            bytes.push(u8::from(mutation.value().is_none()));
            bytes.push(u8::from(mutation.expected_hash().is_some()));
            bytes.push(0);
            bytes.extend_from_slice(
                &u32::try_from(mutation.key().len())
                    .map_err(|_| JournalCodecError::LimitExceeded)?
                    .to_be_bytes(),
            );
            let value_len = mutation
                .value()
                .map(|value| u32::try_from(value.len()))
                .transpose()
                .map_err(|_| JournalCodecError::LimitExceeded)?
                .unwrap_or(DELETE_VALUE_LENGTH);
            bytes.extend_from_slice(&value_len.to_be_bytes());
            bytes.extend_from_slice(&mutation.expected_hash().unwrap_or([0; HASH_BYTES]));
            bytes.extend_from_slice(mutation.key());
            if let Some(value) = mutation.value() {
                bytes.extend_from_slice(value);
            }
        }
        let frame_hash = digest(&bytes);
        bytes.extend_from_slice(&FOOTER_MAGIC);
        bytes.extend_from_slice(
            &u64::try_from(total_len)
                .map_err(|_| JournalCodecError::LimitExceeded)?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(&frame_hash);
        debug_assert_eq!(bytes.len(), total_len);
        Ok(EncodedJournalFrame {
            kind: self.kind,
            bytes: Arc::from(bytes),
            frame_hash,
            transition_count: self.transition_count,
            command_count: self.command_count,
            audit_count: self.audit_count,
            covered_sequence: self.covered_sequence,
            covered_administration_sequence: self.covered_administration_sequence,
            selected_command_segment_bytes: 0,
            raw_command_segment_bytes: 0,
            raw_equivalent_frame_bytes: total_len,
            records_changelog: header_bytes == FRAME_HEADER_BYTES && self.changelog.is_some(),
        })
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<(Self, [u8; HASH_BYTES]), JournalCodecError> {
        Self::decode_with(bytes, None, None)
    }

    /// Decodes a retained frame a history stub names (ADR-0263 Amendment 3).
    /// Its hash must equal `proven`, recorded from this frame after full
    /// validation, so identical bytes skip only re-deriving the command counts
    /// from every segment: that deep decode would otherwise run inside every
    /// history read, on threads with fixed stacks. All other checks remain.
    pub(crate) fn decode_proven(
        bytes: &[u8],
        proven: &[u8; HASH_BYTES],
    ) -> Result<Self, JournalCodecError> {
        Self::decode_with(bytes, Some(proven), None).map(|(frame, _)| frame)
    }

    /// Reuses only the digest already checked for this immutable borrow. Every
    /// ordinary semantic check, including deep command counts, still runs.
    fn decode_admitted(
        admitted: recovery_memory::LogicalFrameHashAdmission<'_>,
    ) -> Result<(Self, [u8; HASH_BYTES]), JournalCodecError> {
        Self::decode_with(admitted.bytes(), None, Some(admitted))
    }

    fn decode_with<'a>(
        bytes: &'a [u8],
        proven: Option<&[u8; HASH_BYTES]>,
        admitted: Option<recovery_memory::LogicalFrameHashAdmission<'a>>,
    ) -> Result<(Self, [u8; HASH_BYTES]), JournalCodecError> {
        if bytes.len() < FRAME_HEADER_BYTES_V2 + FRAME_FOOTER_BYTES {
            return Err(JournalCodecError::Truncated);
        }
        if bytes[..8] != FRAME_MAGIC {
            return Err(JournalCodecError::UnknownVersion);
        }
        let header_bytes = match read_u16(bytes, 8)? {
            FRAME_VERSION_V3 | FRAME_VERSION_V4 | FRAME_VERSION_V5 => FRAME_HEADER_BYTES,
            FRAME_VERSION_V2 => FRAME_HEADER_BYTES_V2,
            _ => return Err(JournalCodecError::UnknownVersion),
        };
        if bytes.len() < header_bytes + FRAME_FOOTER_BYTES {
            return Err(JournalCodecError::Truncated);
        }
        let changelog = if header_bytes == FRAME_HEADER_BYTES {
            FrameChangelogBinding::decode(&bytes[FRAME_HEADER_BYTES_V2..FRAME_HEADER_BYTES])?
        } else {
            None
        };
        if bytes.get(27) != Some(&0) || read_u16(bytes, 66)? != 0 {
            return Err(JournalCodecError::InvalidValue);
        }
        let mutation_count =
            usize::try_from(read_u32(bytes, 68)?).map_err(|_| JournalCodecError::LimitExceeded)?;
        let payload_len =
            usize::try_from(read_u32(bytes, 72)?).map_err(|_| JournalCodecError::LimitExceeded)?;
        let expected_len = header_bytes
            .checked_add(payload_len)
            .and_then(|value| value.checked_add(FRAME_FOOTER_BYTES))
            .ok_or(JournalCodecError::LimitExceeded)?;
        if expected_len > MAX_JOURNAL_FRAME_BYTES {
            return Err(JournalCodecError::LimitExceeded);
        }
        if bytes.len() != expected_len {
            return Err(JournalCodecError::Truncated);
        }
        let footer = header_bytes + payload_len;
        if bytes[footer..footer + 8] != FOOTER_MAGIC
            || usize::try_from(read_u64(bytes, footer + 8)?)
                .map_err(|_| JournalCodecError::LimitExceeded)?
                != expected_len
        {
            return Err(JournalCodecError::InvalidValue);
        }
        let frame_hash = match admitted {
            Some(admitted) => admitted.hash_for(bytes)?,
            None => logical_frame_digest(&bytes[..footer]),
        };
        if frame_hash != read_array::<HASH_BYTES>(bytes, footer + 16)?
            || proven.is_some_and(|proven| *proven != frame_hash)
        {
            return Err(JournalCodecError::Checksum);
        }
        if mutation_count > payload_len / MUTATION_HEADER_BYTES {
            return Err(JournalCodecError::InvalidValue);
        }
        let kind = JournalFrameKind::decode(bytes[26])?;
        let mut cursor = header_bytes;
        let source_reclamation = if kind == JournalFrameKind::SourceReclamation {
            if read_u16(bytes, 8)? < FRAME_VERSION_V5 {
                return Err(JournalCodecError::UnknownVersion);
            }
            let length = usize::try_from(read_u32(bytes, cursor)?)
                .map_err(|_| JournalCodecError::LimitExceeded)?;
            cursor = cursor
                .checked_add(4)
                .ok_or(JournalCodecError::LimitExceeded)?;
            let end = cursor
                .checked_add(length)
                .filter(|end| *end <= footer)
                .ok_or(JournalCodecError::Truncated)?;
            let proof = SourceReclamationProof::decode(&bytes[cursor..end])?;
            cursor = end;
            Some(proof)
        } else {
            None
        };
        let mut mutations = Vec::with_capacity(mutation_count);
        for _ in 0..mutation_count {
            let table =
                JournalTable::decode(*bytes.get(cursor).ok_or(JournalCodecError::Truncated)?)?;
            let operation = *bytes.get(cursor + 1).ok_or(JournalCodecError::Truncated)?;
            let expectation = *bytes.get(cursor + 2).ok_or(JournalCodecError::Truncated)?;
            if bytes.get(cursor + 3) != Some(&0) {
                return Err(JournalCodecError::InvalidValue);
            }
            let key_len = usize::try_from(read_u32(bytes, cursor + 4)?)
                .map_err(|_| JournalCodecError::LimitExceeded)?;
            let value_len = read_u32(bytes, cursor + 8)?;
            let expected_hash = read_array::<HASH_BYTES>(bytes, cursor + 12)?;
            cursor = cursor
                .checked_add(MUTATION_HEADER_BYTES)
                .ok_or(JournalCodecError::LimitExceeded)?;
            let key_end = cursor
                .checked_add(key_len)
                .ok_or(JournalCodecError::LimitExceeded)?;
            let key = bytes
                .get(cursor..key_end)
                .ok_or(JournalCodecError::Truncated)?
                .to_vec();
            cursor = key_end;
            let mutation = match (operation, value_len) {
                (0, value_len) if value_len != DELETE_VALUE_LENGTH => {
                    let value_end = cursor
                        .checked_add(
                            usize::try_from(value_len)
                                .map_err(|_| JournalCodecError::LimitExceeded)?,
                        )
                        .ok_or(JournalCodecError::LimitExceeded)?;
                    let value = bytes
                        .get(cursor..value_end)
                        .ok_or(JournalCodecError::Truncated)?
                        .to_vec();
                    cursor = value_end;
                    match expectation {
                        0 if expected_hash == [0; HASH_BYTES] => {
                            JournalMutation::put(table, key, value)?
                        }
                        1 => JournalMutation::Put {
                            table,
                            key: key.into_boxed_slice(),
                            expected_hash: Some(expected_hash),
                            value: value.into_boxed_slice(),
                        },
                        _ => return Err(JournalCodecError::InvalidValue),
                    }
                }
                (1, DELETE_VALUE_LENGTH) if expectation == 1 => JournalMutation::Delete {
                    table,
                    key: key.into_boxed_slice(),
                    expected_hash,
                },
                _ => return Err(JournalCodecError::InvalidValue),
            };
            mutations.push(mutation);
        }
        if cursor != footer {
            return Err(JournalCodecError::InvalidValue);
        }
        if read_u16(bytes, 8)? < FRAME_VERSION_V4
            && (bytes[26] == JournalFrameKind::RetentionPrune as u8
                || mutations.iter().any(|m| {
                    matches!(
                        m.table(),
                        JournalTable::HistoryTombstones | JournalTable::OutboxStatus
                    )
                }))
        {
            return Err(JournalCodecError::UnknownVersion);
        }
        let database_id = DatabaseId::from_bytes(read_array::<16>(bytes, 10)?)
            .map_err(|_| JournalCodecError::InvalidValue)?;
        let frame = Self::checked_with(
            database_id,
            JournalFrameKind::decode(*bytes.get(26).ok_or(JournalCodecError::Truncated)?)?,
            CommitSequence::new(read_u64(bytes, 28)?),
            CommitSequence::new(read_u64(bytes, 36)?),
            AdministrationSequence::new(read_u64(bytes, 44)?),
            AdministrationSequence::new(read_u64(bytes, 52)?),
            read_u16(bytes, 60)?,
            read_u16(bytes, 62)?,
            read_u16(bytes, 64)?,
            read_array::<HASH_BYTES>(bytes, 76)?,
            mutations,
            proven.is_none(),
        )?;
        let frame = Self {
            format_version: read_u16(bytes, 8)?,
            changelog,
            source_reclamation,
            ..frame
        };
        frame.validate_source_reclamation()?;
        Ok((frame, frame_hash))
    }

    fn payload_len(&self) -> Result<usize, JournalCodecError> {
        let proof_bytes = self
            .source_reclamation
            .as_ref()
            .map(|proof| {
                proof.encode().and_then(|bytes| {
                    bytes
                        .len()
                        .checked_add(4)
                        .ok_or(JournalCodecError::LimitExceeded)
                })
            })
            .transpose()?
            .unwrap_or(0);
        self.mutations
            .iter()
            .try_fold(proof_bytes, |total, mutation| {
                total
                    .checked_add(MUTATION_HEADER_BYTES)
                    .and_then(|value| value.checked_add(mutation.key().len()))
                    .and_then(|value| value.checked_add(mutation.value().map_or(0, <[u8]>::len)))
                    .ok_or(JournalCodecError::LimitExceeded)
            })
    }

    fn encoded_len(&self) -> Result<usize, JournalCodecError> {
        FRAME_HEADER_BYTES
            .checked_add(self.payload_len()?)
            .and_then(|value| value.checked_add(FRAME_FOOTER_BYTES))
            .ok_or(JournalCodecError::LimitExceeded)
    }

    pub(crate) fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    pub(crate) fn kind(&self) -> JournalFrameKind {
        self.kind
    }

    pub(crate) fn predecessor_sequence(&self) -> Option<CommitSequence> {
        self.predecessor_sequence
    }

    pub(crate) fn covered_sequence(&self) -> Option<CommitSequence> {
        self.covered_sequence
    }

    pub(crate) fn predecessor_administration_sequence(&self) -> Option<AdministrationSequence> {
        self.predecessor_administration_sequence
    }

    pub(crate) fn covered_administration_sequence(&self) -> Option<AdministrationSequence> {
        self.covered_administration_sequence
    }

    pub(crate) fn transition_count(&self) -> u16 {
        self.transition_count
    }

    pub(crate) fn command_count(&self) -> u16 {
        self.command_count
    }

    pub(crate) fn audit_count(&self) -> u16 {
        self.audit_count
    }

    pub(crate) fn previous_hash(&self) -> [u8; HASH_BYTES] {
        self.previous_hash
    }

    pub(crate) fn mutations(&self) -> &[JournalMutation] {
        &self.mutations
    }

    pub(crate) fn composite(
        &self,
    ) -> Result<riffdb_storage_api::CompositeFrameV1, riffdb_storage_api::StorageValueError> {
        let encoded = self
            .encode()
            .map_err(|_| riffdb_storage_api::StorageValueError::InvalidShape)?;
        let kind = match self.kind {
            JournalFrameKind::Command => riffdb_storage_api::CompositeFrameKindV1::Command,
            JournalFrameKind::RetentionPrune => {
                riffdb_storage_api::CompositeFrameKindV1::RetentionPrune
            }
            JournalFrameKind::SourceReclamation => {
                riffdb_storage_api::CompositeFrameKindV1::SourceReclamation
            }
            JournalFrameKind::ServiceAudit => {
                riffdb_storage_api::CompositeFrameKindV1::ServiceAudit
            }
        };
        let mutations = self
            .mutations
            .iter()
            .map(JournalMutation::composite)
            .collect::<Result<Vec<_>, _>>()?;
        riffdb_storage_api::CompositeFrameV1::new(
            kind,
            self.database_id,
            self.predecessor_sequence,
            self.covered_sequence,
            self.predecessor_administration_sequence,
            self.covered_administration_sequence,
            self.transition_count,
            encoded.as_bytes().len(),
            self.previous_hash,
            encoded.frame_hash(),
            mutations,
        )
    }
}

#[derive(Clone)]
pub(crate) struct EncodedJournalFrame {
    kind: JournalFrameKind,
    bytes: Arc<[u8]>,
    frame_hash: [u8; HASH_BYTES],
    transition_count: u16,
    command_count: u16,
    audit_count: u16,
    covered_sequence: Option<CommitSequence>,
    covered_administration_sequence: Option<AdministrationSequence>,
    selected_command_segment_bytes: usize,
    raw_command_segment_bytes: usize,
    raw_equivalent_frame_bytes: usize,
    /// Whether the frame records its changelog binding (version 3 with one).
    records_changelog: bool,
}

impl EncodedJournalFrame {
    pub(crate) fn kind(&self) -> JournalFrameKind {
        self.kind
    }

    /// Whether the frame records the binding its receipt re-derives with.
    pub(crate) const fn records_changelog(&self) -> bool {
        self.records_changelog
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn frame_hash(&self) -> [u8; HASH_BYTES] {
        self.frame_hash
    }

    pub(crate) fn transition_count(&self) -> u16 {
        self.transition_count
    }

    pub(crate) fn command_count(&self) -> u16 {
        self.command_count
    }

    fn selected_command_segment_bytes(&self) -> usize {
        self.selected_command_segment_bytes
    }

    fn raw_command_segment_bytes(&self) -> usize {
        self.raw_command_segment_bytes
    }

    fn raw_equivalent_frame_bytes(&self) -> usize {
        self.raw_equivalent_frame_bytes
    }

    pub(crate) fn audit_count(&self) -> u16 {
        self.audit_count
    }

    pub(crate) fn covered_sequence(&self) -> Option<CommitSequence> {
        self.covered_sequence
    }

    pub(crate) fn covered_administration_sequence(&self) -> Option<AdministrationSequence> {
        self.covered_administration_sequence
    }
}

struct EncodedExtentFrame {
    bytes: Vec<u8>,
    position: usize,
}

impl EncodedExtentFrame {
    fn encode(
        generation: u64,
        position: usize,
        logical: &EncodedJournalFrame,
    ) -> Result<Self, JournalIoError> {
        if generation == 0
            || position < EXTENT_DATA_OFFSET
            || !position.is_multiple_of(EXTENT_FRAME_ALIGNMENT)
        {
            return Err(JournalIoError::Corrupt);
        }
        let encoded_len = logical.as_bytes().len();
        let padded_len = extent_frame_bytes(encoded_len).ok_or(JournalIoError::Capacity)?;
        if padded_len > EXTENT_DATA_BYTES
            || position
                .checked_add(padded_len)
                .filter(|end| *end <= EXTENT_FILE_BYTES)
                .is_none()
        {
            return Err(JournalIoError::Capacity);
        }
        let encoded_len_u32 = u32::try_from(encoded_len).map_err(|_| JournalIoError::Capacity)?;
        let padded_len_u32 = u32::try_from(padded_len).map_err(|_| JournalIoError::Capacity)?;
        let position_u64 = u64::try_from(position).map_err(|_| JournalIoError::Capacity)?;
        let mut bytes = vec![0_u8; padded_len];
        bytes[..8].copy_from_slice(&EXTENT_FRAME_MAGIC);
        bytes[8..10].copy_from_slice(&EXTENT_FRAME_VERSION.to_be_bytes());
        bytes[10..12].copy_from_slice(
            &u16::try_from(EXTENT_FRAME_HEADER_BYTES)
                .expect("physical frame header is bounded")
                .to_be_bytes(),
        );
        bytes[12..20].copy_from_slice(&generation.to_be_bytes());
        bytes[20..28].copy_from_slice(&position_u64.to_be_bytes());
        bytes[28..32].copy_from_slice(&encoded_len_u32.to_be_bytes());
        bytes[32..36].copy_from_slice(&padded_len_u32.to_be_bytes());
        bytes[36..68].copy_from_slice(&logical.frame_hash());
        bytes[EXTENT_FRAME_HEADER_BYTES..EXTENT_FRAME_HEADER_BYTES + encoded_len]
            .copy_from_slice(logical.as_bytes());
        let footer = padded_len - EXTENT_FRAME_FOOTER_BYTES;
        bytes[footer..footer + 8].copy_from_slice(&EXTENT_FRAME_FOOTER_MAGIC);
        bytes[footer + 8..footer + 16].copy_from_slice(&generation.to_be_bytes());
        bytes[footer + 16..footer + 24].copy_from_slice(&position_u64.to_be_bytes());
        bytes[footer + 24..footer + 56].copy_from_slice(&logical.frame_hash());
        let checksum = extent_frame_checksum(&bytes);
        bytes[68..100].copy_from_slice(&checksum);
        Ok(Self { bytes, position })
    }
}

pub(crate) fn extent_frame_bytes(encoded_len: usize) -> Option<usize> {
    EXTENT_FRAME_HEADER_BYTES
        .checked_add(encoded_len)
        .and_then(|value| value.checked_add(EXTENT_FRAME_FOOTER_BYTES))
        .and_then(|value| align_up(value, EXTENT_FRAME_ALIGNMENT))
        .filter(|value| *value <= EXTENT_DATA_BYTES)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct JournalFence {
    pub(crate) covered_sequence: Option<CommitSequence>,
    pub(crate) covered_administration_sequence: Option<AdministrationSequence>,
    pub(crate) frame_hash: [u8; HASH_BYTES],
    pub(crate) durable_at: Instant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum JournalIoError {
    Corrupt,
    Io,
    Stopped,
    Capacity,
    LegacyNonEmpty(PathBuf),
}

struct JournalSubmission {
    frame: EncodedJournalFrame,
    completion: Arc<JournalFenceCompletion>,
    submitted_at: Instant,
}

impl Drop for JournalSubmission {
    fn drop(&mut self) {
        // Preserve the former channel-disconnect behavior: a worker unwind or
        // premature exit must wake every waiter instead of leaving a shared
        // completion cell pending forever. Successful/error completion is
        // idempotent and therefore wins before this fallback runs.
        self.completion.complete(Err(JournalIoError::Stopped));
    }
}

#[derive(Default)]
struct JournalFenceCompletion {
    result: Mutex<Option<Result<JournalFence, JournalIoError>>>,
    ready: Condvar,
    command_bearing: bool,
}

/// Counts blocking-method invocations, including ready rechecks and errors.
/// Concurrent callers can overlap: this is not a partition of writer wall.
#[derive(Default)]
struct CommandReceiptWaitCensus {
    calls: AtomicU64,
    errors: AtomicU64,
    sum_us: AtomicU64,
    max_us: AtomicU64,
}

static COMMAND_RECEIPT_WAITS: CommandReceiptWaitCensus = CommandReceiptWaitCensus {
    calls: AtomicU64::new(0),
    errors: AtomicU64::new(0),
    sum_us: AtomicU64::new(0),
    max_us: AtomicU64::new(0),
};

impl CommandReceiptWaitCensus {
    fn record(&self, elapsed: Duration, failed: bool) {
        saturating_atomic_add(&self.calls, 1);
        if failed {
            saturating_atomic_add(&self.errors, 1);
        }
        record_duration(&self.sum_us, &self.max_us, elapsed);
    }

    fn snapshot(&self) -> [u64; 4] {
        [
            self.calls.load(Ordering::Relaxed),
            self.errors.load(Ordering::Relaxed),
            self.sum_us.load(Ordering::Relaxed),
            self.max_us.load(Ordering::Relaxed),
        ]
    }
}

pub(crate) fn command_receipt_wait_census() -> [u64; 4] {
    COMMAND_RECEIPT_WAITS.snapshot()
}

#[cfg(test)]
#[path = "journal_wait_tests.rs"]
mod wait_tests;

impl JournalFenceCompletion {
    fn complete(&self, result: Result<JournalFence, JournalIoError>) {
        if let Ok(mut slot) = self.result.lock()
            && slot.is_none()
        {
            *slot = Some(result);
            self.ready.notify_all();
        }
    }

    fn try_wait(&self) -> Result<Option<JournalFence>, JournalIoError> {
        self.result
            .lock()
            .map_err(|_| JournalIoError::Stopped)?
            .clone()
            .transpose()
    }

    fn wait(&self) -> Result<JournalFence, JournalIoError> {
        self.wait_observed(&COMMAND_RECEIPT_WAITS, Instant::now, || {})
    }

    fn wait_observed(
        &self,
        census: &CommandReceiptWaitCensus,
        now: impl Fn() -> Instant,
        before_park: impl Fn(),
    ) -> Result<JournalFence, JournalIoError> {
        let started = self.command_bearing.then(&now);
        let result = (|| {
            let mut slot = self.result.lock().map_err(|_| JournalIoError::Stopped)?;
            while slot.is_none() {
                before_park();
                slot = self.ready.wait(slot).map_err(|_| JournalIoError::Stopped)?;
            }
            slot.clone().expect("journal completion is present")
        })();
        if let Some(started) = started {
            census.record(now().saturating_duration_since(started), result.is_err());
        }
        result
    }
}

#[derive(Clone)]
pub(crate) struct JournalFenceReceipt {
    completion: Arc<JournalFenceCompletion>,
}

impl JournalFenceReceipt {
    pub(crate) fn try_wait(&self) -> Result<Option<JournalFence>, JournalIoError> {
        self.completion.try_wait()
    }

    pub(crate) fn wait(&self) -> Result<JournalFence, JournalIoError> {
        self.completion.wait()
    }
}

pub(crate) struct JournalLane {
    sender: Option<mpsc::SyncSender<JournalSubmission>>,
    worker: Option<thread::JoinHandle<()>>,
    durable_flushes: Arc<AtomicU64>,
    /// Generation of the extent this lane appends to; a history stub names it.
    generation: u64,
    file_identity: Option<JournalFileIdentity>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct JournalFileIdentity {
    device: u64,
    inode: u64,
}

fn journal_file_identity(file: &MediaFile) -> Result<Option<JournalFileIdentity>, JournalIoError> {
    #[cfg(unix)]
    if let MediaFile::Fs(file) = file {
        use std::os::unix::fs::MetadataExt;
        let metadata = file.metadata().map_err(|_| JournalIoError::Io)?;
        return Ok(Some(JournalFileIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        }));
    }
    let _ = file;
    Ok(None)
}

impl JournalLane {
    /// Real-filesystem wrapper retained for this module's unit tests; every
    /// production caller routes through the media-parameterized form.
    #[cfg(test)]
    pub(crate) fn open(path: &Path, header: &JournalFileHeader) -> Result<Self, JournalIoError> {
        Self::open_with_media(
            &RealJournalMedia,
            path,
            header,
            #[cfg(feature = "test-fixtures")]
            None,
        )
    }

    pub(crate) fn open_with_media(
        media: &dyn JournalMedia,
        path: &Path,
        header: &JournalFileHeader,
        #[cfg(feature = "test-fixtures")] controller: Option<crate::hooks::RedbTestController>,
    ) -> Result<Self, JournalIoError> {
        let state = initialize_or_validate_file_with_media(media, path, header)?;
        let generation = state.header.generation;
        let file = media
            .open_read_write(path)
            .map_err(|_| JournalIoError::Io)?;
        let file_identity = journal_file_identity(&file)?;
        let (sender, receiver) = mpsc::sync_channel(MAX_JOURNAL_COMMANDS);
        let durable_flushes = Arc::new(AtomicU64::new(0));
        let worker_flushes = Arc::clone(&durable_flushes);
        let worker = thread::Builder::new()
            .name("riffdb-journal".to_string())
            .stack_size(crate::PRODUCTION_THREAD_STACK_BYTES)
            .spawn(move || {
                journal_worker(
                    file,
                    receiver,
                    &worker_flushes,
                    state,
                    #[cfg(feature = "test-fixtures")]
                    controller,
                )
            })
            .map_err(|_| JournalIoError::Io)?;
        Ok(Self {
            sender: Some(sender),
            worker: Some(worker),
            durable_flushes,
            generation,
            file_identity,
        })
    }

    /// Only the same owned native file can retain an idle append cursor.
    /// Simulated or otherwise unidentifiable handles keep the fresh-open path.
    pub(crate) fn owns_current_file(
        &self,
        media: &dyn JournalMedia,
        path: &Path,
    ) -> Result<bool, JournalIoError> {
        let Some(identity) = self.file_identity else {
            return Ok(false);
        };
        let file = media.open_read(path).map_err(|_| JournalIoError::Io)?;
        Ok(journal_file_identity(&file)? == Some(identity))
    }

    /// Runs the ordinary complete open checks before reusing physical ownership.
    pub(crate) fn can_reuse_cursor(
        &self,
        media: &dyn JournalMedia,
        path: &Path,
        header: &JournalFileHeader,
        physical_bytes: usize,
    ) -> Result<bool, JournalIoError> {
        if self
            .worker
            .as_ref()
            .is_none_or(thread::JoinHandle::is_finished)
        {
            return Ok(false);
        }
        let state = initialize_or_validate_file_with_media(media, path, header)?;
        Ok(state.header.generation == self.generation
            && extent_used_bytes(state.next_position) == physical_bytes
            && self.owns_current_file(media, path)?)
    }

    /// Generation of the extent this lane appends to.
    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn submit(
        &self,
        frame: EncodedJournalFrame,
    ) -> Result<JournalFenceReceipt, JournalIoError> {
        let completion = Arc::new(JournalFenceCompletion {
            command_bearing: frame.command_count() > 0,
            ..JournalFenceCompletion::default()
        });
        self.sender
            .as_ref()
            .ok_or(JournalIoError::Stopped)?
            .send(JournalSubmission {
                frame,
                completion: Arc::clone(&completion),
                submitted_at: Instant::now(),
            })
            .map_err(|_| JournalIoError::Stopped)?;
        Ok(JournalFenceReceipt { completion })
    }

    #[cfg(test)]
    fn durable_flushes(&self) -> u64 {
        self.durable_flushes.load(Ordering::Relaxed)
    }
}

impl Drop for JournalLane {
    fn drop(&mut self) {
        drop(self.sender.take());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

/// Real-filesystem wrapper retained for this module's unit tests; every
/// production caller routes through the media-parameterized form.
#[cfg(test)]
fn initialize_or_validate_file(
    path: &Path,
    expected: &JournalFileHeader,
) -> Result<ExtentState, JournalIoError> {
    initialize_or_validate_file_with_media(&RealJournalMedia, path, expected)
}

fn initialize_or_validate_file_with_media(
    media: &dyn JournalMedia,
    path: &Path,
    expected: &JournalFileHeader,
) -> Result<ExtentState, JournalIoError> {
    match media.open_read(path) {
        Ok(mut file) => {
            let mut magic = [0_u8; 8];
            file.read_exact(&mut magic)
                .map_err(|_| JournalIoError::Corrupt)?;
            if magic == FILE_MAGIC {
                let (_, tail) = scan_legacy_journal_with_media(
                    media,
                    path,
                    expected.database_id(),
                    |_| Ok(()),
                )?
                .ok_or(JournalIoError::Corrupt)?;
                if tail.transition_count != 0 || tail.incomplete_tail {
                    return Err(JournalIoError::LegacyNonEmpty(path.to_path_buf()));
                }
                migrate_empty_legacy_journal_with_media(media, path, expected)?;
            } else if magic != EXTENT_MAGIC {
                return Err(JournalIoError::Corrupt);
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // The active name must never expose a partially initialized file.
            // Reuse the existing synced replacement ceremony for the first
            // extent too: a crash leaves absence or a complete anchored extent.
            migrate_empty_legacy_journal_with_media(media, path, expected)?;
        }
        Err(_) => return Err(JournalIoError::Io),
    }
    let state = inspect_extent_with_media(media, path)?;
    if state.header.logical != *expected {
        return Err(JournalIoError::Corrupt);
    }
    Ok(state)
}

fn journal_worker(
    mut file: MediaFile,
    receiver: mpsc::Receiver<JournalSubmission>,
    durable_flushes: &AtomicU64,
    state: ExtentState,
    #[cfg(feature = "test-fixtures")] controller: Option<crate::hooks::RedbTestController>,
) {
    let generation = state.header.generation;
    let file_bytes = extent_file_bytes(state.header.data_bytes);
    let mut next_position = state.next_position;
    let mut deferred = None;
    loop {
        let first = match deferred.take() {
            Some(value) => value,
            None => match receiver.recv() {
                Ok(value) => value,
                Err(_) => return,
            },
        };
        let mut transition_count = usize::from(first.frame.transition_count());
        let mut byte_count = first.frame.as_bytes().len();
        let mut batch = vec![first];
        while batch.len() < MAX_JOURNAL_TRANSITIONS {
            let next = match receiver.try_recv() {
                Ok(value) => value,
                Err(mpsc::TryRecvError::Empty | mpsc::TryRecvError::Disconnected) => break,
            };
            let next_transitions =
                transition_count.saturating_add(usize::from(next.frame.transition_count()));
            let next_bytes = byte_count.saturating_add(next.frame.as_bytes().len());
            if next_transitions > MAX_JOURNAL_TRANSITIONS || next_bytes > MAX_JOURNAL_FRAME_BYTES {
                deferred = Some(next);
                break;
            }
            transition_count = next_transitions;
            byte_count = next_bytes;
            batch.push(next);
        }

        let encode_started = Instant::now();
        let mut physical = Vec::with_capacity(batch.len());
        let mut encode_error = None;
        for submission in &batch {
            match EncodedExtentFrame::encode(generation, next_position, &submission.frame) {
                Ok(frame) => {
                    next_position = match next_position.checked_add(frame.bytes.len()) {
                        Some(position) => position,
                        None => {
                            encode_error = Some(JournalIoError::Capacity);
                            break;
                        }
                    };
                    physical.push(frame);
                }
                Err(error) => {
                    encode_error = Some(error);
                    break;
                }
            }
        }
        let encode_elapsed = encode_started.elapsed();
        let tail_position = next_position;
        let write_started = Instant::now();
        let positional_write = if let Some(error) = encode_error {
            Err(error)
        } else {
            physical
                .iter()
                .try_for_each(|frame| {
                    file.write_all_at(&frame.bytes, frame.position)
                        .map_err(|_| JournalIoError::Io)
                })
                .and_then(|()| {
                    // A recycled extent can retain an older, differently sized
                    // frame beyond this generation's shorter suffix. Seal one
                    // zero header at the new tail in the same durability fence
                    // so recovery cannot mistake stale frame interior bytes for
                    // a torn current-generation frame.
                    if tail_position < file_bytes {
                        file.write_all_at(&[0_u8; EXTENT_FRAME_HEADER_BYTES], tail_position)
                            .map_err(|_| JournalIoError::Io)?;
                    }
                    Ok(())
                })
        };
        let write_elapsed = write_started.elapsed();
        #[cfg(feature = "test-fixtures")]
        if let Some(controller) = &controller {
            controller
                .checkpoint_test_pause(crate::hooks::RedbCheckpointTestPoint::BeforeJournalSync);
        }
        let sync_started = Instant::now();
        let write =
            positional_write.and_then(|()| file.sync_data().map_err(|_| JournalIoError::Io));
        let sync_elapsed = sync_started.elapsed();
        if write.is_ok() {
            durable_flushes.fetch_add(1, Ordering::Relaxed);
            record_command_flush_census(
                &batch,
                encode_started,
                encode_elapsed,
                write_elapsed,
                sync_elapsed,
            );
            for submission in &batch {
                record_command_frame_census(&submission.frame);
            }
        }
        for submission in batch {
            let result = if write.is_ok() {
                Ok(JournalFence {
                    covered_sequence: submission.frame.covered_sequence(),
                    covered_administration_sequence: submission
                        .frame
                        .covered_administration_sequence(),
                    frame_hash: submission.frame.frame_hash(),
                    durable_at: Instant::now(),
                })
            } else {
                Err(write.clone().expect_err("failed write carries an error"))
            };
            submission.completion.complete(result);
        }
        if let Err(error) = write {
            for submission in receiver.try_iter() {
                submission.completion.complete(Err(error.clone()));
            }
            return;
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum JournalCodecError {
    Checksum,
    InvalidValue,
    LimitExceeded,
    Truncated,
    UnknownTable,
    UnknownVersion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct JournalScanTail {
    pub(crate) last_sequence: Option<CommitSequence>,
    pub(crate) last_administration_sequence: Option<AdministrationSequence>,
    pub(crate) last_hash: [u8; HASH_BYTES],
    pub(crate) incomplete_tail: bool,
    pub(crate) complete_bytes: usize,
    pub(crate) transition_count: usize,
    pub(crate) command_count: usize,
    pub(crate) audit_count: usize,
}

pub(crate) fn journal_path(database_path: &Path) -> PathBuf {
    let mut path = database_path.as_os_str().to_os_string();
    path.push(".riffjournal");
    PathBuf::from(path)
}

/// Immutable journal extent currently being materialized into redb.
pub(crate) fn checkpoint_journal_path(database_path: &Path) -> PathBuf {
    let mut path = database_path.as_os_str().to_os_string();
    path.push(".riffjournal.checkpoint");
    PathBuf::from(path)
}

/// Preallocated non-authoritative extent reserved for the next rotation.
pub(crate) fn spare_journal_path(database_path: &Path) -> PathBuf {
    let mut path = database_path.as_os_str().to_os_string();
    path.push(".riffjournal.next");
    PathBuf::from(path)
}

static SEALED_EXTENTS: AtomicU64 = AtomicU64::new(0);
static SEAL_FAILURES: AtomicU64 = AtomicU64::new(0);
static PREPARED_SPARES: AtomicU64 = AtomicU64::new(0);
static SPARE_FAILURES: AtomicU64 = AtomicU64::new(0);

/// Counts one background seal attempt's outcome.
pub(crate) fn record_seal_outcome(outcome: Result<Option<PathBuf>, JournalIoError>) {
    match outcome {
        Ok(Some(_)) => {
            SEALED_EXTENTS.fetch_add(1, Ordering::Relaxed);
        }
        Ok(None) => {}
        Err(_) => {
            SEAL_FAILURES.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Counts one background spare preparation's outcome.
pub(crate) fn record_spare_outcome(outcome: Result<bool, JournalIoError>) {
    match outcome {
        Ok(true) => {
            PREPARED_SPARES.fetch_add(1, Ordering::Relaxed);
        }
        Ok(false) => {}
        Err(_) => {
            SPARE_FAILURES.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Sealed extents, seal failures, prepared spares, and spare failures.
pub(crate) fn retained_extent_census() -> [u64; 4] {
    [
        SEALED_EXTENTS.load(Ordering::Relaxed),
        SEAL_FAILURES.load(Ordering::Relaxed),
        PREPARED_SPARES.load(Ordering::Relaxed),
        SPARE_FAILURES.load(Ordering::Relaxed),
    ]
}

/// The one extent being sealed after its checkpoint installed (ADR-0263).
/// Sealing finishes before another extent may take this name.
pub(crate) fn sealing_journal_path(database_path: &Path) -> PathBuf {
    let mut path = database_path.as_os_str().to_os_string();
    path.push(".riffjournal.sealing");
    PathBuf::from(path)
}

/// A sealed, retained extent, named by its unique generation.
pub(crate) fn sealed_journal_path(database_path: &Path, generation: u64) -> PathBuf {
    let mut path = database_path.as_os_str().to_os_string();
    path.push(format!(".riffjournal.sealed.{generation:020}"));
    PathBuf::from(path)
}

/// Where a spare extent is zero-filled before it takes the spare name.
pub(crate) fn preparing_spare_journal_path(database_path: &Path) -> PathBuf {
    let mut path = database_path.as_os_str().to_os_string();
    path.push(".riffjournal.next.prep");
    PathBuf::from(path)
}

const SEALED_FOOTER_VERSION: u16 = 1;
const SEALED_FOOTER_HEADER_BYTES: usize = 8 + 2 + 2 + 4 + 8 + 8 + 8 + HASH_BYTES + 8;
const SEALED_FOOTER_ENTRY_BYTES: usize = 192;

/// One retained frame as a sealed footer records it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SealedFrameEntry {
    pub(crate) placement: FramePlacement,
    pub(crate) kind: JournalFrameKind,
    pub(crate) predecessor_sequence: Option<CommitSequence>,
    pub(crate) covered_sequence: Option<CommitSequence>,
    pub(crate) predecessor_administration_sequence: Option<AdministrationSequence>,
    pub(crate) covered_administration_sequence: Option<AdministrationSequence>,
    pub(crate) changelog: Option<FrameChangelogBinding>,
}

impl SealedFrameEntry {
    fn from_frame(frame: &JournalFrame, placement: FramePlacement) -> Self {
        Self {
            placement,
            kind: frame.kind,
            predecessor_sequence: frame.predecessor_sequence,
            covered_sequence: frame.covered_sequence,
            predecessor_administration_sequence: frame.predecessor_administration_sequence,
            covered_administration_sequence: frame.covered_administration_sequence,
            changelog: frame.changelog,
        }
    }
}

/// A sealed extent's self-describing index (ADR-0263 Decision 3 and
/// Amendment 2 B4): every frame's placement, frontiers, and receipt binding,
/// so the history index can be rebuilt from footers alone.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SealedExtentFooter {
    pub(crate) generation: u64,
    pub(crate) origin: ChainPoint,
    pub(crate) entries: Vec<SealedFrameEntry>,
}

impl SealedExtentFooter {
    fn encoded_len(entries: usize) -> Option<usize> {
        entries
            .checked_mul(SEALED_FOOTER_ENTRY_BYTES)?
            .checked_add(SEALED_FOOTER_HEADER_BYTES + HASH_BYTES)
    }

    fn encode(&self) -> Result<Vec<u8>, JournalIoError> {
        let len = Self::encoded_len(self.entries.len()).ok_or(JournalIoError::Corrupt)?;
        let padded = align_up(len, EXTENT_FRAME_ALIGNMENT).ok_or(JournalIoError::Corrupt)?;
        let mut bytes = Vec::with_capacity(padded);
        bytes.extend_from_slice(&SEALED_FOOTER_MAGIC);
        bytes.extend_from_slice(&SEALED_FOOTER_VERSION.to_be_bytes());
        bytes.extend_from_slice(&0_u16.to_be_bytes());
        bytes.extend_from_slice(
            &u32::try_from(self.entries.len())
                .map_err(|_| JournalIoError::Corrupt)?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(&self.generation.to_be_bytes());
        bytes.extend_from_slice(&encode_commit_frontier(self.origin.0));
        bytes.extend_from_slice(&encode_administration_frontier(self.origin.1));
        bytes.extend_from_slice(&self.origin.2);
        bytes.extend_from_slice(&[0; 8]);
        for entry in &self.entries {
            bytes.extend_from_slice(
                &u64::try_from(entry.placement.position)
                    .map_err(|_| JournalIoError::Corrupt)?
                    .to_be_bytes(),
            );
            bytes.extend_from_slice(
                &u32::try_from(entry.placement.padded_len)
                    .map_err(|_| JournalIoError::Corrupt)?
                    .to_be_bytes(),
            );
            bytes.push(entry.kind as u8);
            bytes.extend_from_slice(&[0; 3]);
            bytes.extend_from_slice(&entry.placement.frame_hash);
            bytes.extend_from_slice(&encode_commit_frontier(entry.predecessor_sequence));
            bytes.extend_from_slice(&encode_commit_frontier(entry.covered_sequence));
            bytes.extend_from_slice(&encode_administration_frontier(
                entry.predecessor_administration_sequence,
            ));
            bytes.extend_from_slice(&encode_administration_frontier(
                entry.covered_administration_sequence,
            ));
            bytes.extend_from_slice(&FrameChangelogBinding::encode(entry.changelog.as_ref()));
            bytes.extend_from_slice(&[0; 16]);
        }
        let checksum = digest(&bytes);
        bytes.extend_from_slice(&checksum);
        debug_assert_eq!(bytes.len(), len);
        bytes.resize(padded, 0);
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Self, JournalIoError> {
        let corrupt = |_| JournalIoError::Corrupt;
        if bytes.get(..8) != Some(SEALED_FOOTER_MAGIC.as_slice())
            || read_u16(bytes, 8).map_err(corrupt)? != SEALED_FOOTER_VERSION
            || read_u16(bytes, 10).map_err(corrupt)? != 0
        {
            return Err(JournalIoError::Corrupt);
        }
        let count = usize::try_from(read_u32(bytes, 12).map_err(corrupt)?)
            .map_err(|_| JournalIoError::Corrupt)?;
        let len = Self::encoded_len(count).ok_or(JournalIoError::Corrupt)?;
        let body = bytes
            .get(..len - HASH_BYTES)
            .ok_or(JournalIoError::Corrupt)?;
        // The reserved bytes follow the origin hash at 40..72.
        if digest(body).as_slice() != &bytes[len - HASH_BYTES..len]
            || bytes[len..].iter().any(|byte| *byte != 0)
            || body[72..SEALED_FOOTER_HEADER_BYTES]
                .iter()
                .any(|byte| *byte != 0)
        {
            return Err(JournalIoError::Corrupt);
        }
        let origin = (
            CommitSequence::new(read_u64(body, 24).map_err(corrupt)?),
            AdministrationSequence::new(read_u64(body, 32).map_err(corrupt)?),
            read_array::<HASH_BYTES>(body, 40).map_err(corrupt)?,
        );
        let mut entries = Vec::with_capacity(count);
        for index in 0..count {
            let at = SEALED_FOOTER_HEADER_BYTES + index * SEALED_FOOTER_ENTRY_BYTES;
            let entry = &body[at..at + SEALED_FOOTER_ENTRY_BYTES];
            if entry[13..16].iter().any(|byte| *byte != 0)
                || entry[176..].iter().any(|byte| *byte != 0)
            {
                return Err(JournalIoError::Corrupt);
            }
            entries.push(SealedFrameEntry {
                placement: FramePlacement {
                    position: usize::try_from(read_u64(entry, 0).map_err(corrupt)?)
                        .map_err(|_| JournalIoError::Corrupt)?,
                    padded_len: usize::try_from(read_u32(entry, 8).map_err(corrupt)?)
                        .map_err(|_| JournalIoError::Corrupt)?,
                    frame_hash: read_array::<HASH_BYTES>(entry, 16).map_err(corrupt)?,
                },
                kind: JournalFrameKind::decode(entry[12]).map_err(corrupt)?,
                predecessor_sequence: CommitSequence::new(read_u64(entry, 48).map_err(corrupt)?),
                covered_sequence: CommitSequence::new(read_u64(entry, 56).map_err(corrupt)?),
                predecessor_administration_sequence: AdministrationSequence::new(
                    read_u64(entry, 64).map_err(corrupt)?,
                ),
                covered_administration_sequence: AdministrationSequence::new(
                    read_u64(entry, 72).map_err(corrupt)?,
                ),
                changelog: FrameChangelogBinding::decode(&entry[80..176]).map_err(corrupt)?,
            });
        }
        Ok(Self {
            generation: read_u64(body, 16).map_err(corrupt)?,
            origin,
            entries,
        })
    }
}

/// Retires an installed checkpoint extent: finishes any earlier seal so the
/// sealing name is free, moves the extent to it, and seals it (ADR-0263). An
/// absent checkpoint extent makes this a no-op, so a caller may repeat it.
pub(crate) fn retire_checkpoint_extent_with_media(
    media: &dyn JournalMedia,
    database_path: &Path,
    database_id: DatabaseId,
) -> Result<(), JournalIoError> {
    let checkpoint = checkpoint_journal_path(database_path);
    if !media
        .try_exists(&checkpoint)
        .map_err(|_| JournalIoError::Io)?
    {
        return Ok(());
    }
    seal_extent_with_media(media, database_path, database_id)?;
    let sealing = sealing_journal_path(database_path);
    media
        .rename(&checkpoint, &sealing)
        .map_err(|_| JournalIoError::Io)?;
    sync_parent_with_media(media, &sealing)?;
    seal_extent_with_media(media, database_path, database_id).map(drop)
}

/// Seals the extent waiting at [`sealing_journal_path`], if any, and returns
/// the retained path it now has.
///
/// The caller renamed it there only after the checkpoint that materialized
/// its last frame installed, so every frame in it is already in the engine.
/// Each step is idempotent, so a crash at any point resumes here on the next
/// seal or open: anchor the header at the tail, write the footer at the anchor,
/// trim the file to frames plus footer, sync it, then rename it to its unique
/// sealed name and sync the directory.
pub(crate) fn seal_extent_with_media(
    media: &dyn JournalMedia,
    database_path: &Path,
    database_id: DatabaseId,
) -> Result<Option<PathBuf>, JournalIoError> {
    let sealing = sealing_journal_path(database_path);
    if !media.try_exists(&sealing).map_err(|_| JournalIoError::Io)? {
        return Ok(None);
    }
    let mut file = media
        .open_read_write(&sealing)
        .map_err(|_| JournalIoError::Io)?;
    let extent = read_selected_extent_header(&mut file, false)?;
    if extent.logical.database_id() != database_id {
        return Err(JournalIoError::Corrupt);
    }
    let file_len = usize::try_from(file.len().map_err(|_| JournalIoError::Io)?)
        .map_err(|_| JournalIoError::Corrupt)?;
    let mut entries = Vec::new();
    let walk = walk_extent_frames(
        &mut file,
        &extent,
        ExtentWalk::FromOrigin,
        file_len,
        |frame, placement| {
            entries.push(SealedFrameEntry::from_frame(frame, placement));
            Ok(())
        },
    )?;
    if walk.tail.incomplete_tail {
        return Err(JournalIoError::Corrupt);
    }
    let tail = walk.tail.complete_bytes;
    let footer = SealedExtentFooter {
        generation: extent.generation,
        origin: walk.origin.unwrap_or((
            extent.logical.checkpoint_sequence(),
            extent.logical.checkpoint_administration_sequence(),
            extent.logical.checkpoint_frame_hash(),
        )),
        entries,
    };
    let encoded = footer.encode()?;
    if extent.anchor_position != tail {
        let anchored = extent.anchored(
            JournalFileHeader::with_frontiers(
                database_id,
                walk.tail
                    .last_sequence
                    .max(extent.logical.checkpoint_sequence()),
                walk.tail
                    .last_administration_sequence
                    .max(extent.logical.checkpoint_administration_sequence()),
                walk.tail.last_hash,
            ),
            tail,
        )?;
        file.write_all_at(
            &anchored.encode(),
            usize::from(anchored.slot) * EXTENT_HEADER_SLOT_BYTES,
        )
        .and_then(|()| file.sync_data())
        .map_err(|_| JournalIoError::Io)?;
    }
    if walk.footer {
        let mut written = vec![0_u8; encoded.len()];
        file.read_exact_at(&mut written, tail)
            .map_err(|_| JournalIoError::Corrupt)?;
        if SealedExtentFooter::decode(&written)? != footer {
            return Err(JournalIoError::Corrupt);
        }
    } else {
        file.write_all_at(&encoded, tail)
            .and_then(|()| file.sync_data())
            .map_err(|_| JournalIoError::Io)?;
    }
    let end = tail
        .checked_add(encoded.len())
        .ok_or(JournalIoError::Corrupt)?;
    if file_len != end {
        file.set_len(u64::try_from(end).map_err(|_| JournalIoError::Corrupt)?)
            .map_err(|_| JournalIoError::Io)?;
    }
    file.sync_all().map_err(|_| JournalIoError::Io)?;
    drop(file);
    let sealed = sealed_journal_path(database_path, extent.generation);
    if media.try_exists(&sealed).map_err(|_| JournalIoError::Io)? {
        return Err(JournalIoError::Corrupt);
    }
    media
        .rename(&sealing, &sealed)
        .map_err(|_| JournalIoError::Io)?;
    media
        .sync_parent_all(&sealed)
        .map_err(|_| JournalIoError::Io)?;
    Ok(Some(sealed))
}

/// Test support: removes a sealed extent's footer and restores its full
/// zero-filled capacity, leaving it exactly as a seal that crashed after
/// anchoring its tail and before writing the footer would.
#[cfg(any(test, feature = "test-fixtures"))]
pub(crate) fn unseal_extent_with_media(
    media: &dyn JournalMedia,
    path: &Path,
    database_id: DatabaseId,
) -> Result<(), JournalIoError> {
    read_sealed_extent_with_media(media, path, database_id)?;
    let mut file = media
        .open_read_write(path)
        .map_err(|_| JournalIoError::Io)?;
    let extent = read_selected_extent_header(&mut file, false)?;
    let to_u64 = |bytes: usize| u64::try_from(bytes).map_err(|_| JournalIoError::Corrupt);
    let (tail, full) = (
        to_u64(extent.anchor_position)?,
        to_u64(extent_file_bytes(extent.data_bytes))?,
    );
    file.set_len(tail)
        .and_then(|()| file.set_len(full))
        .and_then(|()| file.sync_all())
        .map_err(|_| JournalIoError::Io)
}

/// Reads the bounded, checksummed sealed index without walking frame payloads.
/// This is only a preflight: reclamation must verify every frame before reuse.
pub(crate) fn read_sealed_index_with_media(
    media: &dyn JournalMedia,
    path: &Path,
    database_id: DatabaseId,
) -> Result<SealedExtentFooter, JournalIoError> {
    let mut file = media.open_read(path).map_err(|_| JournalIoError::Io)?;
    let extent = read_selected_extent_header(&mut file, false)?;
    if extent.logical.database_id() != database_id {
        return Err(JournalIoError::Corrupt);
    }
    let length = usize::try_from(file.len().map_err(|_| JournalIoError::Io)?)
        .map_err(|_| JournalIoError::Corrupt)?;
    let bytes = bounded_sealed_footer_bytes(length, extent.anchor_position)?;
    let mut encoded = vec![0; bytes];
    file.read_exact_at(&mut encoded, extent.anchor_position)
        .map_err(|_| JournalIoError::Corrupt)?;
    let footer = SealedExtentFooter::decode(&encoded)?;
    if footer.generation != extent.generation {
        return Err(JournalIoError::Corrupt);
    }
    Ok(footer)
}

/// The index preflight and complete reader share one allocation ceiling.
/// Validate the footer tail before either reader allocates its contents.
fn bounded_sealed_footer_bytes(length: usize, anchor: usize) -> Result<usize, JournalIoError> {
    let bytes = length.checked_sub(anchor).ok_or(JournalIoError::Corrupt)?;
    let maximum = SealedExtentFooter::encoded_len(MAX_JOURNAL_SUFFIX_TRANSITIONS)
        .and_then(|bytes| align_up(bytes, EXTENT_FRAME_ALIGNMENT))
        .ok_or(JournalIoError::Corrupt)?;
    if bytes < SEALED_FOOTER_HEADER_BYTES + HASH_BYTES || bytes > maximum {
        return Err(JournalIoError::Corrupt);
    }
    Ok(bytes)
}

/// Reads a sealed extent's footer, verifying it against the frames it indexes.
pub(crate) fn read_sealed_extent_with_media(
    media: &dyn JournalMedia,
    path: &Path,
    database_id: DatabaseId,
) -> Result<SealedExtentFooter, JournalIoError> {
    let mut file = media.open_read(path).map_err(|_| JournalIoError::Io)?;
    let extent = read_selected_extent_header(&mut file, false)?;
    if extent.logical.database_id() != database_id {
        return Err(JournalIoError::Corrupt);
    }
    let file_len = usize::try_from(file.len().map_err(|_| JournalIoError::Io)?)
        .map_err(|_| JournalIoError::Corrupt)?;
    let footer_bytes = bounded_sealed_footer_bytes(file_len, extent.anchor_position)?;
    let mut entries = Vec::new();
    let walk = walk_extent_frames(
        &mut file,
        &extent,
        ExtentWalk::FromOrigin,
        file_len,
        |frame, placement| {
            entries.push(SealedFrameEntry::from_frame(frame, placement));
            Ok(())
        },
    )?;
    let tail = walk.tail.complete_bytes;
    if !walk.footer || extent.anchor_position != tail {
        return Err(JournalIoError::Corrupt);
    }
    let mut bytes = vec![0_u8; footer_bytes];
    file.read_exact_at(&mut bytes, tail)
        .map_err(|_| JournalIoError::Corrupt)?;
    let footer = SealedExtentFooter::decode(&bytes)?;
    if footer.generation != extent.generation || footer.entries != entries {
        return Err(JournalIoError::Corrupt);
    }
    Ok(footer)
}

/// One reusable, fully unreferenced extent; never an authoritative history name.
pub(crate) fn recycled_journal_path(database_path: &Path) -> PathBuf {
    let mut path = database_path.as_os_str().to_os_string();
    path.push(".riffjournal.recycled");
    PathBuf::from(path)
}

fn prepare_recycled_extent_with_media(
    media: &dyn JournalMedia,
    path: &Path,
    header: &JournalFileHeader,
) -> Result<(), JournalIoError> {
    let mut file = media
        .open_read_write(path)
        .map_err(|_| JournalIoError::Io)?;
    let current = read_selected_extent_header(&mut file, false)?;
    if current.logical.database_id() != header.database_id() {
        return Err(JournalIoError::Corrupt);
    }
    let length = usize::try_from(file.len().map_err(|_| JournalIoError::Io)?)
        .map_err(|_| JournalIoError::Corrupt)?;
    let full = extent_file_bytes(EXTENT_DATA_BYTES);
    // Preserve already allocated frame blocks. Only the trimmed tail needs
    // allocation, on the background preparation thread, never the writer.
    let zeros = [0_u8; 64 * 1024];
    let mut offset = length.min(full);
    let mut unsynced = 0_usize;
    while offset < full {
        let bytes = zeros.len().min(full - offset);
        file.write_all_at(&zeros[..bytes], offset)
            .map_err(|_| JournalIoError::Io)?;
        offset += bytes;
        unsynced += bytes;
        if unsynced >= EXTENT_ZERO_FILL_SYNC_BYTES {
            file.sync_data().map_err(|_| JournalIoError::Io)?;
            unsynced = 0;
        }
    }
    file.set_len(u64::try_from(full).map_err(|_| JournalIoError::Corrupt)?)
        .map_err(|_| JournalIoError::Io)?;
    let mut successor = current.successor(header.clone())?;
    successor.data_bytes = EXTENT_DATA_BYTES;
    file.write_all_at(
        &successor.encode(),
        usize::from(successor.slot) * EXTENT_HEADER_SLOT_BYTES,
    )
    .and_then(|()| file.sync_all())
    .map_err(|_| JournalIoError::Io)
}

/// Prepares a recycled or zero-filled extent off the writer's path, then
/// publishes it under the spare name. Rotation fences its generation above
/// the active predecessor before use.
pub(crate) fn prepare_spare_extent_with_media(
    media: &dyn JournalMedia,
    database_path: &Path,
    header: &JournalFileHeader,
    spare_lock: &std::sync::Mutex<()>,
) -> Result<bool, JournalIoError> {
    let spare = spare_journal_path(database_path);
    if media.try_exists(&spare).map_err(|_| JournalIoError::Io)? {
        return Ok(false);
    }
    let preparing = preparing_spare_journal_path(database_path);
    match media.remove_file(&preparing) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(_) => return Err(JournalIoError::Io),
    }
    let recycled = recycled_journal_path(database_path);
    let reuse = {
        let _guard = spare_lock.lock().map_err(|_| JournalIoError::Io)?;
        if media
            .try_exists(&recycled)
            .map_err(|_| JournalIoError::Io)?
        {
            media
                .rename(&recycled, &preparing)
                .map_err(|_| JournalIoError::Io)?;
            true
        } else {
            false
        }
    };
    if reuse {
        // The pool claim is complete. Foreground rotation need not wait for
        // this scratch-name directory sync while holding its spare lock.
        sync_parent_with_media(media, &preparing)?;
        prepare_recycled_extent_with_media(media, &preparing, header)?;
    } else {
        create_extent_file_with_media(media, &preparing, &ExtentHeader::initial(header.clone()))?;
    }
    let _guard = spare_lock.lock().map_err(|_| JournalIoError::Io)?;
    if media.try_exists(&spare).map_err(|_| JournalIoError::Io)? {
        media
            .remove_file(&preparing)
            .map_err(|_| JournalIoError::Io)?;
        return Ok(false);
    }
    media
        .rename(&preparing, &spare)
        .map_err(|_| JournalIoError::Io)?;
    sync_parent_with_media(media, &spare)?;
    Ok(true)
}

pub(crate) fn sync_parent_directory_with_media(
    media: &dyn JournalMedia,
    path: &Path,
) -> Result<(), JournalIoError> {
    media.sync_parent_all(path).map_err(|_| JournalIoError::Io)
}

fn align_up(value: usize, alignment: usize) -> Option<usize> {
    value
        .checked_add(alignment.checked_sub(1)?)
        .map(|value| value / alignment * alignment)
}

fn extent_frame_checksum(bytes: &[u8]) -> [u8; HASH_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(&bytes[..68]);
    hasher.update([0_u8; HASH_BYTES]);
    hasher.update(&bytes[100..]);
    hasher.finalize().into()
}

fn create_extent_file_with_media(
    media: &dyn JournalMedia,
    path: &Path,
    header: &ExtentHeader,
) -> Result<(), JournalIoError> {
    let mut file = media
        .create_new_read_write(path)
        .map_err(|_| JournalIoError::Io)?;
    let zeroes = vec![0_u8; 1024 * 1024];
    let mut remaining = extent_file_bytes(header.data_bytes);
    let mut unsynced = 0_usize;
    while remaining != 0 {
        let count = remaining.min(zeroes.len());
        let written = file.write_all(&zeroes[..count]).and_then(|()| {
            unsynced += count;
            if unsynced >= EXTENT_ZERO_FILL_SYNC_BYTES {
                unsynced = 0;
                file.sync_data()
            } else {
                Ok(())
            }
        });
        if let Err(error) = written {
            drop(file);
            let _ = media.remove_file(path);
            return Err(classify_extent_creation_error(&error));
        }
        remaining -= count;
    }
    if let Err(error) = file
        .write_all_at(&header.encode(), 0)
        .and_then(|()| file.sync_data())
    {
        drop(file);
        let _ = media.remove_file(path);
        return Err(classify_extent_creation_error(&error));
    }
    sync_parent_with_media(media, path)
}

fn classify_extent_creation_error(error: &io::Error) -> JournalIoError {
    if error.kind() == io::ErrorKind::StorageFull {
        JournalIoError::Capacity
    } else {
        JournalIoError::Io
    }
}

fn migrate_empty_legacy_journal_with_media(
    media: &dyn JournalMedia,
    path: &Path,
    expected: &JournalFileHeader,
) -> Result<(), JournalIoError> {
    let parent = path.parent().ok_or(JournalIoError::Io)?;
    let file_name = path.file_name().ok_or(JournalIoError::Io)?;
    let mut replacement_name = file_name.to_os_string();
    replacement_name.push(".extent-v3");
    let replacement = parent.join(replacement_name);
    match media.remove_file(&replacement) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(JournalIoError::Io),
    }
    create_extent_file_with_media(
        media,
        &replacement,
        &ExtentHeader::initial(expected.clone()),
    )?;
    media
        .rename(&replacement, path)
        .map_err(|_| JournalIoError::Io)?;
    sync_parent_with_media(media, path)
}

/// Replaces a recovered extent whose stamped capacity is not the configured one
/// (an extent written before ADR-0263 raised the ceilings) with a zero-filled
/// extent of the configured capacity carrying the same checkpoint anchor.
///
/// Runs only after recovery has re-anchored the extent, so its suffix is empty;
/// a non-empty suffix refuses rather than dropping frames. A crash leaves either
/// the old extent or the replacement at `path`, never a partial file: the
/// replacement is complete and synced before the rename.
pub(crate) fn normalize_extent_capacity_with_media(
    media: &dyn JournalMedia,
    path: &Path,
) -> Result<bool, JournalIoError> {
    normalize_extent_capacity_to(media, path, EXTENT_DATA_BYTES)
}

fn normalize_extent_capacity_to(
    media: &dyn JournalMedia,
    path: &Path,
    data_bytes: usize,
) -> Result<bool, JournalIoError> {
    let (_, tail, state) = scan_extent_with_media(media, path, None, |_, _| Ok(()))?;
    if state.header.data_bytes == data_bytes {
        return Ok(false);
    }
    if tail.transition_count != 0 || tail.incomplete_tail {
        return Err(JournalIoError::Corrupt);
    }
    let parent = path.parent().ok_or(JournalIoError::Io)?;
    let mut replacement_name = path.file_name().ok_or(JournalIoError::Io)?.to_os_string();
    replacement_name.push(".capacity");
    let replacement = parent.join(replacement_name);
    match media.remove_file(&replacement) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(_) => return Err(JournalIoError::Io),
    }
    let header = ExtentHeader {
        logical: state.header.logical.clone(),
        generation: state
            .header
            .generation
            .checked_add(1)
            .ok_or(JournalIoError::Corrupt)?,
        slot: 0,
        data_bytes,
        revision: 0,
        anchor_position: EXTENT_DATA_OFFSET,
    };
    create_extent_file_with_media(media, &replacement, &header)?;
    media
        .rename(&replacement, path)
        .map_err(|_| JournalIoError::Io)?;
    sync_parent_with_media(media, path)?;
    Ok(true)
}

fn sync_parent_with_media(media: &dyn JournalMedia, path: &Path) -> Result<(), JournalIoError> {
    media.sync_parent_data(path).map_err(|_| JournalIoError::Io)
}

/// Selects the current header slot. `exact_len` requires the file to have the
/// full extent length; a sealed extent is trimmed to its frames and footer.
fn read_selected_extent_header(
    file: &mut MediaFile,
    exact_len: bool,
) -> Result<ExtentHeader, JournalIoError> {
    let actual_len = usize::try_from(file.len().map_err(|_| JournalIoError::Io)?)
        .map_err(|_| JournalIoError::Corrupt)?;
    if actual_len < EXTENT_DATA_OFFSET {
        return Err(JournalIoError::Corrupt);
    }
    let mut valid = Vec::with_capacity(EXTENT_HEADER_SLOT_COUNT);
    for slot in 0..EXTENT_HEADER_SLOT_COUNT {
        let mut bytes = [0_u8; EXTENT_HEADER_SLOT_BYTES];
        file.read_exact_at(&mut bytes, slot * EXTENT_HEADER_SLOT_BYTES)
            .map_err(|_| JournalIoError::Io)?;
        if bytes.iter().all(|byte| *byte == 0) {
            continue;
        }
        if let Ok(header) = ExtentHeader::decode(
            &bytes,
            u16::try_from(slot).expect("extent header slot is bounded"),
        ) {
            valid.push(header);
        }
    }
    valid.sort_by_key(|header| (header.generation, header.revision));
    let selected = valid.pop().ok_or(JournalIoError::Corrupt)?;
    if valid.last().is_some_and(|header| {
        (header.generation, header.revision) == (selected.generation, selected.revision)
            && header != &selected
    }) || (exact_len && actual_len != extent_file_bytes(selected.data_bytes))
        || actual_len > extent_file_bytes(selected.data_bytes)
    {
        return Err(JournalIoError::Corrupt);
    }
    Ok(selected)
}

/// Real-filesystem wrapper retained for this module's unit tests; every
/// production caller routes through the media-parameterized form.
#[cfg(test)]
fn inspect_extent(path: &Path) -> Result<ExtentState, JournalIoError> {
    inspect_extent_with_media(&RealJournalMedia, path)
}

fn inspect_extent_with_media(
    media: &dyn JournalMedia,
    path: &Path,
) -> Result<ExtentState, JournalIoError> {
    let (_, _, state) = scan_extent_with_media(media, path, None, |_, _| Ok(()))?;
    Ok(state)
}

fn scan_extent_journal_with_media(
    media: &dyn JournalMedia,
    path: &Path,
    expected_database: DatabaseId,
    mut visit: impl FnMut(&JournalFrame, Option<StubLocator>) -> Result<(), JournalIoError>,
) -> Result<(JournalFileHeader, JournalScanTail), JournalIoError> {
    let (header, tail, _) =
        scan_extent_with_media(media, path, Some(expected_database), |frame, locator| {
            visit(frame, Some(locator))
        })?;
    Ok((header, tail))
}

/// Scans one extent from its anchor; `visit` receives each complete frame
/// with the locator its history stubs name (ADR-0263 Amendment 3).
fn scan_extent_with_media(
    media: &dyn JournalMedia,
    path: &Path,
    expected_database: Option<DatabaseId>,
    mut visit: impl FnMut(&JournalFrame, StubLocator) -> Result<(), JournalIoError>,
) -> Result<(JournalFileHeader, JournalScanTail, ExtentState), JournalIoError> {
    let mut file = media.open_read(path).map_err(|_| JournalIoError::Io)?;
    let extent = read_selected_extent_header(&mut file, true)?;
    if expected_database.is_some_and(|expected| extent.logical.database_id() != expected) {
        return Err(JournalIoError::Corrupt);
    }
    let file_bytes = extent_file_bytes(extent.data_bytes);
    let (database_id, generation) = (extent.logical.database_id(), extent.generation);
    let walk = walk_extent_frames(
        &mut file,
        &extent,
        ExtentWalk::FromAnchor,
        file_bytes,
        |frame, placement| {
            visit(
                frame,
                StubLocator {
                    database_id,
                    generation,
                    placement,
                },
            )
        },
    )?;
    if walk.footer {
        // A sealed footer is never part of an active or recovering extent.
        return Err(JournalIoError::Corrupt);
    }
    let logical = extent.logical.clone();
    let position = walk.tail.complete_bytes;
    Ok((
        logical,
        walk.tail,
        ExtentState {
            header: extent,
            next_position: position,
        },
    ))
}

/// Where a frame walk starts and which chain point it verifies against.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExtentWalk {
    /// Recovery: from the anchor, chained from the header's checkpoint, and
    /// bounded by the suffix ceilings.
    FromAnchor,
    /// Sealing and retained reads: every frame from the data offset, chained
    /// from the first frame's own predecessor, bounded only by the file.
    FromOrigin,
}

/// Physical placement of one complete frame inside an extent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FramePlacement {
    pub(crate) position: usize,
    pub(crate) padded_len: usize,
    pub(crate) frame_hash: [u8; HASH_BYTES],
}

/// The chain point one retained extent's first frame continues from.
type ChainPoint = (
    Option<CommitSequence>,
    Option<AdministrationSequence>,
    [u8; HASH_BYTES],
);

struct ExtentWalkResult {
    tail: JournalScanTail,
    /// The chain point the first frame continued from (origin walks only).
    origin: Option<ChainPoint>,
    /// A sealed footer begins at `tail.complete_bytes`.
    footer: bool,
}

/// Validates one complete padded physical frame read at `position` of an
/// extent of `generation`. `None` means its footer is not current: a torn tail.
fn decode_physical_frame(
    physical: &[u8],
    generation: u64,
    position: usize,
    encoded_len: usize,
    proven: Option<&[u8; HASH_BYTES]>,
) -> Result<Option<(JournalFrame, [u8; HASH_BYTES])>, JournalIoError> {
    let padded_len = physical.len();
    let frame_footer = padded_len
        .checked_sub(EXTENT_FRAME_FOOTER_BYTES)
        .filter(|footer| *footer >= EXTENT_FRAME_HEADER_BYTES + encoded_len)
        .ok_or(JournalIoError::Corrupt)?;
    let footer_is_current = physical[frame_footer..frame_footer + 8] == EXTENT_FRAME_FOOTER_MAGIC
        && read_u64(physical, frame_footer + 8).ok() == Some(generation)
        && usize::try_from(read_u64(physical, frame_footer + 16).unwrap_or(0)).ok()
            == Some(position)
        && physical[frame_footer + 24..frame_footer + 56] == physical[36..68]
        && physical[frame_footer + 56..frame_footer + 64]
            .iter()
            .all(|byte| *byte == 0);
    if !footer_is_current {
        return Ok(None);
    }
    if physical[100..EXTENT_FRAME_HEADER_BYTES]
        .iter()
        .any(|byte| *byte != 0)
        || physical[EXTENT_FRAME_HEADER_BYTES + encoded_len..frame_footer]
            .iter()
            .any(|byte| *byte != 0)
        || extent_frame_checksum(physical).as_slice() != &physical[68..100]
    {
        return Err(JournalIoError::Corrupt);
    }
    let logical = &physical[EXTENT_FRAME_HEADER_BYTES..EXTENT_FRAME_HEADER_BYTES + encoded_len];
    let admitted = if proven.is_none() {
        recovery_memory::admit_physical_recovery(
            logical,
            &read_array::<HASH_BYTES>(physical, 36).map_err(|_| JournalIoError::Corrupt)?,
        )?
    } else {
        None
    };
    let (frame, frame_hash) = match (proven, admitted) {
        (Some(proven), _) => {
            JournalFrame::decode_proven(logical, proven).map(|frame| (frame, *proven))
        }
        (None, Some(admitted)) => JournalFrame::decode_admitted(admitted),
        (None, None) => JournalFrame::decode(logical),
    }
    .map_err(|_| JournalIoError::Corrupt)?;
    if frame_hash.as_slice() != &physical[36..68] {
        return Err(JournalIoError::Corrupt);
    }
    Ok(Some((frame, frame_hash)))
}

/// Reads the one retained frame a history stub names (ADR-0263 Amendment 3):
/// the extent of `generation` at any of its names, the complete physical frame
/// at `position`, validated like a scan and required to hash to `frame_hash`.
pub(crate) fn read_retained_frame_with_media(
    media: &dyn JournalMedia,
    database_path: &Path,
    database_id: DatabaseId,
    placement: FramePlacement,
    generation: u64,
) -> Result<JournalFrame, JournalIoError> {
    read_retained_frame_from_with_media(
        media,
        &[
            sealed_journal_path(database_path, generation),
            journal_path(database_path),
            checkpoint_journal_path(database_path),
            sealing_journal_path(database_path),
        ],
        database_id,
        placement,
        generation,
    )
}

/// [`read_retained_frame_with_media`] over explicit candidate extent files,
/// such as a backup's artifacts; the first of `generation` is read.
pub(crate) fn read_retained_frame_from_with_media(
    media: &dyn JournalMedia,
    candidates: &[PathBuf],
    database_id: DatabaseId,
    placement: FramePlacement,
    generation: u64,
) -> Result<JournalFrame, JournalIoError> {
    read_retained_frame_from_with_admission(
        media,
        candidates,
        database_id,
        placement,
        generation,
        |_| Ok(()),
    )
    .map(|(frame, ())| frame)
}

pub(crate) fn read_provider_retained_frame_v1(
    media: &dyn JournalMedia,
    candidates: &[PathBuf],
    database_id: DatabaseId,
    placement: FramePlacement,
    generation: u64,
    budget: &riffdb_types::exact_provider_budget_v1::ProviderBudget,
) -> Result<
    (
        JournalFrame,
        riffdb_types::exact_provider_budget_v1::Reservation,
    ),
    JournalIoError,
> {
    let maximum = extent_frame_bytes(MAX_JOURNAL_FRAME_BYTES).ok_or(JournalIoError::Corrupt)?;
    if placement.padded_len > maximum {
        return Err(JournalIoError::Corrupt);
    }
    let _physical_work = budget
        .reserve(
            u64::try_from(placement.padded_len)
                .ok()
                .and_then(|bytes| bytes.checked_mul(2))
                .and_then(|bytes| bytes.checked_add(32 * 1024))
                .ok_or(JournalIoError::Corrupt)?,
        )
        .map_err(|_| JournalIoError::Capacity)?;
    read_retained_frame_from_with_admission(
        media,
        candidates,
        database_id,
        placement,
        generation,
        |logical| {
            if logical.len() < FRAME_HEADER_BYTES_V2 + FRAME_FOOTER_BYTES
                || logical.len() > MAX_JOURNAL_FRAME_BYTES
            {
                return Err(JournalIoError::Corrupt);
            }
            let count =
                usize::try_from(read_u32(logical, 68).map_err(|_| JournalIoError::Corrupt)?)
                    .map_err(|_| JournalIoError::Corrupt)?;
            let payload =
                usize::try_from(read_u32(logical, 72).map_err(|_| JournalIoError::Corrupt)?)
                    .map_err(|_| JournalIoError::Corrupt)?;
            if count > payload / MUTATION_HEADER_BYTES || payload > logical.len() {
                return Err(JournalIoError::Corrupt);
            }
            let fold = riffdb_storage_api::provider_receipt_work_bound_v1(count, logical.len())
                .map_err(|_| JournalIoError::Capacity)?;
            let frame = count
                .checked_add(4)
                .and_then(|count| count.checked_mul(std::mem::size_of::<JournalMutation>()))
                .and_then(|bytes| bytes.checked_mul(4))
                .and_then(|bytes| u64::try_from(bytes).ok())
                .and_then(|bytes| fold.checked_add(bytes))
                .ok_or(JournalIoError::Capacity)?;
            budget.reserve(frame).map_err(|_| JournalIoError::Capacity)
        },
    )
}

fn read_retained_frame_from_with_admission<R>(
    media: &dyn JournalMedia,
    candidates: &[PathBuf],
    database_id: DatabaseId,
    placement: FramePlacement,
    generation: u64,
    mut admit: impl FnMut(&[u8]) -> Result<R, JournalIoError>,
) -> Result<(JournalFrame, R), JournalIoError> {
    let maximum = extent_frame_bytes(MAX_JOURNAL_FRAME_BYTES).ok_or(JournalIoError::Corrupt)?;
    if placement.padded_len < EXTENT_FRAME_HEADER_BYTES + EXTENT_FRAME_FOOTER_BYTES
        || placement.padded_len > maximum
    {
        return Err(JournalIoError::Corrupt);
    }
    for path in candidates {
        let mut file = match media.open_read(path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(_) => return Err(JournalIoError::Io),
        };
        let extent = read_selected_extent_header(&mut file, false)?;
        if extent.generation != generation {
            continue;
        }
        // The anchor may lag a materialized frame until the extent is sealed;
        // the frame hash, not the anchor, proves the stub's frame.
        if extent.logical.database_id() != database_id || placement.position < EXTENT_DATA_OFFSET {
            return Err(JournalIoError::Corrupt);
        }
        let mut physical = vec![0_u8; placement.padded_len];
        file.read_exact_at(&mut physical, placement.position)
            .map_err(|_| JournalIoError::Corrupt)?;
        if physical[..8] != EXTENT_FRAME_MAGIC
            || read_u64(&physical, 12).ok() != Some(generation)
            || read_u16(&physical, 8).ok() != Some(EXTENT_FRAME_VERSION)
            || usize::try_from(read_u64(&physical, 20).unwrap_or(0)).ok()
                != Some(placement.position)
            || usize::try_from(read_u32(&physical, 32).unwrap_or(0)).ok()
                != Some(placement.padded_len)
        {
            return Err(JournalIoError::Corrupt);
        }
        let encoded_len =
            usize::try_from(read_u32(&physical, 28).map_err(|_| JournalIoError::Corrupt)?)
                .map_err(|_| JournalIoError::Corrupt)?;
        let logical_end = EXTENT_FRAME_HEADER_BYTES
            .checked_add(encoded_len)
            .ok_or(JournalIoError::Corrupt)?;
        let logical = physical
            .get(EXTENT_FRAME_HEADER_BYTES..logical_end)
            .ok_or(JournalIoError::Corrupt)?;
        let admitted = admit(logical)?;
        let (frame, frame_hash) = decode_physical_frame(
            &physical,
            generation,
            placement.position,
            encoded_len,
            Some(&placement.frame_hash),
        )?
        .ok_or(JournalIoError::Corrupt)?;
        if frame_hash != placement.frame_hash || frame.database_id() != database_id {
            return Err(JournalIoError::Corrupt);
        }
        return Ok((frame, admitted));
    }
    Err(JournalIoError::Corrupt)
}

fn walk_extent_frames(
    file: &mut MediaFile,
    extent: &ExtentHeader,
    walk: ExtentWalk,
    file_bytes: usize,
    visit: impl FnMut(&JournalFrame, FramePlacement) -> Result<(), JournalIoError>,
) -> Result<ExtentWalkResult, JournalIoError> {
    walk_extent_frames_with_limits(file, extent, walk, file_bytes, false, visit)
}

fn walk_extent_frames_with_limits(
    file: &mut MediaFile,
    extent: &ExtentHeader,
    walk: ExtentWalk,
    file_bytes: usize,
    online_source: bool,
    mut visit: impl FnMut(&JournalFrame, FramePlacement) -> Result<(), JournalIoError>,
) -> Result<ExtentWalkResult, JournalIoError> {
    let mut previous_hash = extent.logical.checkpoint_frame_hash();
    let mut previous_sequence = extent.logical.checkpoint_sequence();
    let mut previous_administration_sequence = extent.logical.checkpoint_administration_sequence();
    let mut position = match walk {
        ExtentWalk::FromAnchor => extent.anchor_position,
        ExtentWalk::FromOrigin => EXTENT_DATA_OFFSET,
    };
    let mut origin = None;
    let mut footer = false;
    let mut transition_count = 0_usize;
    let mut command_count = 0_usize;
    let mut audit_count = 0_usize;
    let mut encoded_byte_count = 0_usize;
    let mut incomplete_tail = false;
    while position < file_bytes {
        let mut physical_header = [0_u8; EXTENT_FRAME_HEADER_BYTES];
        file.read_exact_at(&mut physical_header, position)
            .map_err(|_| JournalIoError::Io)?;
        if physical_header.iter().all(|byte| *byte == 0) {
            break;
        }
        if physical_header[..8] == SEALED_FOOTER_MAGIC {
            footer = true;
            break;
        }
        if physical_header[..8] != EXTENT_FRAME_MAGIC {
            incomplete_tail = true;
            break;
        }
        let generation = read_u64(&physical_header, 12).map_err(|_| JournalIoError::Corrupt)?;
        if generation < extent.generation {
            break;
        }
        if generation != extent.generation
            || read_u16(&physical_header, 8).map_err(|_| JournalIoError::Corrupt)?
                != EXTENT_FRAME_VERSION
            || usize::from(read_u16(&physical_header, 10).map_err(|_| JournalIoError::Corrupt)?)
                != EXTENT_FRAME_HEADER_BYTES
            || usize::try_from(read_u64(&physical_header, 20).map_err(|_| JournalIoError::Corrupt)?)
                .ok()
                != Some(position)
        {
            return Err(JournalIoError::Corrupt);
        }
        let encoded_len =
            usize::try_from(read_u32(&physical_header, 28).map_err(|_| JournalIoError::Corrupt)?)
                .map_err(|_| JournalIoError::Corrupt)?;
        let padded_len =
            usize::try_from(read_u32(&physical_header, 32).map_err(|_| JournalIoError::Corrupt)?)
                .map_err(|_| JournalIoError::Corrupt)?;
        let minimum = EXTENT_FRAME_HEADER_BYTES
            .checked_add(encoded_len)
            .and_then(|value| value.checked_add(EXTENT_FRAME_FOOTER_BYTES))
            .ok_or(JournalIoError::Corrupt)?;
        if encoded_len == 0
            || encoded_len > MAX_JOURNAL_FRAME_BYTES
            || padded_len < minimum
            || padded_len % EXTENT_FRAME_ALIGNMENT != 0
            || position
                .checked_add(padded_len)
                .filter(|end| *end <= file_bytes)
                .is_none()
        {
            return Err(JournalIoError::Corrupt);
        }
        // Source admission proves a much smaller suffix before taking it.
        // Recheck raw-media lengths before any payload allocation or decode;
        // corrupt headers cannot turn that proof into an extent-sized read.
        if online_source
            && (transition_count >= SOURCE_SUFFIX_TRANSITIONS
                || encoded_byte_count
                    .checked_add(encoded_len)
                    .is_none_or(|bytes| bytes > SOURCE_SUFFIX_BYTES)
                || extent_frame_bytes(encoded_len).is_none_or(|maximum| padded_len > maximum))
        {
            return Err(JournalIoError::Corrupt);
        }
        let mut physical = vec![0_u8; padded_len];
        file.read_exact_at(&mut physical, position)
            .map_err(|_| JournalIoError::Io)?;
        // Recovery re-derives every frame fully. Sealing and retained reads
        // check each frame's bytes against the hash its header recorded when
        // it was written and validated, and skip re-decoding its segments.
        let recorded =
            read_array::<HASH_BYTES>(&physical_header, 36).map_err(|_| JournalIoError::Corrupt)?;
        let proven = (walk == ExtentWalk::FromOrigin).then_some(&recorded);
        let Some((frame, frame_hash)) =
            decode_physical_frame(&physical, extent.generation, position, encoded_len, proven)?
        else {
            incomplete_tail = true;
            break;
        };
        // The decoded frame owns its bytes. Release the physical allocation
        // before receipt planning/materialization creates its bounded scratch.
        drop(physical);
        if walk == ExtentWalk::FromOrigin && origin.is_none() {
            previous_hash = frame.previous_hash();
            previous_sequence = frame.predecessor_sequence();
            previous_administration_sequence = frame.predecessor_administration_sequence();
            origin = Some((
                previous_sequence,
                previous_administration_sequence,
                previous_hash,
            ));
        }
        // A direct commit between two journaled frames advances the frontier
        // without a frame; the hash chain stays unbroken. Recovery walks from an
        // anchor the drain re-established, so its frames chain exactly; a walk
        // from the origin crosses those gaps and accepts only a forward jump.
        let chained = match walk {
            ExtentWalk::FromAnchor => {
                frame.predecessor_sequence() == previous_sequence
                    && frame.predecessor_administration_sequence()
                        == previous_administration_sequence
            }
            ExtentWalk::FromOrigin => {
                frame.predecessor_sequence() >= previous_sequence
                    && frame.predecessor_administration_sequence()
                        >= previous_administration_sequence
            }
        };
        if frame_hash.as_slice() != &physical_header[36..68]
            || frame.database_id() != extent.logical.database_id()
            || frame.previous_hash() != previous_hash
            || !chained
        {
            return Err(JournalIoError::Corrupt);
        }
        transition_count = transition_count
            .checked_add(usize::from(frame.transition_count()))
            .ok_or(JournalIoError::Corrupt)?;
        command_count = command_count
            .checked_add(usize::from(frame.command_count()))
            .ok_or(JournalIoError::Corrupt)?;
        audit_count = audit_count
            .checked_add(usize::from(frame.audit_count()))
            .ok_or(JournalIoError::Corrupt)?;
        encoded_byte_count = encoded_byte_count
            .checked_add(encoded_len)
            .ok_or(JournalIoError::Corrupt)?;
        if walk == ExtentWalk::FromAnchor
            && (transition_count
                > if online_source {
                    SOURCE_SUFFIX_TRANSITIONS
                } else {
                    MAX_JOURNAL_SUFFIX_TRANSITIONS
                }
                || encoded_byte_count
                    > if online_source {
                        SOURCE_SUFFIX_BYTES
                    } else {
                        MAX_JOURNAL_SUFFIX_BYTES
                    })
        {
            return Err(JournalIoError::Corrupt);
        }
        visit(
            &frame,
            FramePlacement {
                position,
                padded_len,
                frame_hash,
            },
        )?;
        previous_sequence = frame.covered_sequence();
        previous_administration_sequence = frame.covered_administration_sequence();
        previous_hash = frame_hash;
        position = position
            .checked_add(padded_len)
            .ok_or(JournalIoError::Corrupt)?;
    }
    Ok(ExtentWalkResult {
        tail: JournalScanTail {
            last_sequence: previous_sequence,
            last_administration_sequence: previous_administration_sequence,
            last_hash: previous_hash,
            incomplete_tail,
            complete_bytes: position,
            transition_count,
            command_count,
            audit_count,
        },
        origin,
        footer,
    })
}

pub(crate) fn scan_journal(
    path: &Path,
    expected_database: DatabaseId,
    visit: impl FnMut(&JournalFrame) -> Result<(), JournalIoError>,
) -> Result<Option<(JournalFileHeader, JournalScanTail)>, JournalIoError> {
    scan_journal_with_media(&RealJournalMedia, path, expected_database, visit)
}

pub(crate) fn scan_journal_with_media(
    media: &dyn JournalMedia,
    path: &Path,
    expected_database: DatabaseId,
    mut visit: impl FnMut(&JournalFrame) -> Result<(), JournalIoError>,
) -> Result<Option<(JournalFileHeader, JournalScanTail)>, JournalIoError> {
    scan_located_journal_with_media(media, path, expected_database, |frame, _| visit(frame))
}

/// [`scan_journal_with_media`], also giving each frame of an extent journal
/// the locator its history stubs name. A legacy journal has none.
pub(crate) fn scan_located_journal_with_media(
    media: &dyn JournalMedia,
    path: &Path,
    expected_database: DatabaseId,
    mut visit: impl FnMut(&JournalFrame, Option<StubLocator>) -> Result<(), JournalIoError>,
) -> Result<Option<(JournalFileHeader, JournalScanTail)>, JournalIoError> {
    let mut file = match media.open_read(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(JournalIoError::Io),
    };
    let mut magic = [0_u8; 8];
    file.read_exact(&mut magic)
        .map_err(|_| JournalIoError::Corrupt)?;
    if magic == EXTENT_MAGIC {
        return scan_extent_journal_with_media(media, path, expected_database, visit).map(Some);
    }
    if magic != FILE_MAGIC {
        return Err(JournalIoError::Corrupt);
    }
    let result =
        scan_legacy_journal_with_media(media, path, expected_database, |frame| visit(frame, None))?
            .ok_or(JournalIoError::Corrupt)?;
    if result.1.transition_count != 0 || result.1.incomplete_tail {
        return Err(JournalIoError::LegacyNonEmpty(path.to_path_buf()));
    }
    Ok(Some(result))
}

fn scan_legacy_journal_with_media(
    media: &dyn JournalMedia,
    path: &Path,
    expected_database: DatabaseId,
    mut visit: impl FnMut(&JournalFrame) -> Result<(), JournalIoError>,
) -> Result<Option<(JournalFileHeader, JournalScanTail)>, JournalIoError> {
    let mut file = match media.open_read(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(JournalIoError::Io),
    };
    let mut header_bytes = [0_u8; FILE_HEADER_BYTES];
    file.read_exact(&mut header_bytes)
        .map_err(|_| JournalIoError::Corrupt)?;
    let header = JournalFileHeader::decode(&header_bytes).map_err(|_| JournalIoError::Corrupt)?;
    if header.database_id() != expected_database {
        return Err(JournalIoError::Corrupt);
    }
    let mut previous_hash = header.checkpoint_frame_hash();
    let mut previous_sequence = header.checkpoint_sequence();
    let mut previous_administration_sequence = header.checkpoint_administration_sequence();
    let mut complete_bytes = FILE_HEADER_BYTES;
    let mut transition_count = 0_usize;
    let mut command_count = 0_usize;
    let mut audit_count = 0_usize;
    loop {
        let mut frame_header = [0_u8; FRAME_HEADER_BYTES_V2];
        let read = read_until_full_or_eof(&mut file, &mut frame_header)?;
        if read == 0 {
            return Ok(Some((
                header,
                JournalScanTail {
                    last_sequence: previous_sequence,
                    last_administration_sequence: previous_administration_sequence,
                    last_hash: previous_hash,
                    incomplete_tail: false,
                    complete_bytes,
                    transition_count,
                    command_count,
                    audit_count,
                },
            )));
        }
        if read != FRAME_HEADER_BYTES_V2 {
            return Ok(Some((
                header,
                JournalScanTail {
                    last_sequence: previous_sequence,
                    last_administration_sequence: previous_administration_sequence,
                    last_hash: previous_hash,
                    incomplete_tail: true,
                    complete_bytes,
                    transition_count,
                    command_count,
                    audit_count,
                },
            )));
        }
        let payload_len =
            usize::try_from(read_u32(&frame_header, 72).map_err(|_| JournalIoError::Corrupt)?)
                .map_err(|_| JournalIoError::Corrupt)?;
        let remaining = payload_len
            .checked_add(FRAME_FOOTER_BYTES)
            .ok_or(JournalIoError::Corrupt)?;
        let total = FRAME_HEADER_BYTES_V2
            .checked_add(remaining)
            .ok_or(JournalIoError::Corrupt)?;
        if total > MAX_JOURNAL_FRAME_BYTES {
            return Err(JournalIoError::Corrupt);
        }
        let mut encoded = Vec::with_capacity(total);
        encoded.extend_from_slice(&frame_header);
        let mut rest = vec![0_u8; remaining];
        let read = read_until_full_or_eof(&mut file, &mut rest)?;
        if read != remaining {
            return Ok(Some((
                header,
                JournalScanTail {
                    last_sequence: previous_sequence,
                    last_administration_sequence: previous_administration_sequence,
                    last_hash: previous_hash,
                    incomplete_tail: true,
                    complete_bytes,
                    transition_count,
                    command_count,
                    audit_count,
                },
            )));
        }
        encoded.extend_from_slice(&rest);
        let (frame, frame_hash) =
            JournalFrame::decode(&encoded).map_err(|_| JournalIoError::Corrupt)?;
        drop(rest);
        drop(encoded);
        if frame.database_id() != expected_database
            || frame.previous_hash() != previous_hash
            || frame.predecessor_sequence() != previous_sequence
            || frame.predecessor_administration_sequence() != previous_administration_sequence
        {
            return Err(JournalIoError::Corrupt);
        }
        complete_bytes = complete_bytes
            .checked_add(total)
            .ok_or(JournalIoError::Corrupt)?;
        transition_count = transition_count
            .checked_add(usize::from(frame.transition_count()))
            .ok_or(JournalIoError::Corrupt)?;
        command_count = command_count
            .checked_add(usize::from(frame.command_count()))
            .ok_or(JournalIoError::Corrupt)?;
        audit_count = audit_count
            .checked_add(usize::from(frame.audit_count()))
            .ok_or(JournalIoError::Corrupt)?;
        if complete_bytes.saturating_sub(FILE_HEADER_BYTES) > MAX_JOURNAL_SUFFIX_BYTES
            || transition_count > MAX_JOURNAL_SUFFIX_TRANSITIONS
        {
            return Err(JournalIoError::Corrupt);
        }
        visit(&frame)?;
        previous_sequence = frame.covered_sequence();
        previous_administration_sequence = frame.covered_administration_sequence();
        previous_hash = frame_hash;
    }
}

/// Restores one bounded journal epoch before any structural startup evidence is
/// collected. The only accepted redb positions are the exact checkpoint in the
/// file header or the complete journal tail left by a crash after checkpoint
/// and before reclamation.
/// Real-filesystem wrapper retained for this module's unit tests; every
/// production caller routes through the media-parameterized form.
#[cfg(test)]
pub(crate) fn recover_journal(
    database: &Database,
    database_path: &Path,
    database_id: DatabaseId,
) -> Result<(), JournalIoError> {
    recover_journal_path(database, &journal_path(database_path), database_id)
}

pub(crate) fn recover_journal_with_media(
    media: &dyn JournalMedia,
    database: &Database,
    database_path: &Path,
    database_id: DatabaseId,
) -> Result<(), JournalIoError> {
    recover_journal_path_with_media(media, database, &journal_path(database_path), database_id)
}

/// Real-filesystem wrapper retained for this module's unit tests; every
/// production caller routes through the media-parameterized form.
#[cfg(test)]
pub(crate) fn recover_journal_path(
    database: &Database,
    path: &Path,
    database_id: DatabaseId,
) -> Result<(), JournalIoError> {
    recover_journal_path_with_media(&RealJournalMedia, database, path, database_id)
}

pub(crate) fn recover_journal_path_with_media(
    media: &dyn JournalMedia,
    database: &Database,
    path: &Path,
    database_id: DatabaseId,
) -> Result<(), JournalIoError> {
    offline_integrity::inspect_journal_with_media(media, database, path, database_id, true)
}

/// The same complete recovery plan, without replay or journal mutation.
/// Complete V3 history validation attached to the exact owned read snapshot.
/// This has no serialized representation, mutation authority or public selector.
pub(crate) struct VerifiedMaterializedHistory {
    read: ReadTransaction,
    restore: Option<crate::restore_history::CompleteRestoreHistory>,
}

impl VerifiedMaterializedHistory {
    pub(crate) fn capture(read: ReadTransaction) -> Result<Option<Self>, JournalIoError> {
        if !crate::changelog_v3_journal::has_recovery_roots(&read)
            .map_err(|_| JournalIoError::Corrupt)?
        {
            return Ok(None);
        }
        crate::changelog_v3_roots::validate_retained_history(&read)
            .map_err(|_| JournalIoError::Corrupt)?
            .ok_or(JournalIoError::Corrupt)?;
        Ok(Some(Self {
            read,
            restore: None,
        }))
    }

    pub(crate) fn capture_owned_restore(
        read: ReadTransaction,
        binding: &crate::maintenance::RestoreIntegrityBinding,
    ) -> Result<Option<Self>, JournalIoError> {
        let Some(restore) = crate::restore_history::CompleteRestoreHistory::capture(&read, binding)
            .map_err(|_| JournalIoError::Corrupt)?
        else {
            return Ok(None);
        };
        Ok(Some(Self {
            read,
            restore: Some(restore),
        }))
    }

    pub(crate) fn reuse_owned_restore(
        read: ReadTransaction,
        binding: &crate::maintenance::RestoreIntegrityBinding,
        restore: crate::restore_history::CompleteRestoreHistory,
    ) -> Result<Option<Self>, JournalIoError> {
        restore
            .verify_read(&read, binding)
            .map_err(|_| JournalIoError::Corrupt)?;
        Ok(Some(Self {
            read,
            restore: Some(restore),
        }))
    }

    pub(crate) fn take_restore_history(
        &mut self,
    ) -> Option<crate::restore_history::CompleteRestoreHistory> {
        self.restore.take()
    }

    pub(crate) fn verify_journal(
        &self,
        media: &dyn JournalMedia,
        path: &Path,
        database_id: DatabaseId,
    ) -> Result<(), JournalIoError> {
        recovery_stream::inspect_materialized_snapshot(media, &self.read, path, database_id)
    }

    pub(crate) fn into_read(self) -> ReadTransaction {
        self.read
    }
}

pub(crate) fn verify_materialized_journal_with_media(
    media: &dyn JournalMedia,
    database: &Database,
    path: &Path,
    database_id: DatabaseId,
) -> Result<(), JournalIoError> {
    offline_integrity::inspect_journal_with_media(media, database, path, database_id, false)
}

fn read_redb_frontier(
    database: &Database,
) -> Result<(Option<CommitSequence>, Option<AdministrationSequence>), JournalIoError> {
    let transaction = database.begin_read().map_err(|_| JournalIoError::Io)?;
    read_redb_frontier_from_snapshot(&transaction)
}

fn read_redb_frontier_from_snapshot(
    transaction: &ReadTransaction,
) -> Result<(Option<CommitSequence>, Option<AdministrationSequence>), JournalIoError> {
    let commit = transaction
        .open_table(COMMITS)
        .map_err(|_| JournalIoError::Corrupt)?;
    let (mut commit_sequence, command_audit_sequence) =
        match commit.last().map_err(|_| JournalIoError::Corrupt)? {
            None => (None, None),
            Some((key, value)) => {
                let physical = decode_application_sequence_key(key.value())
                    .map_err(|_| JournalIoError::Corrupt)?;
                match crate::history_stub::decode_segment_value(value.value()) {
                    Ok(segment) => {
                        let segment = segment.value();
                        if segment.first_commit_sequence() != physical {
                            return Err(JournalIoError::Corrupt);
                        }
                        let terminal = segment
                            .commands()
                            .last()
                            .ok_or(JournalIoError::Corrupt)?
                            .base()
                            .terminal_audit()
                            .administration_sequence();
                        (Some(segment.last_commit_sequence()), Some(terminal))
                    }
                    // Non-segment current and historical commit rows are resolved
                    // by their physical sequence. If bytes claim to be a damaged
                    // segment, a multi-command journal frontier cannot match this
                    // conservative first-sequence fallback; a singleton remains
                    // subject to the complete startup structural decoder.
                    Err(_) => (Some(physical), None),
                }
            }
        };
    if commit_sequence.is_none() {
        commit_sequence = crate::retention::load_watermark(transaction)
            .map_err(|_| JournalIoError::Corrupt)?
            .and_then(|watermark| CommitSequence::new(watermark.watermark_sequence()));
    }
    let audit = transaction
        .open_table(AUDIT)
        .map_err(|_| JournalIoError::Corrupt)?;
    let physical_audit_sequence = audit
        .last()
        .map_err(|_| JournalIoError::Corrupt)?
        .map(|(key, _)| decode_audit_key(key.value()))
        .transpose()
        .map_err(|_| JournalIoError::Corrupt)?;
    let administration_sequence = match (physical_audit_sequence, command_audit_sequence) {
        (Some(physical), Some(command)) if physical == command => {
            return Err(JournalIoError::Corrupt);
        }
        (Some(physical), Some(command)) => Some(physical.max(command)),
        (Some(sequence), None) | (None, Some(sequence)) => Some(sequence),
        (None, None) => None,
    };
    Ok((commit_sequence, administration_sequence))
}

/// Storage-internal proof that ordinary recovery left no journal suffix for
/// offline retention to race or discard (ADR-0085 Amendment 4).
pub(crate) struct RetentionJournalRebaseWitness {
    _database_id: DatabaseId,
    _application_frontier: Option<CommitSequence>,
    _administration_frontier: Option<AdministrationSequence>,
    _terminal_frame_hash: [u8; HASH_BYTES],
    _selected_generation: Option<u64>,
}

pub(crate) fn verify_retention_journal_rebase_with_media(
    media: &dyn JournalMedia,
    database_path: &Path,
    database_id: DatabaseId,
    application_frontier: Option<CommitSequence>,
    administration_frontier: Option<AdministrationSequence>,
) -> Result<RetentionJournalRebaseWitness, JournalIoError> {
    for non_authoritative in [
        checkpoint_journal_path(database_path),
        spare_journal_path(database_path),
    ] {
        if media
            .try_exists(&non_authoritative)
            .map_err(|_| JournalIoError::Io)?
        {
            return Err(JournalIoError::Corrupt);
        }
    }
    let active = journal_path(database_path);
    if !media.try_exists(&active).map_err(|_| JournalIoError::Io)? {
        return Ok(RetentionJournalRebaseWitness {
            _database_id: database_id,
            _application_frontier: application_frontier,
            _administration_frontier: administration_frontier,
            _terminal_frame_hash: [0; HASH_BYTES],
            _selected_generation: None,
        });
    }
    let (header, tail, state) =
        scan_extent_with_media(media, &active, Some(database_id), |_, _| Ok(()))?;
    if header.checkpoint_sequence() != application_frontier
        || header.checkpoint_administration_sequence() != administration_frontier
        || tail.last_sequence != application_frontier
        || tail.last_administration_sequence != administration_frontier
        || tail.last_hash != header.checkpoint_frame_hash()
        || tail.incomplete_tail
        || tail.transition_count != 0
        || tail.command_count != 0
        || tail.audit_count != 0
    {
        return Err(JournalIoError::Corrupt);
    }
    Ok(RetentionJournalRebaseWitness {
        _database_id: database_id,
        _application_frontier: application_frontier,
        _administration_frontier: administration_frontier,
        _terminal_frame_hash: tail.last_hash,
        _selected_generation: Some(state.header.generation),
    })
}

/// Verifies the final clean-close journal boundary and returns the SHA-256 of
/// the selected extent's complete canonical encoded header slot (ADR-0157).
pub(crate) fn verify_clean_close_header_digest_with_media(
    media: &dyn JournalMedia,
    database_path: &Path,
    database_id: DatabaseId,
    application_frontier: Option<CommitSequence>,
    administration_frontier: Option<AdministrationSequence>,
) -> Result<[u8; HASH_BYTES], JournalIoError> {
    for non_authoritative in [
        checkpoint_journal_path(database_path),
        spare_journal_path(database_path),
    ] {
        if media
            .try_exists(&non_authoritative)
            .map_err(|_| JournalIoError::Io)?
        {
            return Err(JournalIoError::Corrupt);
        }
    }
    let active = journal_path(database_path);
    if !media.try_exists(&active).map_err(|_| JournalIoError::Io)? {
        return Err(JournalIoError::Corrupt);
    }
    let (header, tail, state) =
        scan_extent_with_media(media, &active, Some(database_id), |_, _| Ok(()))?;
    if header.checkpoint_sequence() != application_frontier
        || header.checkpoint_administration_sequence() != administration_frontier
        || tail.last_sequence != application_frontier
        || tail.last_administration_sequence != administration_frontier
        || tail.last_hash != header.checkpoint_frame_hash()
        || tail.incomplete_tail
        || tail.transition_count != 0
        || tail.command_count != 0
        || tail.audit_count != 0
    {
        return Err(JournalIoError::Corrupt);
    }
    Ok(digest(&state.header.encode()))
}

fn replay_frames(
    database: &Database,
    frames: &[JournalFrame],
    locators: &[Option<StubLocator>],
) -> Result<(), JournalIoError> {
    if frames.len() != locators.len() {
        return Err(JournalIoError::Corrupt);
    }
    let mut transaction = database.begin_write().map_err(|_| JournalIoError::Io)?;
    transaction.set_two_phase_commit(true);
    transaction
        .set_durability(Durability::Immediate)
        .map_err(|_| JournalIoError::Io)?;
    let mut chain = crate::changelog_v3_journal::ChainedFrameMaterialization::default();
    for (frame, locator) in frames.iter().zip(locators) {
        if crate::changelog_v3_journal::has_receipt_source(frame) {
            chain
                .materialize(&transaction, frame, *locator)
                .map_err(|_| JournalIoError::Corrupt)?;
        } else {
            chain
                .break_chain(&transaction)
                .map_err(|_| JournalIoError::Corrupt)?;
            for mutation in frame.mutations() {
                apply_located_mutation(&transaction, mutation, *locator)?;
            }
        }
    }
    chain
        .finish(&transaction)
        .map_err(|_| JournalIoError::Corrupt)?;
    #[cfg(test)]
    crate::changelog_v3_journal::recovery_crash_edge("staged");
    transaction.commit().map_err(|_| JournalIoError::Io)?;
    #[cfg(test)]
    crate::changelog_v3_journal::recovery_crash_edge("committed");
    Ok(())
}

/// Applies one mutation of a recovered or drained frame. With the frame's
/// locator, a command segment it freshly inserts is written as its history
/// stub (ADR-0263 Amendment 3); every other mutation is applied as recorded.
pub(crate) fn apply_located_mutation(
    transaction: &redb::WriteTransaction,
    mutation: &JournalMutation,
    locator: Option<StubLocator>,
) -> Result<(), JournalIoError> {
    if let (Some(locator), JournalTable::Commits, Some(value), None) = (
        locator,
        mutation.table(),
        mutation.value(),
        mutation.expected_hash(),
    ) && let Some(stub) = locator
        .segment_stub(value)
        .map_err(|_| JournalIoError::Corrupt)?
    {
        return apply_byte_mutation_parts(transaction, COMMITS, mutation.key(), Some(&stub), None);
    }
    apply_mutation(transaction, mutation)
}

pub(crate) fn apply_mutation(
    transaction: &redb::WriteTransaction,
    mutation: &JournalMutation,
) -> Result<(), JournalIoError> {
    match mutation.table() {
        JournalTable::Meta => apply_meta_mutation(transaction, mutation),
        JournalTable::Entities => apply_byte_mutation(transaction, ENTITIES, mutation),
        JournalTable::SecondaryIndexes => {
            apply_byte_mutation(transaction, SECONDARY_INDEXES, mutation)
        }
        JournalTable::IndexEpochs => apply_byte_mutation(transaction, INDEX_EPOCHS, mutation),
        JournalTable::Idempotency => apply_byte_mutation(transaction, IDEMPOTENCY, mutation),
        JournalTable::IdempotencyLocators => {
            apply_byte_mutation(transaction, crate::layout::IDEMPOTENCY_LOCATORS, mutation)
        }
        JournalTable::ProvenanceLocators => {
            apply_byte_mutation(transaction, crate::layout::PROVENANCE_LOCATORS, mutation)
        }
        JournalTable::AuditByRequestLocators => apply_byte_mutation(
            transaction,
            crate::layout::AUDIT_BY_REQUEST_LOCATORS,
            mutation,
        ),
        JournalTable::IdempotencyPending => {
            apply_byte_mutation(transaction, IDEMPOTENCY_PENDING, mutation)
        }
        JournalTable::HistoryTombstones => {
            apply_byte_mutation(transaction, crate::layout::HISTORY_TOMBSTONES, mutation)
        }
        JournalTable::OutboxStatus => {
            apply_byte_mutation(transaction, crate::layout::OUTBOX_STATUS, mutation)
        }
        JournalTable::Events => apply_byte_mutation(transaction, EVENTS, mutation),
        JournalTable::EventRoutes => apply_byte_mutation(transaction, EVENT_ROUTES, mutation),
        JournalTable::Outbox => apply_byte_mutation(transaction, crate::layout::OUTBOX, mutation),
        JournalTable::Provenance => apply_byte_mutation(transaction, PROVENANCE, mutation),
        JournalTable::Commits => apply_byte_mutation(transaction, COMMITS, mutation),
        JournalTable::Audit => apply_byte_mutation(transaction, AUDIT, mutation),
        JournalTable::AuditByRequest => {
            apply_byte_mutation(transaction, AUDIT_BY_REQUEST, mutation)
        }
        JournalTable::EntityChainHeads => {
            apply_byte_mutation(transaction, ENTITY_CHAIN_HEADS, mutation)
        }
        JournalTable::VectorEvidence => apply_byte_mutation(transaction, VECTOR_EVIDENCE, mutation),
        JournalTable::VectorObservations => {
            apply_byte_mutation(transaction, VECTOR_OBSERVATIONS, mutation)
        }
        JournalTable::VectorEvidenceIndex => {
            apply_byte_mutation(transaction, VECTOR_EVIDENCE_INDEX, mutation)
        }
    }
}

/// Applies one mutation already validated by the live composite staging path.
///
/// This is a same-process checkpoint materialization boundary. It preserves
/// the transaction-current before-image check, but deliberately does not
/// decode or hash the durable journal frame again. Recovery and startup never
/// call this function.
pub(crate) fn apply_validated_composite_mutation(
    transaction: &redb::WriteTransaction,
    mutation: &riffdb_storage_api::CompositeMutationV1,
) -> Result<(), JournalIoError> {
    use riffdb_storage_api::CompositeTableV1;

    match mutation.table() {
        CompositeTableV1::Meta => apply_meta_mutation_parts(
            transaction,
            mutation.key(),
            mutation.value(),
            mutation.expected_hash(),
        ),
        CompositeTableV1::HistoryTombstones => apply_byte_mutation_parts(
            transaction,
            crate::layout::HISTORY_TOMBSTONES,
            mutation.key(),
            mutation.value(),
            mutation.expected_hash(),
        ),
        CompositeTableV1::OutboxStatus => apply_byte_mutation_parts(
            transaction,
            crate::layout::OUTBOX_STATUS,
            mutation.key(),
            mutation.value(),
            mutation.expected_hash(),
        ),
        CompositeTableV1::IdempotencyLocators => apply_byte_mutation_parts(
            transaction,
            crate::layout::IDEMPOTENCY_LOCATORS,
            mutation.key(),
            mutation.value(),
            mutation.expected_hash(),
        ),
        CompositeTableV1::ProvenanceLocators => apply_byte_mutation_parts(
            transaction,
            crate::layout::PROVENANCE_LOCATORS,
            mutation.key(),
            mutation.value(),
            mutation.expected_hash(),
        ),
        CompositeTableV1::AuditByRequestLocators => apply_byte_mutation_parts(
            transaction,
            crate::layout::AUDIT_BY_REQUEST_LOCATORS,
            mutation.key(),
            mutation.value(),
            mutation.expected_hash(),
        ),
        CompositeTableV1::Entities => apply_byte_mutation_parts(
            transaction,
            ENTITIES,
            mutation.key(),
            mutation.value(),
            mutation.expected_hash(),
        ),
        CompositeTableV1::SecondaryIndexes => apply_byte_mutation_parts(
            transaction,
            SECONDARY_INDEXES,
            mutation.key(),
            mutation.value(),
            mutation.expected_hash(),
        ),
        CompositeTableV1::IndexEpochs => apply_byte_mutation_parts(
            transaction,
            INDEX_EPOCHS,
            mutation.key(),
            mutation.value(),
            mutation.expected_hash(),
        ),
        CompositeTableV1::Idempotency => apply_byte_mutation_parts(
            transaction,
            IDEMPOTENCY,
            mutation.key(),
            mutation.value(),
            mutation.expected_hash(),
        ),
        CompositeTableV1::IdempotencyPending => apply_byte_mutation_parts(
            transaction,
            IDEMPOTENCY_PENDING,
            mutation.key(),
            mutation.value(),
            mutation.expected_hash(),
        ),
        CompositeTableV1::Events => apply_byte_mutation_parts(
            transaction,
            EVENTS,
            mutation.key(),
            mutation.value(),
            mutation.expected_hash(),
        ),
        CompositeTableV1::EventRoutes => apply_byte_mutation_parts(
            transaction,
            EVENT_ROUTES,
            mutation.key(),
            mutation.value(),
            mutation.expected_hash(),
        ),
        CompositeTableV1::Outbox => apply_byte_mutation_parts(
            transaction,
            crate::layout::OUTBOX,
            mutation.key(),
            mutation.value(),
            mutation.expected_hash(),
        ),
        CompositeTableV1::Provenance => apply_byte_mutation_parts(
            transaction,
            PROVENANCE,
            mutation.key(),
            mutation.value(),
            mutation.expected_hash(),
        ),
        CompositeTableV1::Commits => apply_byte_mutation_parts(
            transaction,
            COMMITS,
            mutation.key(),
            mutation.value(),
            mutation.expected_hash(),
        ),
        CompositeTableV1::Audit => apply_byte_mutation_parts(
            transaction,
            AUDIT,
            mutation.key(),
            mutation.value(),
            mutation.expected_hash(),
        ),
        CompositeTableV1::AuditByRequest => apply_byte_mutation_parts(
            transaction,
            AUDIT_BY_REQUEST,
            mutation.key(),
            mutation.value(),
            mutation.expected_hash(),
        ),
        CompositeTableV1::EntityChainHeads => apply_byte_mutation_parts(
            transaction,
            ENTITY_CHAIN_HEADS,
            mutation.key(),
            mutation.value(),
            mutation.expected_hash(),
        ),
        CompositeTableV1::VectorEvidence => apply_byte_mutation_parts(
            transaction,
            VECTOR_EVIDENCE,
            mutation.key(),
            mutation.value(),
            mutation.expected_hash(),
        ),
        CompositeTableV1::VectorObservations => apply_byte_mutation_parts(
            transaction,
            VECTOR_OBSERVATIONS,
            mutation.key(),
            mutation.value(),
            mutation.expected_hash(),
        ),
        CompositeTableV1::VectorEvidenceIndex => apply_byte_mutation_parts(
            transaction,
            VECTOR_EVIDENCE_INDEX,
            mutation.key(),
            mutation.value(),
            mutation.expected_hash(),
        ),
    }
}

fn apply_meta_mutation(
    transaction: &redb::WriteTransaction,
    mutation: &JournalMutation,
) -> Result<(), JournalIoError> {
    apply_meta_mutation_parts(
        transaction,
        mutation.key(),
        mutation.value(),
        mutation.expected_hash(),
    )
}

fn apply_meta_mutation_parts(
    transaction: &redb::WriteTransaction,
    key: &[u8],
    value: Option<&[u8]>,
    expected_hash: Option<[u8; 32]>,
) -> Result<(), JournalIoError> {
    let key = std::str::from_utf8(key).map_err(|_| JournalIoError::Corrupt)?;
    match key {
        crate::layout::META_APPLICATION_SEQUENCE | crate::layout::META_ADMINISTRATION_SEQUENCE => {}
        crate::layout::META_RETENTION_WATERMARK => {
            let bytes = value.ok_or(JournalIoError::Corrupt)?;
            riffdb_storage_api::decode_retention_watermark_v1(bytes)
                .map_err(|_| JournalIoError::Corrupt)?;
        }
        crate::layout::META_VALIDATED_PREFIX_CHECKPOINT
            if value.is_none() && expected_hash.is_some() => {}
        _ => return Err(JournalIoError::Corrupt),
    }
    let mut table = transaction
        .open_table(META)
        .map_err(|_| JournalIoError::Corrupt)?;
    let current = table
        .get(key)
        .map_err(|_| JournalIoError::Corrupt)?
        .map(|value| value.value().to_vec());
    validate_before_image_parts(current.as_deref(), expected_hash)?;
    match value {
        Some(value) => {
            table
                .insert(key, value)
                .map_err(|_| JournalIoError::Corrupt)?;
        }
        None => {
            table.remove(key).map_err(|_| JournalIoError::Corrupt)?;
        }
    }
    Ok(())
}

fn apply_byte_mutation(
    transaction: &redb::WriteTransaction,
    definition: TableDefinition<&[u8], &[u8]>,
    mutation: &JournalMutation,
) -> Result<(), JournalIoError> {
    apply_byte_mutation_parts(
        transaction,
        definition,
        mutation.key(),
        mutation.value(),
        mutation.expected_hash(),
    )
}

pub(crate) fn apply_byte_mutation_parts(
    transaction: &redb::WriteTransaction,
    definition: TableDefinition<&[u8], &[u8]>,
    key: &[u8],
    value: Option<&[u8]>,
    expected_hash: Option<[u8; 32]>,
) -> Result<(), JournalIoError> {
    let mut table = transaction
        .open_table(definition)
        .map_err(|_| JournalIoError::Corrupt)?;
    {
        let current = table.get(key).map_err(|_| JournalIoError::Corrupt)?;
        let stored = current.as_ref().map(|row| row.value());
        let resolved = if redb::TableHandle::name(&definition) == redb::TableHandle::name(&COMMITS)
        {
            // Hash a stub's resolved segment (ADR-0263 C3); ordinary rows
            // remain borrowed from the engine throughout the before-image check.
            stored
                .map(crate::history_stub::resolve_commits_value)
                .transpose()
                .map_err(|_| JournalIoError::Corrupt)?
        } else {
            stored.map(std::borrow::Cow::Borrowed)
        };
        validate_before_image_parts(resolved.as_deref(), expected_hash)?;
    }
    match value {
        Some(value) => {
            table
                .insert(key, value)
                .map_err(|_| JournalIoError::Corrupt)?;
        }
        None => {
            table.remove(key).map_err(|_| JournalIoError::Corrupt)?;
        }
    }
    Ok(())
}

fn validate_before_image(
    current: Option<&[u8]>,
    mutation: &JournalMutation,
) -> Result<(), JournalIoError> {
    validate_before_image_parts(current, mutation.expected_hash())
}

fn validate_before_image_parts(
    current: Option<&[u8]>,
    expected_hash: Option<[u8; 32]>,
) -> Result<(), JournalIoError> {
    match (current, expected_hash) {
        (None, None) => Ok(()),
        (Some(value), Some(expected)) if digest(value) == expected => Ok(()),
        _ => Err(JournalIoError::Corrupt),
    }
}

thread_local! {
    /// Per-thread `open_table` / `get` / value-copy nanoseconds for the last
    /// point-read window, in that order.
    ///
    /// A thread-local avoids threading a profile through
    /// `CompositeViewBase::read_base`, whose signature is fixed by
    /// `riffdb-storage-api`. Only the diagnostic path writes it, and one query
    /// execute runs entirely on one thread.
    static POINT_READ_SUBSTAGES: std::cell::Cell<[u64; 3]> = const { std::cell::Cell::new([0; 3]) };
}

/// Clears the point-read sub-stage accumulator before one timed window.
pub(crate) fn reset_point_read_substages() {
    POINT_READ_SUBSTAGES.with(|cell| cell.set([0; 3]));
}

/// Returns `open_table`, `get` and value-copy nanoseconds since the last reset.
pub(crate) fn point_read_substages() -> [u64; 3] {
    POINT_READ_SUBSTAGES.with(std::cell::Cell::get)
}

pub(crate) fn charge_point_read_substage(index: usize, started: Instant) {
    let elapsed = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    POINT_READ_SUBSTAGES.with(|cell| {
        let mut current = cell.get();
        current[index] = current[index].saturating_add(elapsed);
        cell.set(current);
    });
}

pub(crate) fn read_value(
    transaction: &redb::ReadTransaction,
    table: JournalTable,
    key: &[u8],
) -> Result<Option<Vec<u8>>, JournalIoError> {
    if crate::query_diagnostics::query_execute_diagnostics_enabled()
        && let Some(definition) = byte_table_definition(table)
    {
        return read_value_profiled(transaction, definition, key);
    }
    read_value_inner(transaction, table, key)
}

/// Splits one point read into `open_table`, `get` and value-copy segments.
///
/// Reachable only when the query-execute diagnostic is enabled, so the
/// ordinary read path keeps the single-expression form below.
fn read_value_profiled(
    transaction: &redb::ReadTransaction,
    definition: TableDefinition<'static, &'static [u8], &'static [u8]>,
    key: &[u8],
) -> Result<Option<Vec<u8>>, JournalIoError> {
    let started = Instant::now();
    let opened = transaction
        .open_table(definition)
        .map_err(|_| JournalIoError::Corrupt)?;
    charge_point_read_substage(0, started);
    let started = Instant::now();
    let found = opened.get(key).map_err(|_| JournalIoError::Corrupt)?;
    charge_point_read_substage(1, started);
    let started = Instant::now();
    let value = found.map(|value| value.value().to_vec());
    charge_point_read_substage(2, started);
    Ok(value)
}

fn read_value_inner(
    transaction: &redb::ReadTransaction,
    table: JournalTable,
    key: &[u8],
) -> Result<Option<Vec<u8>>, JournalIoError> {
    read_value_inner_bounded(transaction, table, key, usize::MAX)
}

fn read_value_inner_bounded(
    transaction: &redb::ReadTransaction,
    table: JournalTable,
    key: &[u8],
    limit: usize,
) -> Result<Option<Vec<u8>>, JournalIoError> {
    let copy = |bytes: &[u8]| {
        if bytes.len() <= limit {
            Ok(bytes.to_vec())
        } else {
            Err(JournalIoError::Capacity)
        }
    };
    if table == JournalTable::Meta {
        let key = std::str::from_utf8(key).map_err(|_| JournalIoError::Corrupt)?;
        return transaction
            .open_table(META)
            .map_err(|_| JournalIoError::Corrupt)?
            .get(key)
            .map_err(|_| JournalIoError::Corrupt)
            .and_then(|value| value.map(|value| copy(value.value())).transpose());
    }
    let definition = match table {
        JournalTable::Meta => unreachable!("meta returned above"),
        JournalTable::IdempotencyLocators => crate::layout::IDEMPOTENCY_LOCATORS,
        JournalTable::ProvenanceLocators => crate::layout::PROVENANCE_LOCATORS,
        JournalTable::AuditByRequestLocators => crate::layout::AUDIT_BY_REQUEST_LOCATORS,
        JournalTable::HistoryTombstones => crate::layout::HISTORY_TOMBSTONES,
        JournalTable::OutboxStatus => crate::layout::OUTBOX_STATUS,
        JournalTable::Entities => ENTITIES,
        JournalTable::SecondaryIndexes => SECONDARY_INDEXES,
        JournalTable::IndexEpochs => INDEX_EPOCHS,
        JournalTable::Idempotency => IDEMPOTENCY,
        JournalTable::IdempotencyPending => IDEMPOTENCY_PENDING,
        JournalTable::Events => EVENTS,
        JournalTable::EventRoutes => EVENT_ROUTES,
        JournalTable::Outbox => crate::layout::OUTBOX,
        JournalTable::Provenance => PROVENANCE,
        JournalTable::Commits => COMMITS,
        JournalTable::Audit => AUDIT,
        JournalTable::AuditByRequest => AUDIT_BY_REQUEST,
        JournalTable::EntityChainHeads => ENTITY_CHAIN_HEADS,
        JournalTable::VectorEvidence => VECTOR_EVIDENCE,
        JournalTable::VectorObservations => VECTOR_OBSERVATIONS,
        JournalTable::VectorEvidenceIndex => VECTOR_EVIDENCE_INDEX,
    };
    transaction
        .open_table(definition)
        .map_err(|_| JournalIoError::Corrupt)?
        .get(key)
        .map_err(|_| JournalIoError::Corrupt)
        .and_then(|value| value.map(|value| copy(value.value())).transpose())
}

pub(crate) fn read_write_value(
    transaction: &redb::WriteTransaction,
    table: JournalTable,
    key: &[u8],
) -> Result<Option<Vec<u8>>, JournalIoError> {
    if table == JournalTable::Meta {
        let key = std::str::from_utf8(key).map_err(|_| JournalIoError::Corrupt)?;
        return transaction
            .open_table(META)
            .map_err(|_| JournalIoError::Corrupt)?
            .get(key)
            .map_err(|_| JournalIoError::Corrupt)
            .map(|value| value.map(|value| value.value().to_vec()));
    }
    let definition = match table {
        JournalTable::Meta => unreachable!("meta returned above"),
        JournalTable::IdempotencyLocators => crate::layout::IDEMPOTENCY_LOCATORS,
        JournalTable::ProvenanceLocators => crate::layout::PROVENANCE_LOCATORS,
        JournalTable::AuditByRequestLocators => crate::layout::AUDIT_BY_REQUEST_LOCATORS,
        JournalTable::HistoryTombstones => crate::layout::HISTORY_TOMBSTONES,
        JournalTable::OutboxStatus => crate::layout::OUTBOX_STATUS,
        JournalTable::Entities => ENTITIES,
        JournalTable::SecondaryIndexes => SECONDARY_INDEXES,
        JournalTable::IndexEpochs => INDEX_EPOCHS,
        JournalTable::Idempotency => IDEMPOTENCY,
        JournalTable::IdempotencyPending => IDEMPOTENCY_PENDING,
        JournalTable::Events => EVENTS,
        JournalTable::EventRoutes => EVENT_ROUTES,
        JournalTable::Outbox => crate::layout::OUTBOX,
        JournalTable::Provenance => PROVENANCE,
        JournalTable::Commits => COMMITS,
        JournalTable::Audit => AUDIT,
        JournalTable::AuditByRequest => AUDIT_BY_REQUEST,
        JournalTable::EntityChainHeads => ENTITY_CHAIN_HEADS,
        JournalTable::VectorEvidence => VECTOR_EVIDENCE,
        JournalTable::VectorObservations => VECTOR_OBSERVATIONS,
        JournalTable::VectorEvidenceIndex => VECTOR_EVIDENCE_INDEX,
    };
    transaction
        .open_table(definition)
        .map_err(|_| JournalIoError::Corrupt)?
        .get(key)
        .map_err(|_| JournalIoError::Corrupt)
        .map(|value| value.map(|value| value.value().to_vec()))
}

pub(crate) const fn byte_table_definition(
    table: JournalTable,
) -> Option<TableDefinition<'static, &'static [u8], &'static [u8]>> {
    match table {
        JournalTable::Meta => None,
        JournalTable::IdempotencyLocators => Some(crate::layout::IDEMPOTENCY_LOCATORS),
        JournalTable::ProvenanceLocators => Some(crate::layout::PROVENANCE_LOCATORS),
        JournalTable::AuditByRequestLocators => Some(crate::layout::AUDIT_BY_REQUEST_LOCATORS),
        JournalTable::HistoryTombstones => Some(crate::layout::HISTORY_TOMBSTONES),
        JournalTable::OutboxStatus => Some(crate::layout::OUTBOX_STATUS),
        JournalTable::Entities => Some(ENTITIES),
        JournalTable::SecondaryIndexes => Some(SECONDARY_INDEXES),
        JournalTable::IndexEpochs => Some(INDEX_EPOCHS),
        JournalTable::Idempotency => Some(IDEMPOTENCY),
        JournalTable::IdempotencyPending => Some(IDEMPOTENCY_PENDING),
        JournalTable::Events => Some(EVENTS),
        JournalTable::EventRoutes => Some(EVENT_ROUTES),
        JournalTable::Outbox => Some(crate::layout::OUTBOX),
        JournalTable::Provenance => Some(PROVENANCE),
        JournalTable::Commits => Some(COMMITS),
        JournalTable::Audit => Some(AUDIT),
        JournalTable::AuditByRequest => Some(AUDIT_BY_REQUEST),
        JournalTable::EntityChainHeads => Some(ENTITY_CHAIN_HEADS),
        JournalTable::VectorEvidence => Some(VECTOR_EVIDENCE),
        JournalTable::VectorObservations => Some(VECTOR_OBSERVATIONS),
        JournalTable::VectorEvidenceIndex => Some(VECTOR_EVIDENCE_INDEX),
    }
}

/// Advances the extent's checkpoint anchor to its complete tail, keeping every
/// frame before it in place as retained history (ADR-0263).
///
/// `header` names the checkpoint the anchor now binds: its hash must be the
/// tail frame's, and its frontiers at or after the tail's, since direct engine
/// writes can advance the engine frontier without a frame. An anchor that is
/// already exact writes nothing, so a read-only open leaves the selected header
/// (and any clean-close certificate over it, ADR-0157) unchanged. Otherwise the
/// other slot is written at the next revision and synced; a crash leaves the
/// previous header selected, and recovery rescans the older suffix.
pub(crate) fn advance_journal_anchor_with_media(
    media: &dyn JournalMedia,
    path: &Path,
    header: &JournalFileHeader,
) -> Result<(), JournalIoError> {
    let (_, tail, state) =
        scan_extent_with_media(media, path, Some(header.database_id()), |_, _| Ok(()))?;
    if header.checkpoint_frame_hash() != tail.last_hash
        || header.checkpoint_sequence() < tail.last_sequence
        || header.checkpoint_administration_sequence() < tail.last_administration_sequence
    {
        return Err(JournalIoError::Corrupt);
    }
    if state.header.logical == *header
        && state.header.anchor_position == tail.complete_bytes
        && !tail.incomplete_tail
    {
        return Ok(());
    }
    let anchored = state.header.anchored(header.clone(), tail.complete_bytes)?;
    let mut file = media
        .open_read_write(path)
        .map_err(|_| JournalIoError::Io)?;
    if tail.incomplete_tail {
        // A torn terminal frame stays in place under the same generation. Fence
        // it before the anchor names its position, so no later scan mistakes it
        // for a complete frame or an incomplete tail after the anchor.
        file.write_all_at(&[0_u8; EXTENT_FRAME_HEADER_BYTES], tail.complete_bytes)
            .and_then(|()| file.sync_data())
            .map_err(|_| JournalIoError::Io)?;
    }
    file.write_all_at(
        &anchored.encode(),
        usize::from(anchored.slot) * EXTENT_HEADER_SLOT_BYTES,
    )
    .and_then(|()| file.sync_data())
    .map_err(|_| JournalIoError::Io)
}

pub(crate) fn reset_journal(path: &Path, header: &JournalFileHeader) -> Result<(), JournalIoError> {
    reset_journal_with_media(&RealJournalMedia, path, header)
}

pub(crate) fn reset_journal_with_media(
    media: &dyn JournalMedia,
    path: &Path,
    header: &JournalFileHeader,
) -> Result<(), JournalIoError> {
    reset_journal_after_generation_with_media(media, path, header, None)
}

/// Creates the active extent that a crash left absent while an extent waits at
/// the sealing name: one generation after it, continuing its anchored header.
/// A fresh generation-one extent would later seal under a name a retained
/// extent already holds (ADR-0263).
pub(crate) fn recreate_missing_active_with_media(
    media: &dyn JournalMedia,
    database_path: &Path,
) -> Result<(), JournalIoError> {
    let active = journal_path(database_path);
    let sealing = sealing_journal_path(database_path);
    if media.try_exists(&active).map_err(|_| JournalIoError::Io)?
        || !media.try_exists(&sealing).map_err(|_| JournalIoError::Io)?
    {
        return Ok(());
    }
    let predecessor = inspect_extent_with_media(media, &sealing)?;
    reset_journal_after_with_media(media, &active, &predecessor.header.logical, &sealing)?;
    sync_parent_with_media(media, &active)
}

/// Gives the active extent the configured capacity once its recovered suffix
/// is empty. An extent that retains materialized frames is retired to the
/// sealing name instead of replaced, because history stubs name its frames.
pub(crate) fn normalize_active_capacity_with_media(
    media: &dyn JournalMedia,
    database_path: &Path,
    database_id: DatabaseId,
) -> Result<(), JournalIoError> {
    normalize_active_capacity_to(media, database_path, database_id, EXTENT_DATA_BYTES)
}

fn normalize_active_capacity_to(
    media: &dyn JournalMedia,
    database_path: &Path,
    database_id: DatabaseId,
    data_bytes: usize,
) -> Result<(), JournalIoError> {
    let active = journal_path(database_path);
    if !media.try_exists(&active).map_err(|_| JournalIoError::Io)? {
        return Ok(());
    }
    let (_, tail, state) =
        scan_extent_with_media(media, &active, Some(database_id), |_, _| Ok(()))?;
    if state.header.data_bytes == data_bytes {
        return Ok(());
    }
    if state.header.anchor_position == EXTENT_DATA_OFFSET {
        return normalize_extent_capacity_to(media, &active, data_bytes).map(drop);
    }
    if tail.transition_count != 0 || tail.incomplete_tail {
        return Err(JournalIoError::Corrupt);
    }
    // Finish any earlier seal so the sealing name is free, then retire this
    // extent exactly as a rotation would, successor first made durable.
    seal_extent_with_media(media, database_path, database_id)?;
    let sealing = sealing_journal_path(database_path);
    media
        .rename(&active, &sealing)
        .map_err(|_| JournalIoError::Io)?;
    sync_parent_with_media(media, &sealing)?;
    recreate_missing_active_with_media(media, database_path)
}

/// Prepares one recyclable extent whose generation is strictly newer than the
/// selected generation of `predecessor_path`.
///
/// The spare extent is deliberately non-authoritative and recovery may remove
/// it. Recreating an absent spare at generation one would let the next
/// checkpoint swap a lower generation over a much older active extent. Bind
/// preparation to the current active generation so path recycling preserves
/// ADR-0103's monotonic stale-residue fence across process recovery.
pub(crate) fn reset_journal_after_with_media(
    media: &dyn JournalMedia,
    path: &Path,
    header: &JournalFileHeader,
    predecessor_path: &Path,
) -> Result<(), JournalIoError> {
    let mut predecessor_file = media
        .open_read(predecessor_path)
        .map_err(|_| JournalIoError::Io)?;
    let predecessor = read_selected_extent_header(&mut predecessor_file, true)?;
    if predecessor.logical.database_id() != header.database_id() {
        return Err(JournalIoError::Corrupt);
    }
    reset_journal_after_generation_with_media(media, path, header, Some(predecessor.generation))
}

/// Re-issues the fully checkpointed extent at `path`, empty and with the same
/// anchored header, at a generation above `floor` when its own is not. A
/// restored database whose history is hydrated names none of its frames, and
/// its future seals must never reach a generation a retained extent beside
/// the configured database already holds (ADR-0263).
pub(crate) fn renew_extent_above_with_media(
    media: &dyn JournalMedia,
    path: &Path,
    floor: u64,
) -> Result<(), JournalIoError> {
    let (_, tail, state) = scan_extent_with_media(media, path, None, |_, _| Ok(()))?;
    if state.header.generation > floor {
        return Ok(());
    }
    if tail.transition_count != 0 || tail.incomplete_tail {
        return Err(JournalIoError::Corrupt);
    }
    reset_journal_after_generation_with_media(
        media,
        path,
        &state.header.logical,
        Some(floor.max(state.header.generation)),
    )
}

/// The generation of the extent at `path`, if one exists; a legacy journal
/// file counts as generation zero.
pub(crate) fn extent_generation_with_media(
    media: &dyn JournalMedia,
    path: &Path,
) -> Result<Option<u64>, JournalIoError> {
    let mut file = match media.open_read(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(JournalIoError::Io),
    };
    let mut magic = [0_u8; 8];
    file.read_exact(&mut magic)
        .map_err(|_| JournalIoError::Corrupt)?;
    if magic == FILE_MAGIC {
        return Ok(Some(0));
    }
    read_selected_extent_header(&mut file, false).map(|extent| Some(extent.generation))
}

fn reset_journal_after_generation_with_media(
    media: &dyn JournalMedia,
    path: &Path,
    header: &JournalFileHeader,
    predecessor_generation: Option<u64>,
) -> Result<(), JournalIoError> {
    let mut file = match media.open_read_write(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut initial = ExtentHeader::initial(header.clone());
            if let Some(predecessor) = predecessor_generation {
                initial.generation = predecessor.checked_add(1).ok_or(JournalIoError::Corrupt)?;
            }
            return create_extent_file_with_media(media, path, &initial);
        }
        Err(_) => return Err(JournalIoError::Io),
    };
    let mut magic = [0_u8; 8];
    file.read_exact(&mut magic)
        .map_err(|_| JournalIoError::Corrupt)?;
    if magic == FILE_MAGIC {
        let (_, tail) =
            scan_legacy_journal_with_media(media, path, header.database_id(), |_| Ok(()))?
                .ok_or(JournalIoError::Corrupt)?;
        if tail.transition_count != 0 || tail.incomplete_tail {
            return Err(JournalIoError::LegacyNonEmpty(path.to_path_buf()));
        }
        drop(file);
        return migrate_empty_legacy_journal_with_media(media, path, header);
    }
    if magic != EXTENT_MAGIC {
        return Err(JournalIoError::Corrupt);
    }
    let current = read_selected_extent_header(&mut file, true)?;
    if current.logical.database_id() != header.database_id() {
        return Err(JournalIoError::Corrupt);
    }
    let mut successor = current.successor(header.clone())?;
    if let Some(predecessor) = predecessor_generation {
        successor.generation = successor
            .generation
            .max(predecessor.checked_add(1).ok_or(JournalIoError::Corrupt)?);
    }
    file.write_all_at(
        &successor.encode(),
        usize::from(successor.slot) * EXTENT_HEADER_SLOT_BYTES,
    )
    .and_then(|()| file.sync_data())
    .map_err(|_| JournalIoError::Io)
}

#[cfg(unix)]
pub(crate) fn write_all_at(
    file: &mut File,
    mut bytes: &[u8],
    mut offset: usize,
) -> std::io::Result<()> {
    while !bytes.is_empty() {
        let written = file.write_at(
            bytes,
            u64::try_from(offset).map_err(|_| std::io::Error::other("journal offset overflow"))?,
        )?;
        if written == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "zero-length positional journal write",
            ));
        }
        offset = offset
            .checked_add(written)
            .ok_or_else(|| std::io::Error::other("journal offset overflow"))?;
        bytes = &bytes[written..];
    }
    Ok(())
}

#[cfg(not(unix))]
pub(crate) fn write_all_at(file: &mut File, bytes: &[u8], offset: usize) -> std::io::Result<()> {
    file.seek(SeekFrom::Start(
        u64::try_from(offset).map_err(|_| std::io::Error::other("journal offset overflow"))?,
    ))?;
    file.write_all(bytes)
}

#[cfg(unix)]
pub(crate) fn read_exact_at(
    file: &mut File,
    mut bytes: &mut [u8],
    mut offset: usize,
) -> std::io::Result<()> {
    while !bytes.is_empty() {
        let read = file.read_at(
            bytes,
            u64::try_from(offset).map_err(|_| std::io::Error::other("journal offset overflow"))?,
        )?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "short positional journal read",
            ));
        }
        offset = offset
            .checked_add(read)
            .ok_or_else(|| std::io::Error::other("journal offset overflow"))?;
        bytes = &mut bytes[read..];
    }
    Ok(())
}

#[cfg(not(unix))]
pub(crate) fn read_exact_at(
    file: &mut File,
    bytes: &mut [u8],
    offset: usize,
) -> std::io::Result<()> {
    file.seek(SeekFrom::Start(
        u64::try_from(offset).map_err(|_| std::io::Error::other("journal offset overflow"))?,
    ))?;
    file.read_exact(bytes)
}

fn read_until_full_or_eof(file: &mut MediaFile, bytes: &mut [u8]) -> Result<usize, JournalIoError> {
    let mut read = 0;
    while read < bytes.len() {
        match file.read(&mut bytes[read..]) {
            Ok(0) => break,
            Ok(count) => read += count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(_) => return Err(JournalIoError::Io),
        }
    }
    Ok(read)
}

fn validate_component(bytes: &[u8]) -> Result<(), JournalCodecError> {
    if bytes.is_empty() || bytes.len() > MAX_JOURNAL_FRAME_BYTES {
        return Err(JournalCodecError::LimitExceeded);
    }
    Ok(())
}

/// The journal's SHA-256 content digest, for storage encodings that must not
/// take their own hashing dependency (history stubs, ADR-0263).
pub(crate) fn content_digest(bytes: &[u8]) -> [u8; HASH_BYTES] {
    digest(bytes)
}

fn digest(bytes: &[u8]) -> [u8; HASH_BYTES] {
    Sha256::digest(bytes).into()
}

#[cfg(test)]
thread_local! {
    static LOGICAL_FRAME_HASH_PASSES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn logical_frame_digest(bytes: &[u8]) -> [u8; HASH_BYTES] {
    #[cfg(test)]
    LOGICAL_FRAME_HASH_PASSES.with(|passes| passes.set(passes.get() + 1));
    digest(bytes)
}

fn read_array<const N: usize>(bytes: &[u8], offset: usize) -> Result<[u8; N], JournalCodecError> {
    bytes
        .get(offset..offset.checked_add(N).ok_or(JournalCodecError::Truncated)?)
        .ok_or(JournalCodecError::Truncated)?
        .try_into()
        .map_err(|_| JournalCodecError::Truncated)
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, JournalCodecError> {
    Ok(u16::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, JournalCodecError> {
    Ok(u32::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, JournalCodecError> {
    Ok(u64::from_be_bytes(read_array(bytes, offset)?))
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::Write;

    use super::*;

    mod zero_fill_tests;

    /// Whole-directory scope: `.0` and every side file it grows (journal,
    /// checkpoint, spare, rewrite, …) live in one
    /// [`crate::test_path::ScopedDirectory`] removed on drop — pass, fail,
    /// or panic — so cleanup never depends on a hand-maintained file list.
    /// This retires the stale `target/journal-tests` root.
    struct TestPath(
        PathBuf,
        // Held only so `Drop` removes the whole scope.
        #[allow(dead_code)] crate::test_path::ScopedDirectory,
    );

    impl TestPath {
        fn new(label: &str) -> Self {
            let scope = crate::test_path::ScopedDirectory::new(label);
            Self(scope.join("db.journal"), scope)
        }
    }

    fn database_id(seed: u8) -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [seed; 10])
            .expect("database ID")
    }

    fn sequence(value: u64) -> CommitSequence {
        CommitSequence::new(value).expect("nonzero sequence")
    }

    // req: STO-030, REC-001, END-008
    #[cfg(unix)]
    #[test]
    fn retired_append_owner_cannot_reacquire_physical_write_ownership() {
        let path = TestPath::new("retired-append-owner");
        let header = JournalFileHeader::new(database_id(92), None, [0; HASH_BYTES]);
        let mut lane = JournalLane::open(&path.0, &header).unwrap();
        let state = inspect_extent_with_media(&RealJournalMedia, &path.0).unwrap();
        let physical = extent_used_bytes(state.next_position);
        assert!(
            lane.can_reuse_cursor(&RealJournalMedia, &path.0, &header, physical)
                .unwrap()
        );
        drop(lane.sender.take());
        lane.worker.take().unwrap().join().unwrap();
        assert!(
            !lane
                .can_reuse_cursor(&RealJournalMedia, &path.0, &header, physical)
                .unwrap()
        );
    }

    fn sample_frame() -> JournalFrame {
        JournalFrame::new(
            database_id(3),
            sequence(8),
            sequence(9),
            2,
            [0x44; HASH_BYTES],
            vec![
                JournalMutation::put(JournalTable::Entities, vec![1], vec![2, 3]).expect("put"),
                JournalMutation::delete_matching(JournalTable::SecondaryIndexes, vec![4], &[9])
                    .expect("delete"),
                JournalMutation::put(JournalTable::Commits, vec![8], vec![8]).expect("commit"),
                JournalMutation::put(JournalTable::Commits, vec![9], vec![9]).expect("commit"),
            ],
        )
        .expect("frame")
    }

    #[test]
    fn validated_checkpoint_handoff_preserves_transaction_current_checks() {
        let database_path = TestPath::new("validated-checkpoint-handoff");
        let database = create_database(&database_path.0);
        let insert = riffdb_storage_api::CompositeMutationV1::put(
            riffdb_storage_api::CompositeTableV1::Entities,
            [0x10].as_slice(),
            [0x20].as_slice(),
        )
        .expect("validated insert");
        let transaction = database.begin_write().expect("begin checkpoint apply");
        apply_validated_composite_mutation(&transaction, &insert).expect("apply validated insert");
        transaction.commit().expect("commit validated insert");

        let replace = riffdb_storage_api::CompositeMutationV1::replace(
            riffdb_storage_api::CompositeTableV1::Entities,
            [0x10].as_slice(),
            [0x20].as_slice(),
            [0x30].as_slice(),
        )
        .expect("validated replacement");
        let transaction = database.begin_write().expect("begin replacement");
        apply_validated_composite_mutation(&transaction, &replace)
            .expect("apply validated replacement");
        transaction.commit().expect("commit replacement");

        let stale = riffdb_storage_api::CompositeMutationV1::replace(
            riffdb_storage_api::CompositeTableV1::Entities,
            [0x10].as_slice(),
            [0x20].as_slice(),
            [0x40].as_slice(),
        )
        .expect("stale replacement");
        let transaction = database.begin_write().expect("begin stale replacement");
        assert_eq!(
            apply_validated_composite_mutation(&transaction, &stale),
            Err(JournalIoError::Corrupt),
            "the handoff never bypasses the redb before-image check"
        );
        transaction.abort().expect("abort stale replacement");

        let transaction = database.begin_read().expect("read final value");
        assert_eq!(
            transaction
                .open_table(ENTITIES)
                .expect("entities")
                .get([0x10].as_slice())
                .expect("read entity")
                .map(|value| value.value().to_vec()),
            Some(vec![0x30])
        );
    }

    #[test]
    fn decoded_journal_frame_converts_to_the_shared_overlay_descriptor() {
        let frame = sample_frame();
        let composite = frame.composite().expect("composite frame");
        assert_eq!(
            composite.kind(),
            riffdb_storage_api::CompositeFrameKindV1::Command
        );
    }

    #[test]
    fn service_audit_frame_round_trips_dual_frontier_and_rejects_open_tables() {
        let mutations = vec![
            JournalMutation::put(JournalTable::Audit, vec![1], vec![11]).expect("audit one"),
            JournalMutation::put(JournalTable::Audit, vec![2], vec![12]).expect("audit two"),
            JournalMutation::put(JournalTable::AuditByRequest, vec![3], vec![13])
                .expect("request one"),
            JournalMutation::put(JournalTable::AuditByRequest, vec![4], vec![14])
                .expect("request two"),
            JournalMutation::replace(
                JournalTable::Meta,
                crate::layout::META_ADMINISTRATION_SEQUENCE
                    .as_bytes()
                    .to_vec(),
                &[21],
                vec![22],
            )
            .expect("allocator"),
        ];
        let frame = JournalFrame::service_audit(
            database_id(4),
            Some(sequence(7)),
            None,
            AdministrationSequence::new(2),
            2,
            [0x55; HASH_BYTES],
            mutations.clone(),
        )
        .expect("service-audit frame");
        let encoded = frame.encode().expect("encode service-audit frame");
        let (decoded, _) = JournalFrame::decode(encoded.as_bytes()).expect("decode frame");
        assert_eq!(decoded, frame);
        assert_eq!(decoded.kind(), JournalFrameKind::ServiceAudit);
        assert_eq!(decoded.predecessor_sequence(), Some(sequence(7)));
        assert_eq!(decoded.covered_sequence(), Some(sequence(7)));
        assert_eq!(
            decoded.covered_administration_sequence(),
            AdministrationSequence::new(2)
        );

        let mut open_mutations = mutations;
        open_mutations.push(
            JournalMutation::put(JournalTable::Entities, vec![9], vec![9])
                .expect("open-table mutation"),
        );
        assert_eq!(
            JournalFrame::service_audit(
                database_id(4),
                Some(sequence(7)),
                None,
                AdministrationSequence::new(2),
                2,
                [0x55; HASH_BYTES],
                open_mutations,
            ),
            Err(JournalCodecError::InvalidValue)
        );
    }

    fn create_database(path: &Path) -> Database {
        let database = redb::Builder::new().create(path).expect("create database");
        let mut transaction = database.begin_write().expect("begin initialize");
        transaction.set_two_phase_commit(true);
        transaction
            .set_durability(Durability::Immediate)
            .expect("set durability");
        crate::layout::create_all_tables(&transaction).expect("create tables");
        transaction.commit().expect("commit initialize");
        database
    }

    fn recovery_frame(database_id: DatabaseId) -> JournalFrame {
        JournalFrame::new(
            database_id,
            sequence(1),
            sequence(1),
            1,
            [0; HASH_BYTES],
            vec![
                JournalMutation::put(
                    JournalTable::Commits,
                    crate::keys::encode_application_sequence_key(sequence(1)).to_vec(),
                    vec![0x41],
                )
                .expect("commit mutation"),
                JournalMutation::put(JournalTable::Entities, vec![0x10], vec![0x20])
                    .expect("entity mutation"),
            ],
        )
        .expect("recovery frame")
    }

    fn write_recovery_journal(
        database_path: &Path,
        database_id: DatabaseId,
        frame: &EncodedJournalFrame,
        torn_tail: Option<&EncodedJournalFrame>,
    ) {
        let path = journal_path(database_path);
        let header = JournalFileHeader::new(database_id, None, [0; HASH_BYTES]);
        initialize_or_validate_file(&path, &header).expect("initialize recovery journal");
        write_physical_frame(&path, frame, false);
        if let Some(tail) = torn_tail {
            write_physical_frame(&path, tail, true);
        }
    }

    fn write_physical_frame(path: &Path, frame: &EncodedJournalFrame, torn: bool) {
        let state = inspect_extent(path).expect("inspect extent");
        let physical =
            EncodedExtentFrame::encode(state.header.generation, state.next_position, frame)
                .expect("encode physical frame");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .expect("open recovery journal");
        let bytes = if torn {
            &physical.bytes[..physical.bytes.len() / 2]
        } else {
            physical.bytes.as_slice()
        };
        write_all_at(&mut file, bytes, physical.position).expect("write physical frame");
        file.sync_data().expect("sync recovery journal");
    }

    // req: REC-006, STO-031
    #[test]
    fn sealed_full_reader_refuses_footer_tail_beyond_index_limit() {
        let database_path = TestPath::new("sealed-footer-allocation-bound");
        let database_id = database_id(87);
        let active = journal_path(&database_path.0);
        reset_journal(
            &active,
            &JournalFileHeader::new(database_id, None, [0; HASH_BYTES]),
        )
        .expect("empty journal");
        std::fs::rename(&active, sealing_journal_path(&database_path.0)).expect("sealing journal");
        let sealed = seal_extent_with_media(&RealJournalMedia, &database_path.0, database_id)
            .expect("seal empty journal")
            .expect("sealed path");
        assert!(read_sealed_extent_with_media(&RealJournalMedia, &sealed, database_id).is_ok());
        assert!(read_sealed_index_with_media(&RealJournalMedia, &sealed, database_id).is_ok());
        let maximum = SealedExtentFooter::encoded_len(MAX_JOURNAL_SUFFIX_TRANSITIONS)
            .and_then(|bytes| align_up(bytes, EXTENT_FRAME_ALIGNMENT))
            .expect("bounded footer ceiling");
        let oversized = EXTENT_DATA_OFFSET + maximum + EXTENT_FRAME_ALIGNMENT;
        assert!(oversized < extent_file_bytes(LEGACY_EXTENT_DATA_BYTES));
        let file = OpenOptions::new()
            .write(true)
            .open(&sealed)
            .expect("owned sealed file");
        file.set_len(oversized as u64)
            .expect("append bounded zero padding");
        file.sync_all().expect("sync test corruption");
        assert!(matches!(
            read_sealed_index_with_media(&RealJournalMedia, &sealed, database_id),
            Err(JournalIoError::Corrupt)
        ));
        assert!(
            matches!(
                read_sealed_extent_with_media(&RealJournalMedia, &sealed, database_id),
                Err(JournalIoError::Corrupt)
            ),
            "full reader must refuse an oversized footer before allocation"
        );
        assert_eq!(
            std::fs::metadata(&sealed).expect("preserved source").len(),
            oversized as u64
        );
    }

    #[test]
    fn file_header_round_trips_and_rejects_substitution() {
        let header = JournalFileHeader::new(database_id(1), Some(sequence(7)), [0x22; 32]);
        let encoded = header.encode();
        assert_eq!(JournalFileHeader::decode(&encoded), Ok(header));
        let mut substituted = encoded;
        substituted[40] ^= 1;
        assert_eq!(
            JournalFileHeader::decode(&substituted),
            Err(JournalCodecError::Checksum)
        );
    }

    #[test]
    fn frame_round_trips_and_hash_binds_every_byte() {
        let frame = sample_frame();
        let encoded = frame.encode().expect("encode");
        let (decoded, hash) = JournalFrame::decode(encoded.as_bytes()).expect("decode");
        assert_eq!(decoded, frame);
        assert_eq!(hash, encoded.frame_hash());

        let mut substituted = encoded.as_bytes().to_vec();
        substituted[FRAME_HEADER_BYTES + MUTATION_HEADER_BYTES] ^= 1;
        assert_eq!(
            JournalFrame::decode(&substituted),
            Err(JournalCodecError::Checksum)
        );
    }

    #[test]
    fn buffered_writer_is_byte_identical_to_the_closed_frame_encoder() {
        let frame = sample_frame();
        let expected = frame.encode().expect("closed encoder");
        let mut mutations = JournalMutationBuffer::default();
        mutations
            .extend(sample_frame().mutations)
            .expect("buffer mutations");
        let actual = JournalFrame::encode_buffered_command(
            database_id(3),
            Some(sequence(7)),
            Some(sequence(9)),
            None,
            None,
            2,
            [0x44; HASH_BYTES],
            mutations,
            None,
        )
        .expect("buffered encoder");
        assert_eq!(actual.as_bytes(), expected.as_bytes());
        assert_eq!(actual.frame_hash(), expected.frame_hash());
    }

    #[test]
    fn typed_command_segment_append_preserves_mutation_bytes_and_proven_counts() {
        let key = vec![0x31, 0x32];
        let value = vec![0x41, 0x42, 0x43];

        let mut ordinary = JournalMutationBuffer::default();
        ordinary
            .extend([
                JournalMutation::put(JournalTable::Commits, key.clone(), value.clone())
                    .expect("ordinary commit put"),
            ])
            .expect("buffer ordinary put");

        let mut typed = JournalMutationBuffer::default();
        let raw_envelope_bytes = value.len();
        typed
            .push_command_segment(key, value, 2, raw_envelope_bytes)
            .expect("buffer typed segment put");

        assert_eq!(typed.bytes, ordinary.bytes);
        assert_eq!(typed.mutation_count, ordinary.mutation_count);
        assert_eq!(typed.logical_command_put_count, 2);
        assert_eq!(typed.command_audit_count, 4);
    }

    #[test]
    fn typed_command_segment_carries_exact_raw_equivalent_frame_charge() {
        let key = vec![0x31, 0x32];
        let value = vec![0x41, 0x42, 0x43];
        let selected = value.len();
        let raw = selected + 101;
        let mut buffer = JournalMutationBuffer::default();
        buffer
            .push_command_segment(key, value, 2, raw)
            .expect("buffer compact segment");
        let frame = JournalFrame::encode_buffered_command(
            database_id(4),
            None,
            Some(sequence(2)),
            None,
            AdministrationSequence::new(4),
            2,
            [0x22; HASH_BYTES],
            buffer,
            None,
        )
        .expect("encode compact frame");
        assert_eq!(frame.selected_command_segment_bytes(), selected);
        assert_eq!(frame.raw_command_segment_bytes(), raw);
        assert_eq!(
            frame.raw_equivalent_frame_bytes(),
            frame.as_bytes().len() + raw - selected
        );
    }

    #[test]
    fn frame_rejects_torn_tail_unknown_table_and_sequence_mismatch() {
        let frame = sample_frame();
        let encoded = frame.encode().expect("encode");
        assert_eq!(
            JournalFrame::decode(&encoded.as_bytes()[..encoded.as_bytes().len() - 1]),
            Err(JournalCodecError::Truncated)
        );

        let mut unknown = encoded.as_bytes().to_vec();
        unknown[FRAME_HEADER_BYTES] = 0xff;
        let footer = unknown.len() - FRAME_FOOTER_BYTES;
        let hash = digest(&unknown[..footer]);
        unknown[footer + 16..].copy_from_slice(&hash);
        assert_eq!(
            JournalFrame::decode(&unknown),
            Err(JournalCodecError::UnknownTable)
        );

        assert_eq!(
            JournalFrame::new(
                database_id(3),
                sequence(8),
                sequence(10),
                2,
                [0; 32],
                vec![
                    JournalMutation::delete_matching(JournalTable::Entities, vec![1], &[9],)
                        .expect("delete")
                ],
            ),
            Err(JournalCodecError::InvalidValue)
        );
    }

    #[test]
    fn journal_bounds_keep_one_frame_and_unpublished_prefix_independent() {
        assert_eq!(MAX_JOURNAL_TRANSITIONS, 256);
        assert_eq!(MAX_JOURNAL_FRAME_BYTES, 16 * 1024 * 1024);
        assert_eq!(MAX_JOURNAL_SUFFIX_TRANSITIONS, 65_536);
        assert_eq!(MAX_JOURNAL_SUFFIX_BYTES, 256 * 1024 * 1024);
        assert_eq!(PRODUCTION_EXTENT_DATA_BYTES, 320 * 1024 * 1024);
        assert_eq!(LEGACY_EXTENT_DATA_BYTES, 40 * 1024 * 1024);
        assert_eq!(EXTENT_DATA_BYTES, LEGACY_EXTENT_DATA_BYTES, "test builds");
        let maximum_physical_frame =
            extent_frame_bytes(MAX_JOURNAL_FRAME_BYTES).expect("maximum frame fits extent");
        assert!(maximum_physical_frame > MAX_JOURNAL_FRAME_BYTES);
        assert!(maximum_physical_frame < LEGACY_EXTENT_DATA_BYTES);
        // Decision 7's extent holds its 256 MiB suffix ceiling plus one maximum
        // padded frame, so a full suffix never forces an early checkpoint.
        assert!(MAX_JOURNAL_SUFFIX_BYTES + maximum_physical_frame <= PRODUCTION_EXTENT_DATA_BYTES);
    }

    #[test]
    fn lane_groups_ready_frames_behind_fewer_durable_flushes() {
        let path = TestPath::new("grouped");
        let database_id = database_id(9);
        let header = JournalFileHeader::new(database_id, None, [0; 32]);
        let lane = JournalLane::open(&path.0, &header).expect("open lane");
        let mut receipts = Vec::new();
        let mut previous_hash = [0; 32];
        for ordinal in 1..=100_u64 {
            let frame = JournalFrame::new(
                database_id,
                sequence(ordinal),
                sequence(ordinal),
                1,
                previous_hash,
                vec![
                    JournalMutation::put(
                        JournalTable::Commits,
                        ordinal.to_be_bytes().to_vec(),
                        vec![0x55; 512],
                    )
                    .expect("mutation"),
                ],
            )
            .expect("frame")
            .encode()
            .expect("encode frame");
            previous_hash = frame.frame_hash();
            receipts.push(lane.submit(frame).expect("submit frame"));
        }
        for (index, receipt) in receipts.into_iter().enumerate() {
            assert_eq!(
                receipt.wait().expect("durable frame").covered_sequence,
                Some(sequence(u64::try_from(index + 1).expect("bounded index")))
            );
        }
        assert!(lane.durable_flushes() < 100);
    }

    #[test]
    fn cloned_receipts_observe_one_idempotent_durability_result() {
        let path = TestPath::new("shared-receipt");
        let database_id = database_id(10);
        let header = JournalFileHeader::new(database_id, None, [0; 32]);
        let lane = JournalLane::open(&path.0, &header).expect("open lane");
        let frame = JournalFrame::new(
            database_id,
            sequence(1),
            sequence(1),
            1,
            [0; 32],
            vec![JournalMutation::put(JournalTable::Commits, vec![1], vec![2]).expect("put")],
        )
        .expect("frame")
        .encode()
        .expect("encode");
        let first = lane.submit(frame).expect("submit");
        let second = first.clone();

        let expected = first.wait().expect("first receipt");
        assert_eq!(second.wait(), Ok(expected));
        assert_eq!(first.try_wait(), Ok(Some(expected)));
    }

    #[test]
    fn lane_rejects_a_cross_database_existing_header() {
        let path = TestPath::new("cross-database");
        let first = JournalFileHeader::new(database_id(1), None, [0; 32]);
        drop(JournalLane::open(&path.0, &first).expect("open first lane"));
        let second = JournalFileHeader::new(database_id(2), None, [0; 32]);
        assert!(matches!(
            JournalLane::open(&path.0, &second),
            Err(JournalIoError::Corrupt)
        ));
    }

    #[test]
    fn extent_header_recycle_advances_generation_without_zeroing_stale_frames() {
        let path = TestPath::new("extent-recycle");
        let database_id = database_id(21);
        let header = JournalFileHeader::new(database_id, None, [0; 32]);
        initialize_or_validate_file(&path.0, &header).expect("initialize extent");
        let frame = JournalFrame::new(
            database_id,
            sequence(1),
            sequence(1),
            1,
            [0; 32],
            vec![JournalMutation::put(JournalTable::Commits, vec![1], vec![2]).expect("put")],
        )
        .expect("frame")
        .encode()
        .expect("encode");
        write_physical_frame(&path.0, &frame, false);
        let before = inspect_extent(&path.0).expect("inspect first generation");
        assert_eq!(before.header.generation, 1);
        assert!(before.next_position > EXTENT_DATA_OFFSET);

        reset_journal(&path.0, &header).expect("recycle extent");
        let after = inspect_extent(&path.0).expect("inspect recycled generation");
        assert_eq!(after.header.generation, 2);
        assert_eq!(after.header.slot, 1);
        assert_eq!(after.next_position, EXTENT_DATA_OFFSET);
        let (_, tail) = scan_journal(&path.0, database_id, |_| Ok(()))
            .expect("scan recycled extent")
            .expect("extent exists");
        assert_eq!(tail.transition_count, 0);
        assert!(!tail.incomplete_tail);
    }

    #[test]
    fn recreated_spare_generation_stays_ahead_of_recovered_active_extent() {
        let active = TestPath::new("active-generation-predecessor");
        let spare = TestPath::new("recreated-spare-generation");
        let header = JournalFileHeader::new(database_id(44), None, [0; HASH_BYTES]);
        initialize_or_validate_file(&active.0, &header).expect("initialize active extent");
        for _ in 0..32 {
            reset_journal(&active.0, &header).expect("advance active generation");
        }
        let active_generation = inspect_extent(&active.0)
            .expect("inspect active generation")
            .header
            .generation;

        reset_journal_after_with_media(&RealJournalMedia, &spare.0, &header, &active.0)
            .expect("recreate spare after active generation");
        let first_spare_generation = inspect_extent(&spare.0)
            .expect("inspect recreated spare")
            .header
            .generation;
        assert!(first_spare_generation > active_generation);

        reset_journal_after_with_media(&RealJournalMedia, &spare.0, &header, &active.0)
            .expect("prepare existing spare again");
        let second_spare_generation = inspect_extent(&spare.0)
            .expect("inspect advanced spare")
            .header
            .generation;
        assert!(second_spare_generation > first_spare_generation);
    }

    #[test]
    fn shorter_recycled_suffix_writes_a_zero_tail_over_stale_frame_interior() {
        let path = TestPath::new("shorter-recycled-suffix");
        let database_id = database_id(24);
        let header = JournalFileHeader::new(database_id, None, [0; 32]);
        initialize_or_validate_file(&path.0, &header).expect("initialize extent");
        let stale = JournalFrame::new(
            database_id,
            sequence(1),
            sequence(1),
            1,
            [0; 32],
            vec![
                JournalMutation::put(JournalTable::Commits, vec![1], vec![7; 12_000])
                    .expect("stale put"),
            ],
        )
        .expect("stale frame")
        .encode()
        .expect("encode stale");
        write_physical_frame(&path.0, &stale, false);
        reset_journal(&path.0, &header).expect("recycle extent");

        let current = JournalFrame::new(
            database_id,
            sequence(1),
            sequence(1),
            1,
            [0; 32],
            vec![JournalMutation::put(JournalTable::Commits, vec![2], vec![8]).expect("put")],
        )
        .expect("current frame")
        .encode()
        .expect("encode current");
        {
            let lane = JournalLane::open(&path.0, &header).expect("open recycled lane");
            lane.submit(current)
                .expect("submit current")
                .wait()
                .expect("fence current");
        }

        let mut visited = 0;
        let (_, tail) = scan_journal(&path.0, database_id, |_| {
            visited += 1;
            Ok(())
        })
        .expect("scan current suffix")
        .expect("extent exists");
        assert_eq!(visited, 1);
        assert_eq!(tail.transition_count, 1);
        assert!(!tail.incomplete_tail);
    }

    #[test]
    fn current_generation_overwrite_cannot_join_stale_residue() {
        let path = TestPath::new("stale-residue");
        let database_id = database_id(22);
        let header = JournalFileHeader::new(database_id, None, [0; 32]);
        initialize_or_validate_file(&path.0, &header).expect("initialize extent");
        let stale = JournalFrame::new(
            database_id,
            sequence(1),
            sequence(1),
            1,
            [0; 32],
            vec![
                JournalMutation::put(JournalTable::Commits, vec![1], vec![7; 12_000])
                    .expect("stale put"),
            ],
        )
        .expect("stale frame")
        .encode()
        .expect("encode stale");
        write_physical_frame(&path.0, &stale, false);
        reset_journal(&path.0, &header).expect("recycle extent");

        let current = JournalFrame::new(
            database_id,
            sequence(1),
            sequence(1),
            1,
            [0; 32],
            vec![JournalMutation::put(JournalTable::Commits, vec![2], vec![8]).expect("put")],
        )
        .expect("current frame")
        .encode()
        .expect("encode current");
        write_physical_frame(&path.0, &current, true);

        let mut visited = 0;
        let (_, tail) = scan_journal(&path.0, database_id, |_| {
            visited += 1;
            Ok(())
        })
        .expect("scan torn current frame")
        .expect("extent exists");
        assert_eq!(visited, 0);
        assert_eq!(tail.transition_count, 0);
        assert!(tail.incomplete_tail);
    }

    // req: REC-005
    #[test]
    fn a_capacity_change_retires_an_extent_that_retains_frames_instead_of_replacing_it() {
        let database_path = TestPath::new("capacity-retained");
        let database_id = database_id(32);
        let (first, second) = sealing_extent(&database_path.0, database_id);
        let active = journal_path(&database_path.0);
        let sealing = sealing_journal_path(&database_path.0);
        std::fs::rename(&sealing, &active).expect("back to the active name");
        let (_, tail) = scan_journal(&active, database_id, |_| Ok(()))
            .expect("scan")
            .expect("extent exists");
        let anchored = JournalFileHeader::with_frontiers(
            database_id,
            tail.last_sequence,
            tail.last_administration_sequence,
            tail.last_hash,
        );
        advance_journal_anchor_with_media(&RealJournalMedia, &active, &anchored)
            .expect("materialized frames stay behind the anchor");
        let generation = inspect_extent(&active).expect("active").header.generation;

        normalize_active_capacity_to(
            &RealJournalMedia,
            &database_path.0,
            database_id,
            PRODUCTION_EXTENT_DATA_BYTES,
        )
        .expect("retire the retained extent");
        let retired = inspect_extent(&sealing).expect("retired extent");
        assert_eq!(retired.header.generation, generation);
        let successor = inspect_extent(&active).expect("successor");
        assert_eq!(successor.header.generation, generation + 1);
        assert_eq!(successor.header.logical, anchored, "the chain continues");
        assert_eq!(successor.next_position, EXTENT_DATA_OFFSET);

        // History stubs naming the retired frames still resolve, before and
        // after the retired extent is sealed.
        let placement =
            frame_placement_after(frame_physical_bytes(&first), &second).expect("placement");
        for _ in 0..2 {
            let frame = read_retained_frame_with_media(
                &RealJournalMedia,
                &database_path.0,
                database_id,
                placement,
                generation,
            )
            .expect("retained frame");
            assert_eq!(frame.covered_sequence(), Some(sequence(2)));
            seal_extent_with_media(&RealJournalMedia, &database_path.0, database_id).expect("seal");
        }
        assert!(sealed_journal_path(&database_path.0, generation).exists());
    }

    // req: REC-005
    #[test]
    fn a_renewed_extent_rises_above_every_retained_generation_and_keeps_its_anchor() {
        let database_path = TestPath::new("renew-above");
        let database_id = database_id(35);
        let _ = sealing_extent(&database_path.0, database_id);
        let active = journal_path(&database_path.0);
        std::fs::rename(sealing_journal_path(&database_path.0), &active).expect("activate");
        let (_, tail) = scan_journal(&active, database_id, |_| Ok(()))
            .expect("scan")
            .expect("extent exists");
        let anchored = JournalFileHeader::with_frontiers(
            database_id,
            tail.last_sequence,
            tail.last_administration_sequence,
            tail.last_hash,
        );
        advance_journal_anchor_with_media(&RealJournalMedia, &active, &anchored).expect("anchor");
        let before = inspect_extent(&active).expect("before");

        renew_extent_above_with_media(&RealJournalMedia, &active, 0).expect("already above");
        assert_eq!(inspect_extent(&active).expect("unchanged"), before);

        let floor = before.header.generation + 5;
        renew_extent_above_with_media(&RealJournalMedia, &active, floor).expect("renew");
        let renewed = inspect_extent(&active).expect("renewed");
        assert_eq!(renewed.header.generation, floor + 1);
        assert_eq!(renewed.header.logical, anchored, "the chain continues");
        assert_eq!(
            renewed.next_position, EXTENT_DATA_OFFSET,
            "no retained prefix"
        );
        assert_eq!(
            extent_generation_with_media(&RealJournalMedia, &active).expect("generation"),
            Some(floor + 1)
        );
    }

    // req: REC-005
    #[test]
    fn a_missing_active_extent_is_recreated_after_the_extent_waiting_to_seal() {
        let database_path = TestPath::new("missing-active");
        let database_id = database_id(33);
        let _ = sealing_extent(&database_path.0, database_id);
        let sealing = sealing_journal_path(&database_path.0);
        let predecessor = inspect_extent(&sealing).expect("sealing extent");
        let active = journal_path(&database_path.0);
        assert!(!active.exists());

        recreate_missing_active_with_media(&RealJournalMedia, &database_path.0)
            .expect("recreate the active extent");
        let recreated = inspect_extent(&active).expect("recreated");
        assert_eq!(
            recreated.header.generation,
            predecessor.header.generation + 1,
            "never a generation a retained extent can hold"
        );
        assert_eq!(recreated.header.logical, predecessor.header.logical);
        recreate_missing_active_with_media(&RealJournalMedia, &database_path.0)
            .expect("an existing active extent is left alone");
        assert_eq!(inspect_extent(&active).expect("unchanged"), recreated);
    }

    // req: REC-005
    #[test]
    fn a_legacy_capacity_extent_is_replaced_at_the_configured_capacity_only_when_empty() {
        let path = TestPath::new("capacity-empty");
        let database_id = database_id(31);
        let header = JournalFileHeader::new(database_id, Some(sequence(9)), [0x31; 32]);
        initialize_or_validate_file(&path.0, &header).expect("initialize legacy extent");
        assert_eq!(
            inspect_extent(&path.0).expect("legacy").header.data_bytes,
            LEGACY_EXTENT_DATA_BYTES
        );

        assert!(
            normalize_extent_capacity_to(&RealJournalMedia, &path.0, PRODUCTION_EXTENT_DATA_BYTES)
                .expect("replace empty legacy extent")
        );
        let replaced = inspect_extent(&path.0).expect("replaced extent");
        assert_eq!(replaced.header.data_bytes, PRODUCTION_EXTENT_DATA_BYTES);
        assert_eq!(replaced.header.generation, 2);
        assert_eq!(
            replaced.header.logical, header,
            "the checkpoint anchor carries over"
        );
        assert_eq!(replaced.next_position, EXTENT_DATA_OFFSET);
        assert_eq!(
            usize::try_from(std::fs::metadata(&path.0).expect("metadata").len())
                .expect("bounded file length"),
            extent_file_bytes(PRODUCTION_EXTENT_DATA_BYTES)
        );
        assert!(
            !normalize_extent_capacity_to(&RealJournalMedia, &path.0, PRODUCTION_EXTENT_DATA_BYTES)
                .expect("already configured"),
            "an extent at the configured capacity is left alone"
        );

        let nonempty = TestPath::new("capacity-nonempty");
        let header = JournalFileHeader::new(database_id, None, [0; 32]);
        initialize_or_validate_file(&nonempty.0, &header).expect("initialize legacy extent");
        let frame = JournalFrame::new(
            database_id,
            sequence(1),
            sequence(1),
            1,
            [0; 32],
            vec![JournalMutation::put(JournalTable::Commits, vec![1], vec![7]).expect("put")],
        )
        .expect("frame")
        .encode()
        .expect("encode");
        write_physical_frame(&nonempty.0, &frame, false);
        assert_eq!(
            normalize_extent_capacity_to(
                &RealJournalMedia,
                &nonempty.0,
                PRODUCTION_EXTENT_DATA_BYTES
            ),
            Err(JournalIoError::Corrupt),
            "a suffix that recovery has not re-anchored is never dropped"
        );
        assert_eq!(
            inspect_extent(&nonempty.0)
                .expect("unchanged")
                .header
                .data_bytes,
            LEGACY_EXTENT_DATA_BYTES
        );
    }

    // req: REC-005
    #[test]
    fn an_extent_header_with_an_unknown_capacity_is_refused() {
        let path = TestPath::new("capacity-unknown");
        let header = JournalFileHeader::new(database_id(32), None, [0; 32]);
        let state = initialize_or_validate_file(&path.0, &header).expect("initialize extent");
        let mut bytes = state.header.encode();
        bytes[96..104].copy_from_slice(&(48_u64 * 1024 * 1024).to_be_bytes());
        let checksum = digest(&bytes[..EXTENT_HEADER_CHECKSUM_OFFSET]);
        bytes[EXTENT_HEADER_CHECKSUM_OFFSET..].copy_from_slice(&checksum);
        assert_eq!(
            ExtentHeader::decode(&bytes, 0),
            Err(JournalCodecError::UnknownVersion)
        );
    }

    fn sample_changelog_binding() -> FrameChangelogBinding {
        FrameChangelogBinding {
            history_incarnation: 7,
            predecessor: riffdb_storage_api::ChangelogTransactionSequence::new(41),
            sequence: riffdb_storage_api::ChangelogTransactionSequence::new(42).expect("nonzero"),
            prior_history_hash: [0x5a; HASH_BYTES],
            catalog_digest: [0xc3; HASH_BYTES],
        }
    }

    // req: REC-005
    #[test]
    fn a_version_3_frame_round_trips_its_changelog_binding_and_version_2_carries_none() {
        let binding = sample_changelog_binding();
        let frame = sample_frame().with_changelog(Some(binding));
        let encoded = frame.encode().expect("encode v3");
        assert_eq!(
            read_u16(encoded.as_bytes(), 8).expect("version"),
            FORMAT_VERSION
        );
        let (decoded, hash) = JournalFrame::decode(encoded.as_bytes()).expect("decode v3");
        assert_eq!(hash, encoded.frame_hash());
        assert_eq!(decoded.changelog(), Some(&binding));
        assert_eq!(decoded, frame);

        let unbound = sample_frame();
        let (decoded, _) = JournalFrame::decode(unbound.encode().expect("encode").as_bytes())
            .expect("decode unbound v3");
        assert_eq!(decoded.changelog(), None);
        let legacy = unbound.encode_version_2().expect("encode v2");
        assert_eq!(
            legacy.as_bytes().len() + FRAME_BINDING_BYTES,
            unbound.encode().expect("encode").as_bytes().len()
        );
        let (decoded, _) = JournalFrame::decode(legacy.as_bytes()).expect("decode v2");
        assert_eq!(decoded.changelog(), None);
        assert_eq!(
            decoded,
            JournalFrame {
                format_version: FRAME_VERSION_V2,
                ..unbound
            }
        );
    }

    fn hex_bytes(text: &str) -> Vec<u8> {
        let text = text.trim();
        (0..text.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&text[index..index + 2], 16).expect("hex fixture"))
            .collect()
    }

    fn lower_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    // req: STO-030, STO-032, REC-005
    #[test]
    fn retention_frame_version_preserves_frozen_prior_readers() {
        assert_eq!(
            FRAME_VERSION_V4, 4,
            "the frozen pruning format remains distinct"
        );
        for fixture in [
            include_str!("../../../fixtures/compatibility/journal-frame-v2.hex"),
            include_str!("../../../fixtures/compatibility/journal-frame-v3.hex"),
        ] {
            let bytes = hex_bytes(fixture);
            let (decoded, hash) =
                JournalFrame::decode(&bytes).expect("frozen prior frame remains readable");
            let round_trip = decoded.encode().expect("preserve original version");
            assert_eq!(round_trip.as_bytes(), bytes);
            assert_eq!(round_trip.frame_hash(), hash);
        }
    }

    // req: REC-005
    #[test]
    fn journal_frame_encodings_match_their_compatibility_fixtures() {
        let v2 = include_str!("../../../fixtures/compatibility/journal-frame-v2.hex");
        let v3 = include_str!("../../../fixtures/compatibility/journal-frame-v3.hex");
        let unbound = sample_frame();
        let bound = sample_frame().with_changelog(Some(sample_changelog_binding()));
        let encoded_v2 = unbound.encode_version_2().expect("encode v2");
        let encoded_v3 = bound.encode_as(FRAME_VERSION_V3).expect("encode v3");
        if std::env::var_os("RIFFDB_REGENERATE_JOURNAL_FRAME_FIXTURES").is_some() {
            let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/compatibility");
            std::fs::write(
                root.join("journal-frame-v2.hex"),
                lower_hex(encoded_v2.as_bytes()) + "\n",
            )
            .expect("write v2 fixture");
            std::fs::write(
                root.join("journal-frame-v3.hex"),
                lower_hex(encoded_v3.as_bytes()) + "\n",
            )
            .expect("write v3 fixture");
            return;
        }
        assert_eq!(lower_hex(encoded_v2.as_bytes()), v2.trim());
        assert_eq!(lower_hex(encoded_v3.as_bytes()), v3.trim());
        assert_eq!(
            JournalFrame::decode(&hex_bytes(v2)).expect("decode v2").0,
            JournalFrame {
                format_version: FRAME_VERSION_V2,
                ..unbound
            }
        );
        let (decoded, _) = JournalFrame::decode(&hex_bytes(v3)).expect("decode v3");
        assert_eq!(
            decoded,
            JournalFrame {
                format_version: FRAME_VERSION_V3,
                ..bound
            }
        );
        assert_eq!(decoded.changelog(), Some(&sample_changelog_binding()));
    }

    fn sample_extent_header(version: u16) -> [u8; EXTENT_HEADER_SLOT_BYTES] {
        let logical = JournalFileHeader::new(database_id(33), Some(sequence(9)), [0x33; 32]);
        let header = ExtentHeader::initial(logical.clone());
        if version == EXTENT_HEADER_VERSION_V3 {
            let mut bytes = header.encode();
            bytes[8..10].copy_from_slice(&EXTENT_HEADER_VERSION_V3.to_be_bytes());
            bytes[104..120].fill(0);
            let checksum = digest(&bytes[..EXTENT_HEADER_CHECKSUM_OFFSET]);
            bytes[EXTENT_HEADER_CHECKSUM_OFFSET..].copy_from_slice(&checksum);
            return bytes;
        }
        header
            .anchored(logical, EXTENT_DATA_OFFSET + 3 * EXTENT_FRAME_ALIGNMENT)
            .expect("anchored header")
            .encode()
    }

    // req: REC-005
    #[test]
    fn sealed_footer_encoding_matches_its_compatibility_fixture() {
        let fixture = include_str!("../../../fixtures/compatibility/journal-sealed-footer-v1.hex");
        let first =
            recovery_frame(database_id(40)).with_changelog(Some(sample_changelog_binding()));
        let second = recovery_frame(database_id(40));
        let footer = SealedExtentFooter {
            generation: 7,
            origin: (None, None, [0; HASH_BYTES]),
            entries: vec![
                SealedFrameEntry::from_frame(
                    &first,
                    FramePlacement {
                        position: EXTENT_DATA_OFFSET,
                        padded_len: EXTENT_FRAME_ALIGNMENT,
                        frame_hash: [0x31; HASH_BYTES],
                    },
                ),
                SealedFrameEntry::from_frame(
                    &second,
                    FramePlacement {
                        position: EXTENT_DATA_OFFSET + EXTENT_FRAME_ALIGNMENT,
                        padded_len: 2 * EXTENT_FRAME_ALIGNMENT,
                        frame_hash: [0x32; HASH_BYTES],
                    },
                ),
            ],
        };
        let encoded = footer.encode().expect("encode footer");
        if std::env::var_os("RIFFDB_REGENERATE_JOURNAL_FRAME_FIXTURES").is_some() {
            let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/compatibility");
            std::fs::write(
                root.join("journal-sealed-footer-v1.hex"),
                lower_hex(&encoded) + "\n",
            )
            .expect("write sealed footer fixture");
            return;
        }
        assert_eq!(lower_hex(&encoded), fixture.trim());
        assert_eq!(
            SealedExtentFooter::decode(&hex_bytes(fixture)).expect("decode fixture"),
            footer
        );
    }

    // req: REC-005
    #[test]
    fn extent_header_encodings_match_their_compatibility_fixtures() {
        let v3 = include_str!("../../../fixtures/compatibility/journal-extent-header-v3.hex");
        let v4 = include_str!("../../../fixtures/compatibility/journal-extent-header-v4.hex");
        let encoded_v3 = sample_extent_header(EXTENT_HEADER_VERSION_V3);
        let encoded_v4 = sample_extent_header(EXTENT_FORMAT_VERSION);
        if std::env::var_os("RIFFDB_REGENERATE_JOURNAL_FRAME_FIXTURES").is_some() {
            let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/compatibility");
            std::fs::write(
                root.join("journal-extent-header-v3.hex"),
                lower_hex(&encoded_v3) + "\n",
            )
            .expect("write v3 header fixture");
            std::fs::write(
                root.join("journal-extent-header-v4.hex"),
                lower_hex(&encoded_v4) + "\n",
            )
            .expect("write v4 header fixture");
            return;
        }
        assert_eq!(lower_hex(&encoded_v3), v3.trim());
        assert_eq!(lower_hex(&encoded_v4), v4.trim());
        let legacy = ExtentHeader::decode(&hex_bytes(v3), 0).expect("decode v3 header");
        assert_eq!(
            (legacy.revision, legacy.anchor_position),
            (0, EXTENT_DATA_OFFSET)
        );
        let current = ExtentHeader::decode(&hex_bytes(v4), 1).expect("decode v4 header");
        assert_eq!(
            (current.revision, current.anchor_position),
            (1, EXTENT_DATA_OFFSET + 3 * EXTENT_FRAME_ALIGNMENT)
        );
        assert_eq!(current.generation, legacy.generation);
        assert_eq!(current.logical, legacy.logical);
    }

    // req: REC-005
    #[test]
    fn a_non_canonical_changelog_binding_block_is_refused() {
        let mut valid = FrameChangelogBinding::encode(Some(&sample_changelog_binding()));
        assert!(
            FrameChangelogBinding::decode(&valid)
                .expect("valid")
                .is_some()
        );
        let mutations: [fn(&mut [u8; FRAME_BINDING_BYTES]); 5] = [
            |bytes| bytes[0] = 2,
            |bytes| bytes[3] = 1,
            |bytes| bytes[24..32].copy_from_slice(&0_u64.to_be_bytes()),
            |bytes| bytes[24..32].copy_from_slice(&43_u64.to_be_bytes()),
            |bytes| bytes[8..16].copy_from_slice(&0_u64.to_be_bytes()),
        ];
        for mutate in mutations {
            let mut bytes = valid;
            mutate(&mut bytes);
            assert_eq!(
                FrameChangelogBinding::decode(&bytes),
                Err(JournalCodecError::InvalidValue)
            );
        }
        valid = [0; FRAME_BINDING_BYTES];
        valid[40] = 1;
        assert_eq!(
            FrameChangelogBinding::decode(&valid),
            Err(JournalCodecError::InvalidValue),
            "an absent binding is all zero"
        );
    }

    #[test]
    fn torn_inactive_header_falls_back_to_the_complete_generation() {
        let path = TestPath::new("torn-recycle-header");
        let database_id = database_id(23);
        let header = JournalFileHeader::new(database_id, None, [0; 32]);
        let state = initialize_or_validate_file(&path.0, &header).expect("initialize extent");
        let successor = state
            .header
            .successor(header.clone())
            .expect("successor header")
            .encode();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path.0)
            .expect("open extent");
        write_all_at(
            &mut file,
            &successor[..successor.len() / 2],
            EXTENT_HEADER_SLOT_BYTES,
        )
        .expect("write torn inactive header");
        file.sync_data().expect("sync torn header");
        let selected = inspect_extent(&path.0).expect("select complete header");
        assert_eq!(selected.header.generation, 1);
        assert_eq!(selected.header.slot, 0);
    }

    #[test]
    fn legacy_journal_activation_accepts_only_an_empty_suffix() {
        let empty = TestPath::new("legacy-empty");
        let database_id = database_id(24);
        let header = JournalFileHeader::new(database_id, None, [0; 32]);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&empty.0)
            .expect("create legacy empty journal");
        file.write_all(&header.encode())
            .expect("write legacy header");
        file.sync_data().expect("sync legacy header");
        drop(file);
        let migrated =
            initialize_or_validate_file(&empty.0, &header).expect("migrate empty legacy");
        assert_eq!(migrated.header.generation, 1);
        assert_eq!(
            usize::try_from(std::fs::metadata(&empty.0).expect("metadata").len())
                .expect("bounded file length"),
            EXTENT_FILE_BYTES
        );

        let nonempty = TestPath::new("legacy-nonempty");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&nonempty.0)
            .expect("create legacy nonempty journal");
        file.write_all(&header.encode())
            .expect("write legacy header");
        let frame = JournalFrame::new(
            database_id,
            sequence(1),
            sequence(1),
            1,
            [0; 32],
            vec![JournalMutation::put(JournalTable::Commits, vec![1], vec![2]).expect("put")],
        )
        .expect("legacy frame")
        .encode_version_2()
        .expect("encode legacy frame");
        file.write_all(frame.as_bytes())
            .expect("write legacy suffix");
        file.sync_data().expect("sync legacy suffix");
        drop(file);
        assert_eq!(
            initialize_or_validate_file(&nonempty.0, &header),
            Err(JournalIoError::LegacyNonEmpty(nonempty.0.clone()))
        );
    }

    #[test]
    fn extent_preflight_maps_only_storage_exhaustion_to_capacity() {
        assert_eq!(
            classify_extent_creation_error(&io::Error::from(io::ErrorKind::StorageFull)),
            JournalIoError::Capacity
        );
        assert_eq!(
            classify_extent_creation_error(&io::Error::other("injected non-capacity failure")),
            JournalIoError::Io
        );
    }

    #[test]
    fn incomplete_preallocation_is_rejected_before_lane_readiness() {
        let path = TestPath::new("short-extent");
        let database_id = database_id(25);
        let header = JournalFileHeader::new(database_id, None, [0; 32]);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path.0)
            .expect("create short extent");
        file.write_all(&ExtentHeader::initial(header.clone()).encode())
            .expect("write extent header");
        file.set_len(u64::try_from(EXTENT_DATA_OFFSET).expect("bounded offset"))
            .expect("truncate extent");
        file.sync_data().expect("sync short extent");
        drop(file);
        assert!(matches!(
            JournalLane::open(&path.0, &header),
            Err(JournalIoError::Corrupt)
        ));
    }

    #[test]
    fn scanner_visits_a_gap_free_chain_and_reports_the_exact_tail() {
        let path = TestPath::new("scan-complete");
        let database_id = database_id(10);
        let header = JournalFileHeader::new(database_id, None, [0; 32]);
        initialize_or_validate_file(&path.0, &header).expect("initialize journal");

        let first = JournalFrame::new(
            database_id,
            sequence(1),
            sequence(1),
            1,
            [0; 32],
            vec![
                JournalMutation::put(JournalTable::Commits, vec![1], vec![2])
                    .expect("first mutation"),
            ],
        )
        .expect("first frame")
        .encode()
        .expect("encode first");
        let second = JournalFrame::new(
            database_id,
            sequence(2),
            sequence(2),
            1,
            first.frame_hash(),
            vec![
                JournalMutation::put(JournalTable::Commits, vec![3], vec![4])
                    .expect("second mutation"),
            ],
        )
        .expect("second frame")
        .encode()
        .expect("encode second");
        write_physical_frame(&path.0, &first, false);
        write_physical_frame(&path.0, &second, false);

        let mut visited = Vec::new();
        let (actual_header, tail) = scan_journal(&path.0, database_id, |frame| {
            visited.push((frame.predecessor_sequence(), frame.covered_sequence()));
            Ok(())
        })
        .expect("scan")
        .expect("journal exists");
        assert_eq!(actual_header, header);
        assert_eq!(
            visited,
            vec![
                (None, Some(sequence(1))),
                (Some(sequence(1)), Some(sequence(2)))
            ]
        );
        assert_eq!(tail.last_sequence, Some(sequence(2)));
        assert_eq!(tail.last_hash, second.frame_hash());
        assert!(!tail.incomplete_tail);
    }

    #[test]
    fn scanner_ignores_only_an_incomplete_terminal_frame() {
        let path = TestPath::new("scan-torn-tail");
        let database_id = database_id(11);
        let header = JournalFileHeader::new(database_id, None, [0; 32]);
        initialize_or_validate_file(&path.0, &header).expect("initialize journal");
        let first = JournalFrame::new(
            database_id,
            sequence(1),
            sequence(1),
            1,
            [0; 32],
            vec![
                JournalMutation::put(JournalTable::Commits, vec![1], vec![2])
                    .expect("first mutation"),
            ],
        )
        .expect("first frame")
        .encode()
        .expect("encode first");
        let second = JournalFrame::new(
            database_id,
            sequence(2),
            sequence(2),
            1,
            first.frame_hash(),
            vec![
                JournalMutation::put(JournalTable::Commits, vec![3], vec![4])
                    .expect("second mutation"),
            ],
        )
        .expect("second frame")
        .encode()
        .expect("encode second");
        write_physical_frame(&path.0, &first, false);
        write_physical_frame(&path.0, &second, true);

        let mut visited = 0;
        let (_, tail) = scan_journal(&path.0, database_id, |_| {
            visited += 1;
            Ok(())
        })
        .expect("scan")
        .expect("journal exists");
        assert_eq!(visited, 1);
        assert_eq!(tail.last_sequence, Some(sequence(1)));
        assert_eq!(tail.last_hash, first.frame_hash());
        assert!(tail.incomplete_tail);
    }

    #[test]
    fn scanner_rejects_a_complete_reordered_frame() {
        let path = TestPath::new("scan-reordered");
        let database_id = database_id(12);
        let header = JournalFileHeader::new(database_id, None, [0; 32]);
        initialize_or_validate_file(&path.0, &header).expect("initialize journal");
        let frame = JournalFrame::new(
            database_id,
            sequence(2),
            sequence(2),
            1,
            [0; 32],
            vec![JournalMutation::put(JournalTable::Commits, vec![1], vec![2]).expect("mutation")],
        )
        .expect("frame")
        .encode()
        .expect("encode");
        write_physical_frame(&path.0, &frame, false);

        assert_eq!(
            scan_journal(&path.0, database_id, |_| Ok(())),
            Err(JournalIoError::Corrupt)
        );
    }

    // req: REC-001, REC-002, REC-005, REC-008
    #[test]
    fn offline_journal_verification_requires_exact_materialization_without_replay_or_reanchor() {
        let path = TestPath::new("offline-materialized-journal");
        let database = create_database(&path.0);
        let database_id = database_id(0x69);
        let frame = recovery_frame(database_id);
        let encoded = frame.encode().unwrap();
        write_recovery_journal(&path.0, database_id, &encoded, None);
        let journal = journal_path(&path.0);
        let before = std::fs::read(&journal).unwrap();
        assert_eq!(
            verify_materialized_journal_with_media(
                &RealJournalMedia,
                &database,
                &journal,
                database_id,
            ),
            Err(JournalIoError::Io)
        );
        assert_eq!(std::fs::read(&journal).unwrap(), before);
        assert_eq!(read_redb_frontier(&database).unwrap(), (None, None));
        replay_frames(&database, std::slice::from_ref(&frame), &[None]).unwrap();
        verify_materialized_journal_with_media(&RealJournalMedia, &database, &journal, database_id)
            .unwrap();
        assert_eq!(std::fs::read(&journal).unwrap(), before);
        assert_eq!(read_redb_frontier(&database).unwrap().0, Some(sequence(1)));
        let write = database.begin_write().unwrap();
        write
            .open_table(ENTITIES)
            .unwrap()
            .insert([0x10].as_slice(), [0x21].as_slice())
            .unwrap();
        write.commit().unwrap();
        assert_eq!(
            verify_materialized_journal_with_media(
                &RealJournalMedia,
                &database,
                &journal,
                database_id,
            ),
            Err(JournalIoError::Corrupt)
        );
        assert_eq!(std::fs::read(&journal).unwrap(), before);
    }

    // req: REC-005
    #[test]
    fn recovery_replays_a_complete_suffix_and_anchors_after_it() {
        let database_path = TestPath::new("recovery-replay");
        let database = create_database(&database_path.0);
        let database_id = database_id(13);
        let frame = recovery_frame(database_id).encode().expect("encode frame");
        write_recovery_journal(&database_path.0, database_id, &frame, None);

        recover_journal(&database, &database_path.0, database_id).expect("recover journal");
        let transaction = database.begin_read().expect("begin read");
        let entities = transaction.open_table(ENTITIES).expect("open entities");
        assert_eq!(
            entities
                .get([0x10].as_slice())
                .expect("read entity")
                .map(|value| value.value().to_vec()),
            Some(vec![0x20])
        );
        let (_, tail) = scan_journal(&journal_path(&database_path.0), database_id, |_| Ok(()))
            .expect("scan anchored journal")
            .expect("journal exists");
        assert_eq!(tail.last_sequence, Some(sequence(1)));
        assert_eq!(tail.command_count, 0, "no suffix remains after the anchor");
        assert!(!tail.incomplete_tail);
        let anchored = inspect_extent(&journal_path(&database_path.0)).expect("anchored extent");
        assert_eq!(
            anchored.header.anchor_position,
            EXTENT_DATA_OFFSET + frame_physical_bytes(&frame),
            "the replayed frame is retained before the anchor"
        );
        assert_eq!(anchored.header.revision, 1);
        recover_journal(&database, &database_path.0, database_id).expect("recover again");
        assert_eq!(
            inspect_extent(&journal_path(&database_path.0)).expect("unchanged"),
            anchored,
            "an exact anchor is not rewritten"
        );
    }

    // req: REC-005
    #[test]
    fn frames_appended_after_an_anchor_are_retained_through_a_second_recovery() {
        let database_path = TestPath::new("recovery-anchor-twice");
        let database = create_database(&database_path.0);
        let database_id = database_id(16);
        let first = recovery_frame(database_id).encode().expect("encode first");
        write_recovery_journal(&database_path.0, database_id, &first, None);
        recover_journal(&database, &database_path.0, database_id).expect("first recovery");
        let path = journal_path(&database_path.0);
        let first_anchor = inspect_extent(&path).expect("first anchor").header;

        let second = JournalFrame::new(
            database_id,
            sequence(2),
            sequence(2),
            1,
            first.frame_hash(),
            vec![
                JournalMutation::put(
                    JournalTable::Commits,
                    crate::keys::encode_application_sequence_key(sequence(2)).to_vec(),
                    vec![0x42],
                )
                .expect("commit mutation"),
                JournalMutation::put(JournalTable::Entities, vec![0x11], vec![0x21])
                    .expect("entity mutation"),
            ],
        )
        .expect("second frame")
        .encode()
        .expect("encode second");
        write_physical_frame(&path, &second, false);
        recover_journal(&database, &database_path.0, database_id).expect("second recovery");

        let anchored = inspect_extent(&path).expect("second anchor").header;
        assert_eq!(
            anchored.generation, first_anchor.generation,
            "no generation bump"
        );
        assert_eq!(anchored.revision, first_anchor.revision + 1);
        assert_eq!(
            anchored.anchor_position,
            first_anchor.anchor_position + frame_physical_bytes(&second),
            "both frames stay in the extent, before the anchor"
        );
        assert_eq!(anchored.logical.checkpoint_sequence(), Some(sequence(2)));
        let transaction = database.begin_read().expect("begin read");
        let entities = transaction.open_table(ENTITIES).expect("open entities");
        assert!(entities.get([0x11].as_slice()).expect("read").is_some());
    }

    // req: REC-005
    #[test]
    fn a_torn_anchored_header_falls_back_to_the_previous_revision() {
        let database_path = TestPath::new("torn-anchor-header");
        let database = create_database(&database_path.0);
        let database_id = database_id(17);
        let frame = recovery_frame(database_id).encode().expect("encode frame");
        write_recovery_journal(&database_path.0, database_id, &frame, None);
        let path = journal_path(&database_path.0);
        let before = inspect_extent(&path).expect("before").header;
        let anchored = before
            .anchored(
                JournalFileHeader::with_frontiers(
                    database_id,
                    Some(sequence(1)),
                    None,
                    frame.frame_hash(),
                ),
                EXTENT_DATA_OFFSET + frame_physical_bytes(&frame),
            )
            .expect("anchored")
            .encode();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open extent");
        write_all_at(
            &mut file,
            &anchored[..anchored.len() / 2],
            EXTENT_HEADER_SLOT_BYTES,
        )
        .expect("write torn anchored header");
        file.sync_data().expect("sync");
        drop(file);
        assert_eq!(inspect_extent(&path).expect("fallback").header, before);
        recover_journal(&database, &database_path.0, database_id)
            .expect("recovery replays the frame the torn anchor would have covered");
        assert_eq!(
            inspect_extent(&path)
                .expect("anchored")
                .header
                .anchor_position,
            EXTENT_DATA_OFFSET + frame_physical_bytes(&frame)
        );
    }

    /// A two-frame extent at the sealing name, as an installed checkpoint
    /// leaves it, plus the two encoded frames.
    fn sealing_extent(
        database_path: &Path,
        database_id: DatabaseId,
    ) -> (EncodedJournalFrame, EncodedJournalFrame) {
        let first = recovery_frame(database_id)
            .with_changelog(Some(sample_changelog_binding()))
            .encode()
            .expect("encode first");
        write_recovery_journal(database_path, database_id, &first, None);
        let second = JournalFrame::new(
            database_id,
            sequence(2),
            sequence(2),
            1,
            first.frame_hash(),
            vec![
                JournalMutation::put(
                    JournalTable::Commits,
                    crate::keys::encode_application_sequence_key(sequence(2)).to_vec(),
                    vec![0x42],
                )
                .expect("commit mutation"),
            ],
        )
        .expect("second frame")
        .encode()
        .expect("encode second");
        write_physical_frame(&journal_path(database_path), &second, false);
        std::fs::rename(
            journal_path(database_path),
            sealing_journal_path(database_path),
        )
        .expect("move to the sealing name");
        (first, second)
    }

    // req: REC-005
    #[test]
    fn a_sealed_extent_can_exceed_the_online_retention_step_bound() {
        let database_path = TestPath::new("seal-retention-bound");
        let database_id = database_id(37);
        let first = recovery_frame(database_id).encode().expect("first frame");
        write_recovery_journal(&database_path.0, database_id, &first, None);
        let mut previous_hash = first.frame_hash();
        let count = crate::retention::ONLINE_RETENTION_STEP_SEQUENCES + 1;
        for ordinal in 2..=count {
            let frame = JournalFrame::new(
                database_id,
                sequence(ordinal),
                sequence(ordinal),
                1,
                previous_hash,
                vec![
                    JournalMutation::put(
                        JournalTable::Commits,
                        crate::keys::encode_application_sequence_key(sequence(ordinal)).to_vec(),
                        vec![0x42],
                    )
                    .expect("commit mutation"),
                ],
            )
            .expect("bounded frame")
            .encode()
            .expect("encoded frame");
            write_physical_frame(&journal_path(&database_path.0), &frame, false);
            previous_hash = frame.frame_hash();
        }
        std::fs::rename(
            journal_path(&database_path.0),
            sealing_journal_path(&database_path.0),
        )
        .expect("installed checkpoint extent");
        let sealed = seal_extent_with_media(&RealJournalMedia, &database_path.0, database_id)
            .expect("legal extent seals")
            .expect("sealed extent");
        let footer = read_sealed_extent_with_media(&RealJournalMedia, &sealed, database_id)
            .expect("every frame and footer validates");
        assert_eq!(footer.entries.len(), usize::try_from(count).expect("count"));
        assert_eq!(footer.entries[0].predecessor_sequence, None);
        assert_eq!(
            footer.entries.last().unwrap().covered_sequence,
            Some(sequence(count))
        );
        assert!(count > crate::retention::ONLINE_RETENTION_STEP_SEQUENCES);
    }

    // req: STO-032
    #[test]
    fn recycled_extent_preserves_allocated_frames_but_fences_every_stale_byte() {
        let database_path = TestPath::new("reuse-sealed-extent");
        let database_id = database_id(38);
        let (first, second) = sealing_extent(&database_path.0, database_id);
        let sealed = seal_extent_with_media(&RealJournalMedia, &database_path.0, database_id)
            .unwrap()
            .unwrap();
        let prior = read_sealed_extent_with_media(&RealJournalMedia, &sealed, database_id).unwrap();
        let old = std::fs::read(&sealed).unwrap();
        let index = read_sealed_index_with_media(&RealJournalMedia, &sealed, database_id).unwrap();
        assert_eq!(index, prior);
        std::fs::rename(&sealed, recycled_journal_path(&database_path.0)).unwrap();
        let header = JournalFileHeader::new(database_id, Some(sequence(2)), second.frame_hash());
        assert!(
            prepare_spare_extent_with_media(
                &RealJournalMedia,
                &database_path.0,
                &header,
                &std::sync::Mutex::new(())
            )
            .unwrap()
        );
        let spare = spare_journal_path(&database_path.0);
        let state = inspect_extent(&spare).unwrap();
        assert!(state.header.generation > prior.generation);
        assert_eq!(state.header.anchor_position, EXTENT_DATA_OFFSET);
        let (_, tail) = scan_journal(&spare, database_id, |_| panic!("stale frame replayed"))
            .unwrap()
            .unwrap();
        assert_eq!(tail.transition_count, 0);
        assert!(!tail.incomplete_tail);
        let reused = std::fs::read(&spare).unwrap();
        let first_end = EXTENT_DATA_OFFSET + extent_frame_bytes(first.as_bytes().len()).unwrap();
        assert_eq!(
            &reused[EXTENT_DATA_OFFSET..first_end],
            &old[EXTENT_DATA_OFFSET..first_end]
        );
        assert_eq!(reused.len(), extent_file_bytes(EXTENT_DATA_BYTES));
        assert!(!recycled_journal_path(&database_path.0).exists());
        assert!(!preparing_spare_journal_path(&database_path.0).exists());
    }

    // req: REC-005
    #[test]
    fn sealing_writes_a_footer_indexing_every_retained_frame_and_trims_the_extent() {
        let database_path = TestPath::new("seal-extent");
        let database_id = database_id(18);
        let (first, second) = sealing_extent(&database_path.0, database_id);
        let generation = inspect_extent(&sealing_journal_path(&database_path.0))
            .expect("sealing extent")
            .header
            .generation;

        let sealed = seal_extent_with_media(&RealJournalMedia, &database_path.0, database_id)
            .expect("seal")
            .expect("an extent was waiting");
        assert_eq!(sealed, sealed_journal_path(&database_path.0, generation));
        assert!(!sealing_journal_path(&database_path.0).exists());
        let footer =
            read_sealed_extent_with_media(&RealJournalMedia, &sealed, database_id).expect("read");
        assert_eq!(footer.generation, generation);
        assert_eq!(footer.entries.len(), 2);
        assert_eq!(footer.entries[0].placement.position, EXTENT_DATA_OFFSET);
        assert_eq!(footer.entries[0].placement.frame_hash, first.frame_hash());
        assert_eq!(
            footer.entries[0].changelog,
            Some(sample_changelog_binding())
        );
        assert_eq!(footer.entries[1].placement.frame_hash, second.frame_hash());
        assert_eq!(footer.entries[1].covered_sequence, Some(sequence(2)));
        assert_eq!(footer.origin, (None, None, [0; HASH_BYTES]));
        let tail =
            EXTENT_DATA_OFFSET + frame_physical_bytes(&first) + frame_physical_bytes(&second);
        assert_eq!(
            usize::try_from(std::fs::metadata(&sealed).expect("metadata").len()).expect("len"),
            tail + footer.encode().expect("encode footer").len(),
            "the sealed extent keeps only its frames and footer"
        );
        assert_eq!(
            seal_extent_with_media(&RealJournalMedia, &database_path.0, database_id)
                .expect("nothing waiting"),
            None
        );
    }

    // req: REC-005
    #[test]
    fn sealing_resumes_from_every_crash_point_to_the_same_sealed_extent() {
        let reference = TestPath::new("seal-reference");
        let database_id = database_id(19);
        sealing_extent(&reference.0, database_id);
        let generation = inspect_extent(&sealing_journal_path(&reference.0))
            .expect("sealing extent")
            .header
            .generation;
        let expected = std::fs::read(
            seal_extent_with_media(&RealJournalMedia, &reference.0, database_id)
                .expect("seal")
                .expect("sealed"),
        )
        .expect("read sealed");

        for stop_after in ["anchor", "footer", "trim"] {
            let database_path = TestPath::new(&format!("seal-resume-{stop_after}"));
            sealing_extent(&database_path.0, database_id);
            let path = sealing_journal_path(&database_path.0);
            // Replay the seal's first steps by hand, then stop as a crash would.
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .expect("open");
            let state = inspect_extent_with_media(&RealJournalMedia, &path);
            assert!(state.is_ok());
            let reference_sealed = sealed_journal_path(&reference.0, generation);
            let footer =
                read_sealed_extent_with_media(&RealJournalMedia, &reference_sealed, database_id)
                    .expect("reference footer");
            let tail = footer
                .entries
                .last()
                .map(|entry| entry.placement.position + entry.placement.padded_len);
            let tail = tail.expect("frames");
            let mut header = [0_u8; EXTENT_HEADER_SLOT_BYTES];
            let reference_file = std::fs::read(&reference_sealed).expect("reference bytes");
            let anchored_slot = (0..EXTENT_HEADER_SLOT_COUNT)
                .map(|slot| {
                    ExtentHeader::decode(
                        &reference_file[slot * EXTENT_HEADER_SLOT_BYTES
                            ..(slot + 1) * EXTENT_HEADER_SLOT_BYTES],
                        u16::try_from(slot).expect("slot"),
                    )
                })
                .filter_map(Result::ok)
                .max_by_key(|header| header.revision)
                .expect("anchored header");
            header.copy_from_slice(&anchored_slot.encode());
            write_all_at(
                &mut file,
                &header,
                usize::from(anchored_slot.slot) * EXTENT_HEADER_SLOT_BYTES,
            )
            .expect("anchor");
            if stop_after != "anchor" {
                write_all_at(&mut file, &footer.encode().expect("footer"), tail).expect("footer");
            }
            if stop_after == "trim" {
                file.set_len(u64::try_from(reference_file.len()).expect("len"))
                    .expect("trim");
            }
            file.sync_all().expect("sync");
            drop(file);
            let sealed = seal_extent_with_media(&RealJournalMedia, &database_path.0, database_id)
                .expect("resume seal")
                .expect("sealed");
            assert_eq!(
                std::fs::read(&sealed).expect("read"),
                expected,
                "resuming after the {stop_after} step reaches the same sealed extent"
            );
        }
    }

    // req: REC-005
    #[test]
    fn a_proven_frame_decodes_only_under_the_hash_it_was_recorded_with() {
        let encoded = recovery_frame(database_id(34)).encode().expect("encode");
        let (full, hash) = JournalFrame::decode(encoded.as_bytes()).expect("full decode");
        assert_eq!(
            JournalFrame::decode_proven(encoded.as_bytes(), &hash).expect("proven decode"),
            full
        );
        let mut other = hash;
        other[0] ^= 1;
        assert_eq!(
            JournalFrame::decode_proven(encoded.as_bytes(), &other),
            Err(JournalCodecError::Checksum),
            "a frame is never accepted under another frame's hash"
        );
    }

    // req: REC-001, REC-005, REC-006
    #[test]
    fn physical_recovery_admission_hashes_the_same_logical_bytes_once() {
        let frame =
            recovery_frame(database_id(34)).with_changelog(Some(sample_changelog_binding()));
        let encoded = frame.encode().unwrap();
        let physical = EncodedExtentFrame::encode(1, EXTENT_DATA_OFFSET, &encoded).unwrap();
        LOGICAL_FRAME_HASH_PASSES.with(|passes| passes.set(0));
        let (decoded, hash) = decode_physical_frame(
            &physical.bytes,
            1,
            EXTENT_DATA_OFFSET,
            encoded.as_bytes().len(),
            None,
        )
        .unwrap()
        .unwrap();
        assert_eq!(decoded, frame);
        assert_eq!(hash, encoded.frame_hash());
        assert_eq!(
            LOGICAL_FRAME_HASH_PASSES.with(|passes| passes.get()),
            1,
            "admission and complete decoding share one checked immutable logical buffer"
        );
    }

    // req: REC-001, REC-005, REC-006
    #[test]
    fn physical_recovery_admission_preserves_complete_semantic_count_validation() {
        let frame =
            recovery_frame(database_id(35)).with_changelog(Some(sample_changelog_binding()));
        let mut encoded = frame.encode().unwrap();
        // A correct checksum does not prove the claimed command count: replace
        // its only commit mutation by a different, correctly framed namespace.
        std::sync::Arc::make_mut(&mut encoded.bytes)[FRAME_HEADER_BYTES] =
            JournalTable::Entities as u8;
        let footer = encoded.bytes.len() - FRAME_FOOTER_BYTES;
        let hash = digest(&encoded.bytes[..footer]);
        std::sync::Arc::make_mut(&mut encoded.bytes)[footer + 16..footer + 16 + HASH_BYTES]
            .copy_from_slice(&hash);
        encoded.frame_hash = hash;
        assert!(matches!(
            JournalFrame::decode(encoded.as_bytes()),
            Err(JournalCodecError::InvalidValue)
        ));
        let physical = EncodedExtentFrame::encode(1, EXTENT_DATA_OFFSET, &encoded).unwrap();
        assert!(matches!(
            decode_physical_frame(
                &physical.bytes,
                1,
                EXTENT_DATA_OFFSET,
                encoded.as_bytes().len(),
                None
            ),
            Err(JournalIoError::Corrupt)
        ));
    }

    // req: REC-001, REC-005, REC-006
    #[test]
    fn physical_recovery_admission_cannot_be_rebound_to_another_buffer() {
        let frame =
            recovery_frame(database_id(36)).with_changelog(Some(sample_changelog_binding()));
        let encoded = frame.encode().unwrap();
        let admitted =
            recovery_memory::admit_physical_recovery(encoded.as_bytes(), &encoded.frame_hash())
                .unwrap()
                .unwrap();
        let other = encoded.as_bytes().to_vec();
        assert_eq!(JournalFrame::decode(&other).unwrap().0, frame);
        assert!(matches!(
            JournalFrame::decode_with(&other, None, Some(admitted)),
            Err(JournalCodecError::InvalidValue)
        ));
        let mut damaged = other;
        damaged[FRAME_HEADER_BYTES + MUTATION_HEADER_BYTES] ^= 1;
        assert!(matches!(
            recovery_memory::admit_physical_recovery(&damaged, &encoded.frame_hash()),
            Err(JournalIoError::Corrupt)
        ));
    }

    // req: REC-005
    #[test]
    fn a_sealed_footer_round_trips_an_origin_that_continues_an_earlier_extent() {
        // Every extent after the first continues a real frame hash, whose
        // final bytes must not be mistaken for the reserved field after it.
        let footer = SealedExtentFooter {
            generation: 7,
            origin: (Some(sequence(9)), None, [0xa5; HASH_BYTES]),
            entries: Vec::new(),
        };
        let encoded = footer.encode().expect("encode");
        assert_eq!(
            SealedExtentFooter::decode(&encoded).expect("decode"),
            footer
        );

        let mut reserved = encoded.clone();
        reserved[72] = 1;
        let body_len = SEALED_FOOTER_HEADER_BYTES;
        let checksum = digest(&reserved[..body_len]);
        reserved[body_len..body_len + HASH_BYTES].copy_from_slice(&checksum);
        assert_eq!(
            SealedExtentFooter::decode(&reserved),
            Err(JournalIoError::Corrupt),
            "a nonzero reserved byte is refused even under a valid checksum"
        );
    }

    /// Writes `first` and a second frame starting at `second_first` that
    /// continues its hash chain, and moves the extent to the sealing name.
    fn sealing_extent_with_second_at(
        database_path: &Path,
        database_id: DatabaseId,
        second_first: u64,
    ) {
        let first = recovery_frame(database_id).encode().expect("encode first");
        write_recovery_journal(database_path, database_id, &first, None);
        let second_sequence = sequence(second_first);
        let second = JournalFrame::new(
            database_id,
            second_sequence,
            second_sequence,
            1,
            first.frame_hash(),
            vec![
                JournalMutation::put(
                    JournalTable::Commits,
                    crate::keys::encode_application_sequence_key(second_sequence).to_vec(),
                    vec![0x42],
                )
                .expect("commit mutation"),
            ],
        )
        .expect("second frame")
        .encode()
        .expect("encode second");
        write_physical_frame(&journal_path(database_path), &second, false);
        std::fs::rename(
            journal_path(database_path),
            sealing_journal_path(database_path),
        )
        .expect("move to the sealing name");
    }

    // req: REC-005
    #[test]
    fn sealing_crosses_a_direct_commit_between_frames_but_never_a_backward_step() {
        // Commit 2 was direct: frame two continues the hash chain at commit 3.
        let database_path = TestPath::new("seal-gap");
        let database_id = database_id(36);
        sealing_extent_with_second_at(&database_path.0, database_id, 3);
        let sealed = seal_extent_with_media(&RealJournalMedia, &database_path.0, database_id)
            .expect("a forward gap seals")
            .expect("an extent was waiting");
        let footer =
            read_sealed_extent_with_media(&RealJournalMedia, &sealed, database_id).expect("read");
        assert_eq!(footer.entries.len(), 2);
        assert_eq!(footer.entries[1].predecessor_sequence, Some(sequence(2)));

        // A frame that steps back behind its predecessor is never history.
        let backward = TestPath::new("seal-backward");
        sealing_extent_with_second_at(&backward.0, database_id, 1);
        assert_eq!(
            seal_extent_with_media(&RealJournalMedia, &backward.0, database_id),
            Err(JournalIoError::Corrupt)
        );
    }

    // req: REC-005
    #[test]
    fn sealing_never_overwrites_an_existing_sealed_extent() {
        let database_path = TestPath::new("seal-collision");
        let database_id = database_id(20);
        sealing_extent(&database_path.0, database_id);
        let generation = inspect_extent(&sealing_journal_path(&database_path.0))
            .expect("sealing extent")
            .header
            .generation;
        std::fs::write(
            sealed_journal_path(&database_path.0, generation),
            b"occupied",
        )
        .expect("occupy the sealed name");
        assert_eq!(
            seal_extent_with_media(&RealJournalMedia, &database_path.0, database_id),
            Err(JournalIoError::Corrupt)
        );
        assert_eq!(
            std::fs::read(sealed_journal_path(&database_path.0, generation)).expect("read"),
            b"occupied"
        );
    }

    fn frame_physical_bytes(frame: &EncodedJournalFrame) -> usize {
        extent_frame_bytes(frame.as_bytes().len()).expect("frame fits an extent")
    }

    #[test]
    fn retention_witness_requires_the_recovered_empty_extent_and_no_scratch_name() {
        let database_path = TestPath::new("retention-rebase-witness");
        let database = create_database(&database_path.0);
        let database_id = database_id(27);
        let frame = recovery_frame(database_id).encode().expect("encode frame");
        write_recovery_journal(&database_path.0, database_id, &frame, None);

        assert!(matches!(
            verify_retention_journal_rebase_with_media(
                &RealJournalMedia,
                &database_path.0,
                database_id,
                Some(sequence(1)),
                None,
            ),
            Err(JournalIoError::Corrupt)
        ));

        recover_journal(&database, &database_path.0, database_id).expect("recover and rebase");
        let witness = verify_retention_journal_rebase_with_media(
            &RealJournalMedia,
            &database_path.0,
            database_id,
            Some(sequence(1)),
            None,
        )
        .expect("empty successor extent proves preparation");
        assert!(witness._selected_generation.is_some());

        let active = journal_path(&database_path.0);
        reset_journal_after_with_media(
            &RealJournalMedia,
            &spare_journal_path(&database_path.0),
            &JournalFileHeader::with_frontiers(
                database_id,
                Some(sequence(1)),
                None,
                witness._terminal_frame_hash,
            ),
            &active,
        )
        .expect("prepare scratch extent");
        assert!(matches!(
            verify_retention_journal_rebase_with_media(
                &RealJournalMedia,
                &database_path.0,
                database_id,
                Some(sequence(1)),
                None,
            ),
            Err(JournalIoError::Corrupt)
        ));
    }

    #[test]
    fn recovery_accepts_checkpoint_before_reclamation_only_after_exact_tail_verification() {
        let database_path = TestPath::new("recovery-verify");
        let database = create_database(&database_path.0);
        let database_id = database_id(14);
        let frame = recovery_frame(database_id);
        let encoded = frame.encode().expect("encode frame");
        write_recovery_journal(&database_path.0, database_id, &encoded, None);
        replay_frames(&database, std::slice::from_ref(&frame), &[None])
            .expect("simulate checkpoint");

        recover_journal(&database, &database_path.0, database_id)
            .expect("verify checkpointed tail");

        let corrupt_path = TestPath::new("recovery-verify-corrupt");
        let corrupt_database = create_database(&corrupt_path.0);
        let mut transaction = corrupt_database
            .begin_write()
            .expect("begin corrupting write");
        transaction
            .set_durability(Durability::Immediate)
            .expect("set durability");
        for mutation in frame.mutations() {
            apply_mutation(&transaction, mutation).expect("apply original mutation");
        }
        transaction
            .open_table(ENTITIES)
            .expect("open entities")
            .insert([0x10].as_slice(), [0x21].as_slice())
            .expect("replace entity");
        transaction.commit().expect("commit replacement");
        write_recovery_journal(&corrupt_path.0, database_id, &encoded, None);
        assert_eq!(
            recover_journal(&corrupt_database, &corrupt_path.0, database_id),
            Err(JournalIoError::Corrupt)
        );
    }

    #[test]
    fn recovery_reanchors_an_empty_extent_behind_the_redb_checkpoint() {
        let database_path = TestPath::new("recovery-empty-reanchor");
        let database = create_database(&database_path.0);
        let database_id = database_id(26);
        let frame = recovery_frame(database_id);
        replay_frames(&database, std::slice::from_ref(&frame), &[None])
            .expect("advance redb checkpoint");
        let path = journal_path(&database_path.0);
        initialize_or_validate_file(
            &path,
            &JournalFileHeader::new(database_id, None, [0; HASH_BYTES]),
        )
        .expect("create stale empty extent");

        recover_journal(&database, &database_path.0, database_id)
            .expect("reanchor stale empty extent");
        let (header, tail) = scan_journal(&path, database_id, |_| Ok(()))
            .expect("scan reanchored extent")
            .expect("extent exists");
        assert_eq!(header.checkpoint_sequence(), Some(sequence(1)));
        assert_eq!(tail.last_sequence, Some(sequence(1)));
        assert_eq!(tail.transition_count, 0);
    }

    #[test]
    fn recovery_discards_only_a_torn_terminal_tail() {
        let database_path = TestPath::new("recovery-torn");
        let database = create_database(&database_path.0);
        let database_id = database_id(15);
        let frame = recovery_frame(database_id).encode().expect("encode frame");
        let torn = recovery_frame(database_id)
            .encode()
            .expect("encode torn frame");
        write_recovery_journal(&database_path.0, database_id, &frame, Some(&torn));

        recover_journal(&database, &database_path.0, database_id).expect("recover complete prefix");
        let (_, tail) = scan_journal(&journal_path(&database_path.0), database_id, |_| Ok(()))
            .expect("scan anchored journal")
            .expect("journal exists");
        assert_eq!(tail.command_count, 0);
        assert!(
            !tail.incomplete_tail,
            "the torn frame is fenced before the anchor names it"
        );
        assert_eq!(
            inspect_extent(&journal_path(&database_path.0))
                .expect("anchored extent")
                .header
                .anchor_position,
            EXTENT_DATA_OFFSET + frame_physical_bytes(&frame)
        );
    }
}
