//! Versioned local durability-frame encoding for the standard application profile.
#![allow(
    dead_code,
    reason = "WP-478 lands the closed frame and lane boundary before production coordinator wiring"
)]

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
#[cfg(not(unix))]
use std::io::{Seek, SeekFrom};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;

use sha2::{Digest, Sha256};

use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};

use riffdb_types::{AdministrationSequence, CommitSequence, DatabaseId};

use crate::keys::{decode_application_sequence_key, decode_audit_key};
use crate::layout::{
    AUDIT, AUDIT_BY_REQUEST, COMMITS, ENTITIES, EVENT_ROUTES, EVENTS, IDEMPOTENCY,
    IDEMPOTENCY_PENDING, INDEX_EPOCHS, META, PROVENANCE, SECONDARY_INDEXES,
};

const FILE_MAGIC: [u8; 8] = *b"RDBJRN01";
const FRAME_MAGIC: [u8; 8] = *b"RDBFRM01";
const FOOTER_MAGIC: [u8; 8] = *b"RDBEND01";
const FORMAT_VERSION: u16 = 2;
const EXTENT_MAGIC: [u8; 8] = *b"RDBJEX03";
const EXTENT_FRAME_MAGIC: [u8; 8] = *b"RDBJPF03";
const EXTENT_FRAME_FOOTER_MAGIC: [u8; 8] = *b"RDBJPE03";
const EXTENT_FORMAT_VERSION: u16 = 3;
const EXTENT_HEADER_SLOT_BYTES: usize = 4 * 1024;
const EXTENT_HEADER_SLOT_COUNT: usize = 2;
const EXTENT_DATA_OFFSET: usize = EXTENT_HEADER_SLOT_BYTES * EXTENT_HEADER_SLOT_COUNT;
pub(crate) const EXTENT_DATA_BYTES: usize = 40 * 1024 * 1024;
const EXTENT_FILE_BYTES: usize = EXTENT_DATA_OFFSET + EXTENT_DATA_BYTES;
const EXTENT_FRAME_ALIGNMENT: usize = 4 * 1024;
const EXTENT_FRAME_HEADER_BYTES: usize = 128;
const EXTENT_FRAME_FOOTER_BYTES: usize = 64;
const EXTENT_HEADER_CHECKSUM_OFFSET: usize = EXTENT_HEADER_SLOT_BYTES - HASH_BYTES;
const HASH_BYTES: usize = 32;
const FILE_HEADER_BYTES: usize = 8 + 2 + 16 + 8 + 8 + HASH_BYTES + HASH_BYTES;
const FRAME_HEADER_BYTES: usize =
    8 + 2 + 16 + 1 + 1 + 8 + 8 + 8 + 8 + 2 + 2 + 2 + 2 + 4 + 4 + HASH_BYTES;
const FRAME_FOOTER_BYTES: usize = 8 + 8 + HASH_BYTES;
const MUTATION_HEADER_BYTES: usize = 1 + 1 + 1 + 1 + 4 + 4 + HASH_BYTES;

pub(crate) const MAX_JOURNAL_TRANSITIONS: usize = 256;
pub(crate) const MAX_JOURNAL_COMMANDS: usize = MAX_JOURNAL_TRANSITIONS;
/// Maximum complete published-plus-unpublished recovery suffix retained before
/// redb checkpoint reclamation. The tighter 256-transition limit above still
/// governs one frame flush and all unpublished work; this independent cap
/// prevents already-published small frames from forcing byte-inefficient redb
/// checkpoints while keeping restart CPU bounded.
pub(crate) const MAX_JOURNAL_SUFFIX_TRANSITIONS: usize = 8_192;
pub(crate) const MAX_JOURNAL_FRAME_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_JOURNAL_SUFFIX_BYTES: usize = 32 * 1024 * 1024;
const DELETE_VALUE_LENGTH: u32 = u32::MAX;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
}

impl JournalTable {
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
            _ => Err(JournalCodecError::UnknownTable),
        }
    }

    pub(crate) const fn composite(self) -> riffdb_storage_api::CompositeTableV1 {
        match self {
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
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum JournalFrameKind {
    Command = 1,
    ServiceAudit = 2,
}

impl JournalFrameKind {
    fn decode(value: u8) -> Result<Self, JournalCodecError> {
        match value {
            1 => Ok(Self::Command),
            2 => Ok(Self::ServiceAudit),
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

    fn table(&self) -> JournalTable {
        match self {
            Self::Put { table, .. } | Self::Delete { table, .. } => *table,
        }
    }

    fn key(&self) -> &[u8] {
        match self {
            Self::Put { key, .. } | Self::Delete { key, .. } => key,
        }
    }

    fn value(&self) -> Option<&[u8]> {
        match self {
            Self::Put { value, .. } => Some(value),
            Self::Delete { .. } => None,
        }
    }

    fn expected_hash(&self) -> Option<[u8; HASH_BYTES]> {
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
        }
    }
}

impl JournalMutationBuffer {
    pub(crate) const fn mutation_count(&self) -> u32 {
        self.mutation_count
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
    ) -> Result<(), JournalCodecError> {
        let command_count = u16::try_from(command_count)
            .ok()
            .filter(|count| *count != 0)
            .ok_or(JournalCodecError::LimitExceeded)?;
        let audit_count = command_count
            .checked_mul(2)
            .ok_or(JournalCodecError::LimitExceeded)?;
        let mutation = JournalMutation::put(JournalTable::Commits, key, value)?;
        self.push_with_command_counts(&mutation, Some((command_count, audit_count)))
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
        let frame_hash = digest(&self.bytes);
        self.bytes.extend_from_slice(&FOOTER_MAGIC);
        self.bytes.extend_from_slice(
            &u64::try_from(total_len)
                .map_err(|_| JournalCodecError::LimitExceeded)?
                .to_be_bytes(),
        );
        self.bytes.extend_from_slice(&frame_hash);
        Ok(EncodedJournalFrame {
            bytes: Arc::from(self.bytes),
            frame_hash,
            transition_count,
            command_count,
            audit_count,
            covered_sequence,
            covered_administration_sequence,
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

fn service_audit_mutation_is_closed(mutation: &JournalMutation) -> bool {
    match mutation.table() {
        JournalTable::Audit | JournalTable::AuditByRequest => mutation.value().is_some(),
        JournalTable::Meta => {
            mutation.value().is_some()
                && mutation.key() == crate::layout::META_ADMINISTRATION_SEQUENCE.as_bytes()
        }
        JournalTable::Entities
        | JournalTable::SecondaryIndexes
        | JournalTable::IndexEpochs
        | JournalTable::Idempotency
        | JournalTable::IdempotencyPending
        | JournalTable::Events
        | JournalTable::EventRoutes
        | JournalTable::Outbox
        | JournalTable::Provenance
        | JournalTable::Commits => false,
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
        if bytes[..8] != FILE_MAGIC || read_u16(bytes, 8)? != FORMAT_VERSION {
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
}

impl ExtentHeader {
    fn initial(logical: JournalFileHeader) -> Self {
        Self {
            logical,
            generation: 1,
            slot: 0,
        }
    }

    fn successor(&self, logical: JournalFileHeader) -> Result<Self, JournalIoError> {
        Ok(Self {
            logical,
            generation: self
                .generation
                .checked_add(1)
                .ok_or(JournalIoError::Corrupt)?,
            slot: if self.slot == 0 { 1 } else { 0 },
        })
    }

    fn encode(&self) -> [u8; EXTENT_HEADER_SLOT_BYTES] {
        let mut bytes = [0_u8; EXTENT_HEADER_SLOT_BYTES];
        bytes[..8].copy_from_slice(&EXTENT_MAGIC);
        bytes[8..10].copy_from_slice(&EXTENT_FORMAT_VERSION.to_be_bytes());
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
            &u64::try_from(EXTENT_DATA_BYTES)
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
        if bytes[..8] != EXTENT_MAGIC
            || read_u16(bytes, 8)? != EXTENT_FORMAT_VERSION
            || read_u16(bytes, 10)? != expected_slot
            || usize::try_from(read_u32(bytes, 12)?).ok() != Some(EXTENT_HEADER_SLOT_BYTES)
            || usize::try_from(read_u64(bytes, 88)?).ok() != Some(EXTENT_DATA_OFFSET)
            || usize::try_from(read_u64(bytes, 96)?).ok() != Some(EXTENT_DATA_BYTES)
        {
            return Err(JournalCodecError::UnknownVersion);
        }
        if digest(&bytes[..EXTENT_HEADER_CHECKSUM_OFFSET]).as_slice()
            != &bytes[EXTENT_HEADER_CHECKSUM_OFFSET..]
        {
            return Err(JournalCodecError::Checksum);
        }
        if bytes[104..EXTENT_HEADER_CHECKSUM_OFFSET]
            .iter()
            .any(|byte| *byte != 0)
        {
            return Err(JournalCodecError::InvalidValue);
        }
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
}

impl JournalFrame {
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
            || logical_command_put_count(&mutations)? != command_count
            || match kind {
                JournalFrameKind::Command => {
                    let physical_audits = mutation_put_count(&mutations, JournalTable::Audit)?;
                    let command_audits = logical_command_audit_count(&mutations)?;
                    transition_count != command_count
                        || command_count == 0
                        || (physical_audits != audit_count && command_audits != audit_count)
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
        };
        if frame.encoded_len()? > MAX_JOURNAL_FRAME_BYTES {
            return Err(JournalCodecError::LimitExceeded);
        }
        Ok(frame)
    }

    pub(crate) fn encode(&self) -> Result<EncodedJournalFrame, JournalCodecError> {
        let payload_len = self.payload_len()?;
        let total_len = FRAME_HEADER_BYTES
            .checked_add(payload_len)
            .and_then(|value| value.checked_add(FRAME_FOOTER_BYTES))
            .ok_or(JournalCodecError::LimitExceeded)?;
        if total_len > MAX_JOURNAL_FRAME_BYTES {
            return Err(JournalCodecError::LimitExceeded);
        }
        let mut bytes = Vec::with_capacity(total_len);
        bytes.extend_from_slice(&FRAME_MAGIC);
        bytes.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
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
            bytes: Arc::from(bytes),
            frame_hash,
            transition_count: self.transition_count,
            command_count: self.command_count,
            audit_count: self.audit_count,
            covered_sequence: self.covered_sequence,
            covered_administration_sequence: self.covered_administration_sequence,
        })
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<(Self, [u8; HASH_BYTES]), JournalCodecError> {
        if bytes.len() < FRAME_HEADER_BYTES + FRAME_FOOTER_BYTES {
            return Err(JournalCodecError::Truncated);
        }
        if bytes[..8] != FRAME_MAGIC || read_u16(bytes, 8)? != FORMAT_VERSION {
            return Err(JournalCodecError::UnknownVersion);
        }
        if bytes.get(27) != Some(&0) || read_u16(bytes, 66)? != 0 {
            return Err(JournalCodecError::InvalidValue);
        }
        let mutation_count =
            usize::try_from(read_u32(bytes, 68)?).map_err(|_| JournalCodecError::LimitExceeded)?;
        let payload_len =
            usize::try_from(read_u32(bytes, 72)?).map_err(|_| JournalCodecError::LimitExceeded)?;
        let expected_len = FRAME_HEADER_BYTES
            .checked_add(payload_len)
            .and_then(|value| value.checked_add(FRAME_FOOTER_BYTES))
            .ok_or(JournalCodecError::LimitExceeded)?;
        if expected_len > MAX_JOURNAL_FRAME_BYTES {
            return Err(JournalCodecError::LimitExceeded);
        }
        if bytes.len() != expected_len {
            return Err(JournalCodecError::Truncated);
        }
        let footer = FRAME_HEADER_BYTES + payload_len;
        if bytes[footer..footer + 8] != FOOTER_MAGIC
            || usize::try_from(read_u64(bytes, footer + 8)?)
                .map_err(|_| JournalCodecError::LimitExceeded)?
                != expected_len
        {
            return Err(JournalCodecError::InvalidValue);
        }
        let frame_hash = digest(&bytes[..footer]);
        if frame_hash != read_array::<HASH_BYTES>(bytes, footer + 16)? {
            return Err(JournalCodecError::Checksum);
        }
        let mut cursor = FRAME_HEADER_BYTES;
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
        let database_id = DatabaseId::from_bytes(read_array::<16>(bytes, 10)?)
            .map_err(|_| JournalCodecError::InvalidValue)?;
        let frame = Self::checked(
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
        )?;
        Ok((frame, frame_hash))
    }

    fn payload_len(&self) -> Result<usize, JournalCodecError> {
        self.mutations.iter().try_fold(0_usize, |total, mutation| {
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
    bytes: Arc<[u8]>,
    frame_hash: [u8; HASH_BYTES],
    transition_count: u16,
    command_count: u16,
    audit_count: u16,
    covered_sequence: Option<CommitSequence>,
    covered_administration_sequence: Option<AdministrationSequence>,
}

impl EncodedJournalFrame {
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn frame_hash(&self) -> [u8; HASH_BYTES] {
        self.frame_hash
    }

    fn transition_count(&self) -> u16 {
        self.transition_count
    }

    fn command_count(&self) -> u16 {
        self.command_count
    }

    pub(crate) fn audit_count(&self) -> u16 {
        self.audit_count
    }

    fn covered_sequence(&self) -> Option<CommitSequence> {
        self.covered_sequence
    }

    fn covered_administration_sequence(&self) -> Option<AdministrationSequence> {
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
        bytes[8..10].copy_from_slice(&EXTENT_FORMAT_VERSION.to_be_bytes());
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
    completion: mpsc::Sender<Result<JournalFence, JournalIoError>>,
}

pub(crate) struct JournalFenceReceipt {
    completion: mpsc::Receiver<Result<JournalFence, JournalIoError>>,
}

impl JournalFenceReceipt {
    pub(crate) fn try_wait(&self) -> Result<Option<JournalFence>, JournalIoError> {
        match self.completion.try_recv() {
            Ok(result) => result.map(Some),
            Err(mpsc::TryRecvError::Empty) => Ok(None),
            Err(mpsc::TryRecvError::Disconnected) => Err(JournalIoError::Stopped),
        }
    }

    pub(crate) fn wait(self) -> Result<JournalFence, JournalIoError> {
        self.completion
            .recv()
            .unwrap_or(Err(JournalIoError::Stopped))
    }
}

pub(crate) struct JournalLane {
    sender: Option<mpsc::SyncSender<JournalSubmission>>,
    worker: Option<thread::JoinHandle<()>>,
    durable_flushes: Arc<AtomicU64>,
}

impl JournalLane {
    pub(crate) fn open(path: &Path, header: &JournalFileHeader) -> Result<Self, JournalIoError> {
        let state = initialize_or_validate_file(path, header)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|_| JournalIoError::Io)?;
        let (sender, receiver) = mpsc::sync_channel(MAX_JOURNAL_COMMANDS);
        let durable_flushes = Arc::new(AtomicU64::new(0));
        let worker_flushes = Arc::clone(&durable_flushes);
        let worker = thread::Builder::new()
            .name("riffdb-journal".to_string())
            .spawn(move || journal_worker(file, receiver, &worker_flushes, state))
            .map_err(|_| JournalIoError::Io)?;
        Ok(Self {
            sender: Some(sender),
            worker: Some(worker),
            durable_flushes,
        })
    }

    pub(crate) fn submit(
        &self,
        frame: EncodedJournalFrame,
    ) -> Result<JournalFenceReceipt, JournalIoError> {
        let (completion, receiver) = mpsc::channel();
        self.sender
            .as_ref()
            .ok_or(JournalIoError::Stopped)?
            .send(JournalSubmission { frame, completion })
            .map_err(|_| JournalIoError::Stopped)?;
        Ok(JournalFenceReceipt {
            completion: receiver,
        })
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

fn initialize_or_validate_file(
    path: &Path,
    expected: &JournalFileHeader,
) -> Result<ExtentState, JournalIoError> {
    match File::open(path) {
        Ok(mut file) => {
            let mut magic = [0_u8; 8];
            file.read_exact(&mut magic)
                .map_err(|_| JournalIoError::Corrupt)?;
            if magic == FILE_MAGIC {
                let (_, tail) = scan_legacy_journal(path, expected.database_id(), |_| Ok(()))?
                    .ok_or(JournalIoError::Corrupt)?;
                if tail.transition_count != 0 || tail.incomplete_tail {
                    return Err(JournalIoError::LegacyNonEmpty(path.to_path_buf()));
                }
                migrate_empty_legacy_journal(path, expected)?;
            } else if magic != EXTENT_MAGIC {
                return Err(JournalIoError::Corrupt);
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            create_extent_file(path, &ExtentHeader::initial(expected.clone()))?;
        }
        Err(_) => return Err(JournalIoError::Io),
    }
    let state = inspect_extent(path)?;
    if state.header.logical != *expected {
        return Err(JournalIoError::Corrupt);
    }
    Ok(state)
}

fn journal_worker(
    mut file: File,
    receiver: mpsc::Receiver<JournalSubmission>,
    durable_flushes: &AtomicU64,
    state: ExtentState,
) {
    let generation = state.header.generation;
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
        let write = if let Some(error) = encode_error {
            Err(error)
        } else {
            physical
                .iter()
                .try_for_each(|frame| {
                    write_all_at(&mut file, &frame.bytes, frame.position)
                        .map_err(|_| JournalIoError::Io)
                })
                .and_then(|()| file.sync_data().map_err(|_| JournalIoError::Io))
        };
        if write.is_ok() {
            durable_flushes.fetch_add(1, Ordering::Relaxed);
        }
        for submission in batch {
            let result = if write.is_ok() {
                Ok(JournalFence {
                    covered_sequence: submission.frame.covered_sequence(),
                    covered_administration_sequence: submission
                        .frame
                        .covered_administration_sequence(),
                    frame_hash: submission.frame.frame_hash(),
                })
            } else {
                Err(write.clone().expect_err("failed write carries an error"))
            };
            let _ = submission.completion.send(result);
        }
        if let Err(error) = write {
            for submission in receiver.try_iter() {
                let _ = submission.completion.send(Err(error.clone()));
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

pub(crate) fn sync_parent_directory(path: &Path) -> Result<(), JournalIoError> {
    let parent = path.parent().ok_or(JournalIoError::Io)?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| JournalIoError::Io)
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

fn create_extent_file(path: &Path, header: &ExtentHeader) -> Result<(), JournalIoError> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|_| JournalIoError::Io)?;
    let zeroes = vec![0_u8; 1024 * 1024];
    let mut remaining = EXTENT_FILE_BYTES;
    while remaining != 0 {
        let count = remaining.min(zeroes.len());
        if let Err(error) = file.write_all(&zeroes[..count]) {
            drop(file);
            let _ = std::fs::remove_file(path);
            return Err(classify_extent_creation_error(&error));
        }
        remaining -= count;
    }
    if let Err(error) = write_all_at(&mut file, &header.encode(), 0).and_then(|()| file.sync_data())
    {
        drop(file);
        let _ = std::fs::remove_file(path);
        return Err(classify_extent_creation_error(&error));
    }
    sync_parent(path)
}

fn classify_extent_creation_error(error: &io::Error) -> JournalIoError {
    if error.kind() == io::ErrorKind::StorageFull {
        JournalIoError::Capacity
    } else {
        JournalIoError::Io
    }
}

fn migrate_empty_legacy_journal(
    path: &Path,
    expected: &JournalFileHeader,
) -> Result<(), JournalIoError> {
    let parent = path.parent().ok_or(JournalIoError::Io)?;
    let file_name = path.file_name().ok_or(JournalIoError::Io)?;
    let mut replacement_name = file_name.to_os_string();
    replacement_name.push(".extent-v3");
    let replacement = parent.join(replacement_name);
    match std::fs::remove_file(&replacement) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(JournalIoError::Io),
    }
    create_extent_file(&replacement, &ExtentHeader::initial(expected.clone()))?;
    std::fs::rename(&replacement, path).map_err(|_| JournalIoError::Io)?;
    sync_parent(path)
}

fn sync_parent(path: &Path) -> Result<(), JournalIoError> {
    let parent = path.parent().ok_or(JournalIoError::Io)?;
    File::open(parent)
        .and_then(|directory| directory.sync_data())
        .map_err(|_| JournalIoError::Io)
}

fn read_selected_extent_header(file: &mut File) -> Result<ExtentHeader, JournalIoError> {
    let actual_len = usize::try_from(file.metadata().map_err(|_| JournalIoError::Io)?.len())
        .map_err(|_| JournalIoError::Corrupt)?;
    if actual_len != EXTENT_FILE_BYTES {
        return Err(JournalIoError::Corrupt);
    }
    let mut valid = Vec::with_capacity(EXTENT_HEADER_SLOT_COUNT);
    for slot in 0..EXTENT_HEADER_SLOT_COUNT {
        let mut bytes = [0_u8; EXTENT_HEADER_SLOT_BYTES];
        read_exact_at(file, &mut bytes, slot * EXTENT_HEADER_SLOT_BYTES)
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
    valid.sort_by_key(|header| header.generation);
    let selected = valid.pop().ok_or(JournalIoError::Corrupt)?;
    if valid
        .last()
        .is_some_and(|header| header.generation == selected.generation && header != &selected)
    {
        return Err(JournalIoError::Corrupt);
    }
    Ok(selected)
}

fn inspect_extent(path: &Path) -> Result<ExtentState, JournalIoError> {
    let (_, _, state) = scan_extent(path, None, |_| Ok(()))?;
    Ok(state)
}

fn scan_extent_journal(
    path: &Path,
    expected_database: DatabaseId,
    visit: impl FnMut(&JournalFrame) -> Result<(), JournalIoError>,
) -> Result<(JournalFileHeader, JournalScanTail), JournalIoError> {
    let (header, tail, _) = scan_extent(path, Some(expected_database), visit)?;
    Ok((header, tail))
}

fn scan_extent(
    path: &Path,
    expected_database: Option<DatabaseId>,
    mut visit: impl FnMut(&JournalFrame) -> Result<(), JournalIoError>,
) -> Result<(JournalFileHeader, JournalScanTail, ExtentState), JournalIoError> {
    let mut file = OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(|_| JournalIoError::Io)?;
    let extent = read_selected_extent_header(&mut file)?;
    if expected_database.is_some_and(|expected| extent.logical.database_id() != expected) {
        return Err(JournalIoError::Corrupt);
    }
    let mut previous_hash = extent.logical.checkpoint_frame_hash();
    let mut previous_sequence = extent.logical.checkpoint_sequence();
    let mut previous_administration_sequence = extent.logical.checkpoint_administration_sequence();
    let mut position = EXTENT_DATA_OFFSET;
    let mut transition_count = 0_usize;
    let mut command_count = 0_usize;
    let mut audit_count = 0_usize;
    let mut encoded_byte_count = 0_usize;
    let mut incomplete_tail = false;
    while position < EXTENT_FILE_BYTES {
        let mut physical_header = [0_u8; EXTENT_FRAME_HEADER_BYTES];
        read_exact_at(&mut file, &mut physical_header, position).map_err(|_| JournalIoError::Io)?;
        if physical_header.iter().all(|byte| *byte == 0) {
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
                != EXTENT_FORMAT_VERSION
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
                .filter(|end| *end <= EXTENT_FILE_BYTES)
                .is_none()
        {
            return Err(JournalIoError::Corrupt);
        }
        let mut physical = vec![0_u8; padded_len];
        read_exact_at(&mut file, &mut physical, position).map_err(|_| JournalIoError::Io)?;
        let footer = padded_len - EXTENT_FRAME_FOOTER_BYTES;
        let footer_is_current = physical[footer..footer + 8] == EXTENT_FRAME_FOOTER_MAGIC
            && read_u64(&physical, footer + 8).ok() == Some(extent.generation)
            && usize::try_from(read_u64(&physical, footer + 16).unwrap_or(0)).ok()
                == Some(position)
            && physical[footer + 24..footer + 56] == physical_header[36..68]
            && physical[footer + 56..footer + 64]
                .iter()
                .all(|byte| *byte == 0);
        if !footer_is_current {
            incomplete_tail = true;
            break;
        }
        if physical[100..EXTENT_FRAME_HEADER_BYTES]
            .iter()
            .any(|byte| *byte != 0)
            || physical[EXTENT_FRAME_HEADER_BYTES + encoded_len..footer]
                .iter()
                .any(|byte| *byte != 0)
            || extent_frame_checksum(&physical).as_slice() != &physical[68..100]
        {
            return Err(JournalIoError::Corrupt);
        }
        let logical = &physical[EXTENT_FRAME_HEADER_BYTES..EXTENT_FRAME_HEADER_BYTES + encoded_len];
        let (frame, frame_hash) =
            JournalFrame::decode(logical).map_err(|_| JournalIoError::Corrupt)?;
        if frame_hash.as_slice() != &physical_header[36..68]
            || frame.database_id() != extent.logical.database_id()
            || frame.previous_hash() != previous_hash
            || frame.predecessor_sequence() != previous_sequence
            || frame.predecessor_administration_sequence() != previous_administration_sequence
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
        if transition_count > MAX_JOURNAL_SUFFIX_TRANSITIONS
            || encoded_byte_count > MAX_JOURNAL_SUFFIX_BYTES
        {
            return Err(JournalIoError::Corrupt);
        }
        visit(&frame)?;
        previous_sequence = frame.covered_sequence();
        previous_administration_sequence = frame.covered_administration_sequence();
        previous_hash = frame_hash;
        position = position
            .checked_add(padded_len)
            .ok_or(JournalIoError::Corrupt)?;
    }
    let logical = extent.logical.clone();
    let tail = JournalScanTail {
        last_sequence: previous_sequence,
        last_administration_sequence: previous_administration_sequence,
        last_hash: previous_hash,
        incomplete_tail,
        complete_bytes: position,
        transition_count,
        command_count,
        audit_count,
    };
    Ok((
        logical,
        tail,
        ExtentState {
            header: extent,
            next_position: position,
        },
    ))
}

pub(crate) fn scan_journal(
    path: &Path,
    expected_database: DatabaseId,
    visit: impl FnMut(&JournalFrame) -> Result<(), JournalIoError>,
) -> Result<Option<(JournalFileHeader, JournalScanTail)>, JournalIoError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(JournalIoError::Io),
    };
    let mut magic = [0_u8; 8];
    file.read_exact(&mut magic)
        .map_err(|_| JournalIoError::Corrupt)?;
    if magic == EXTENT_MAGIC {
        return scan_extent_journal(path, expected_database, visit).map(Some);
    }
    if magic != FILE_MAGIC {
        return Err(JournalIoError::Corrupt);
    }
    let result =
        scan_legacy_journal(path, expected_database, visit)?.ok_or(JournalIoError::Corrupt)?;
    if result.1.transition_count != 0 || result.1.incomplete_tail {
        return Err(JournalIoError::LegacyNonEmpty(path.to_path_buf()));
    }
    Ok(Some(result))
}

fn scan_legacy_journal(
    path: &Path,
    expected_database: DatabaseId,
    mut visit: impl FnMut(&JournalFrame) -> Result<(), JournalIoError>,
) -> Result<Option<(JournalFileHeader, JournalScanTail)>, JournalIoError> {
    let mut file = match File::open(path) {
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
        let mut frame_header = [0_u8; FRAME_HEADER_BYTES];
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
        if read != FRAME_HEADER_BYTES {
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
        let total = FRAME_HEADER_BYTES
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
pub(crate) fn recover_journal(
    database: &Database,
    database_path: &Path,
    database_id: DatabaseId,
) -> Result<(), JournalIoError> {
    recover_journal_path(database, &journal_path(database_path), database_id)
}

pub(crate) fn recover_journal_path(
    database: &Database,
    path: &Path,
    database_id: DatabaseId,
) -> Result<(), JournalIoError> {
    let mut frames = Vec::new();
    let Some((header, tail)) = scan_journal(path, database_id, |frame| {
        frames.push(frame.clone());
        Ok(())
    })?
    else {
        return Ok(());
    };
    let redb_frontier = read_redb_frontier(database)?;
    let checkpoint_frontier = (
        header.checkpoint_sequence(),
        header.checkpoint_administration_sequence(),
    );
    let tail_frontier = (tail.last_sequence, tail.last_administration_sequence);
    if frames.is_empty()
        && !tail.incomplete_tail
        && tail_frontier == checkpoint_frontier
        && checkpoint_frontier.0 <= redb_frontier.0
        && checkpoint_frontier.1 <= redb_frontier.1
        && checkpoint_frontier != redb_frontier
    {
        return reset_journal(
            path,
            &JournalFileHeader::with_frontiers(
                database_id,
                redb_frontier.0,
                redb_frontier.1,
                tail.last_hash,
            ),
        );
    }
    if redb_frontier == checkpoint_frontier {
        if !frames.is_empty() {
            replay_frames(database, &frames)?;
        }
    } else if redb_frontier == tail_frontier && !frames.is_empty() {
        verify_replayed_tail(database, &frames)?;
    } else {
        return Err(JournalIoError::Corrupt);
    }
    reset_journal(
        path,
        &JournalFileHeader::with_frontiers(
            database_id,
            tail.last_sequence,
            tail.last_administration_sequence,
            tail.last_hash,
        ),
    )
}

fn read_redb_frontier(
    database: &Database,
) -> Result<(Option<CommitSequence>, Option<AdministrationSequence>), JournalIoError> {
    let transaction = database.begin_read().map_err(|_| JournalIoError::Io)?;
    let commit = transaction
        .open_table(COMMITS)
        .map_err(|_| JournalIoError::Corrupt)?;
    let (commit_sequence, command_audit_sequence) =
        match commit.last().map_err(|_| JournalIoError::Corrupt)? {
            None => (None, None),
            Some((key, value)) => {
                let physical = decode_application_sequence_key(key.value())
                    .map_err(|_| JournalIoError::Corrupt)?;
                match riffdb_storage_api::decode_command_segment_v1(value.value()) {
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

fn replay_frames(database: &Database, frames: &[JournalFrame]) -> Result<(), JournalIoError> {
    let mut transaction = database.begin_write().map_err(|_| JournalIoError::Io)?;
    transaction.set_two_phase_commit(true);
    transaction
        .set_durability(Durability::Immediate)
        .map_err(|_| JournalIoError::Io)?;
    for frame in frames {
        for mutation in frame.mutations() {
            apply_mutation(&transaction, mutation)?;
        }
    }
    transaction.commit().map_err(|_| JournalIoError::Io)
}

fn verify_replayed_tail(
    database: &Database,
    frames: &[JournalFrame],
) -> Result<(), JournalIoError> {
    let mut final_values = BTreeMap::<(u8, Vec<u8>), Option<Vec<u8>>>::new();
    for mutation in frames.iter().flat_map(JournalFrame::mutations) {
        final_values.insert(
            (mutation.table() as u8, mutation.key().to_vec()),
            mutation.value().map(<[u8]>::to_vec),
        );
    }
    let transaction = database.begin_read().map_err(|_| JournalIoError::Io)?;
    for ((table, key), expected) in final_values {
        let table = JournalTable::decode(table).map_err(|_| JournalIoError::Corrupt)?;
        let actual = read_value(&transaction, table, &key)?;
        if actual != expected {
            return Err(JournalIoError::Corrupt);
        }
    }
    Ok(())
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
        JournalTable::IdempotencyPending => {
            apply_byte_mutation(transaction, IDEMPOTENCY_PENDING, mutation)
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
    }
}

fn apply_meta_mutation(
    transaction: &redb::WriteTransaction,
    mutation: &JournalMutation,
) -> Result<(), JournalIoError> {
    let key = std::str::from_utf8(mutation.key()).map_err(|_| JournalIoError::Corrupt)?;
    if !matches!(
        key,
        crate::layout::META_APPLICATION_SEQUENCE | crate::layout::META_ADMINISTRATION_SEQUENCE
    ) {
        return Err(JournalIoError::Corrupt);
    }
    let mut table = transaction
        .open_table(META)
        .map_err(|_| JournalIoError::Corrupt)?;
    let current = table
        .get(key)
        .map_err(|_| JournalIoError::Corrupt)?
        .map(|value| value.value().to_vec());
    validate_before_image(current.as_deref(), mutation)?;
    match mutation {
        JournalMutation::Put { value, .. } => {
            table
                .insert(key, value.as_ref())
                .map_err(|_| JournalIoError::Corrupt)?;
        }
        JournalMutation::Delete { .. } => {
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
    let mut table = transaction
        .open_table(definition)
        .map_err(|_| JournalIoError::Corrupt)?;
    let current = table
        .get(mutation.key())
        .map_err(|_| JournalIoError::Corrupt)?
        .map(|value| value.value().to_vec());
    validate_before_image(current.as_deref(), mutation)?;
    match mutation {
        JournalMutation::Put { value, .. } => {
            table
                .insert(mutation.key(), value.as_ref())
                .map_err(|_| JournalIoError::Corrupt)?;
        }
        JournalMutation::Delete { .. } => {
            table
                .remove(mutation.key())
                .map_err(|_| JournalIoError::Corrupt)?;
        }
    }
    Ok(())
}

fn validate_before_image(
    current: Option<&[u8]>,
    mutation: &JournalMutation,
) -> Result<(), JournalIoError> {
    match (current, mutation.expected_hash()) {
        (None, None) => Ok(()),
        (Some(value), Some(expected)) if digest(value) == expected => Ok(()),
        _ => Err(JournalIoError::Corrupt),
    }
}

pub(crate) fn read_value(
    transaction: &redb::ReadTransaction,
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
    };
    transaction
        .open_table(definition)
        .map_err(|_| JournalIoError::Corrupt)?
        .get(key)
        .map_err(|_| JournalIoError::Corrupt)
        .map(|value| value.map(|value| value.value().to_vec()))
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
    }
}

pub(crate) fn reset_journal(path: &Path, header: &JournalFileHeader) -> Result<(), JournalIoError> {
    let mut file = match OpenOptions::new().read(true).write(true).open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return create_extent_file(path, &ExtentHeader::initial(header.clone()));
        }
        Err(_) => return Err(JournalIoError::Io),
    };
    let mut magic = [0_u8; 8];
    file.read_exact(&mut magic)
        .map_err(|_| JournalIoError::Corrupt)?;
    if magic == FILE_MAGIC {
        let (_, tail) = scan_legacy_journal(path, header.database_id(), |_| Ok(()))?
            .ok_or(JournalIoError::Corrupt)?;
        if tail.transition_count != 0 || tail.incomplete_tail {
            return Err(JournalIoError::LegacyNonEmpty(path.to_path_buf()));
        }
        drop(file);
        return migrate_empty_legacy_journal(path, header);
    }
    if magic != EXTENT_MAGIC {
        return Err(JournalIoError::Corrupt);
    }
    let current = read_selected_extent_header(&mut file)?;
    if current.logical.database_id() != header.database_id() {
        return Err(JournalIoError::Corrupt);
    }
    let successor = current.successor(header.clone())?;
    write_all_at(
        &mut file,
        &successor.encode(),
        usize::from(successor.slot) * EXTENT_HEADER_SLOT_BYTES,
    )
    .and_then(|()| file.sync_data())
    .map_err(|_| JournalIoError::Io)
}

#[cfg(unix)]
fn write_all_at(file: &mut File, mut bytes: &[u8], mut offset: usize) -> std::io::Result<()> {
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
fn write_all_at(file: &mut File, bytes: &[u8], offset: usize) -> std::io::Result<()> {
    file.seek(SeekFrom::Start(
        u64::try_from(offset).map_err(|_| std::io::Error::other("journal offset overflow"))?,
    ))?;
    file.write_all(bytes)
}

#[cfg(unix)]
fn read_exact_at(file: &mut File, mut bytes: &mut [u8], mut offset: usize) -> std::io::Result<()> {
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
fn read_exact_at(file: &mut File, bytes: &mut [u8], offset: usize) -> std::io::Result<()> {
    file.seek(SeekFrom::Start(
        u64::try_from(offset).map_err(|_| std::io::Error::other("journal offset overflow"))?,
    ))?;
    file.read_exact(bytes)
}

fn read_until_full_or_eof(file: &mut File, bytes: &mut [u8]) -> Result<usize, JournalIoError> {
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

fn digest(bytes: &[u8]) -> [u8; HASH_BYTES] {
    Sha256::digest(bytes).into()
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
    use super::*;

    static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

    struct TestPath(PathBuf);

    impl TestPath {
        fn new(label: &str) -> Self {
            let ordinal = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
            let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/journal-tests");
            std::fs::create_dir_all(&root).expect("create journal test root");
            Self(root.join(format!("{label}-{}-{ordinal}.journal", std::process::id())))
        }
    }

    impl Drop for TestPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
            let journal = journal_path(&self.0);
            let _ = std::fs::remove_file(&journal);
            let _ = std::fs::remove_file(checkpoint_journal_path(&self.0));
            let _ = std::fs::remove_file(spare_journal_path(&self.0));
            if let Some(file_name) = journal.file_name() {
                let mut replacement = file_name.to_os_string();
                replacement.push(".rewrite");
                let _ = std::fs::remove_file(journal.with_file_name(replacement));
            }
        }
    }

    fn database_id(seed: u8) -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [seed; 10])
            .expect("database ID")
    }

    fn sequence(value: u64) -> CommitSequence {
        CommitSequence::new(value).expect("nonzero sequence")
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
        typed
            .push_command_segment(key, value, 2)
            .expect("buffer typed segment put");

        assert_eq!(typed.bytes, ordinary.bytes);
        assert_eq!(typed.mutation_count, ordinary.mutation_count);
        assert_eq!(typed.logical_command_put_count, 2);
        assert_eq!(typed.command_audit_count, 4);
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
        assert_eq!(MAX_JOURNAL_SUFFIX_TRANSITIONS, 8_192);
        assert_eq!(MAX_JOURNAL_SUFFIX_BYTES, 32 * 1024 * 1024);
        assert_eq!(EXTENT_DATA_BYTES, 40 * 1024 * 1024);
        let maximum_physical_frame =
            extent_frame_bytes(MAX_JOURNAL_FRAME_BYTES).expect("maximum frame fits extent");
        assert!(maximum_physical_frame > MAX_JOURNAL_FRAME_BYTES);
        assert!(maximum_physical_frame < EXTENT_DATA_BYTES);
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
        .encode()
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

    #[test]
    fn recovery_replays_a_complete_suffix_and_reclaims_it() {
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
            .expect("scan reclaimed journal")
            .expect("journal exists");
        assert_eq!(tail.last_sequence, Some(sequence(1)));
        assert_eq!(tail.command_count, 0);
        assert_eq!(tail.complete_bytes, EXTENT_DATA_OFFSET);
        assert!(!tail.incomplete_tail);
    }

    #[test]
    fn recovery_accepts_checkpoint_before_reclamation_only_after_exact_tail_verification() {
        let database_path = TestPath::new("recovery-verify");
        let database = create_database(&database_path.0);
        let database_id = database_id(14);
        let frame = recovery_frame(database_id);
        let encoded = frame.encode().expect("encode frame");
        write_recovery_journal(&database_path.0, database_id, &encoded, None);
        replay_frames(&database, std::slice::from_ref(&frame)).expect("simulate checkpoint");

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
        replay_frames(&database, std::slice::from_ref(&frame)).expect("advance redb checkpoint");
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
            .expect("scan reclaimed journal")
            .expect("journal exists");
        assert_eq!(tail.command_count, 0);
        assert!(!tail.incomplete_tail);
    }
}
