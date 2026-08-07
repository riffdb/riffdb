//! Versioned local durability-frame encoding for the standard application profile.
#![allow(
    dead_code,
    reason = "WP-478 lands the closed frame and lane boundary before production coordinator wiring"
)]

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;

use sha2::{Digest, Sha256};

use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};

use riffdb_types::{CommitSequence, DatabaseId};

use crate::keys::decode_application_sequence_key;
use crate::layout::{
    AUDIT, AUDIT_BY_REQUEST, COMMITS, ENTITIES, EVENT_ROUTES, EVENTS, IDEMPOTENCY,
    IDEMPOTENCY_PENDING, INDEX_EPOCHS, META, PROVENANCE, SECONDARY_INDEXES,
};

const FILE_MAGIC: [u8; 8] = *b"RDBJRN01";
const FRAME_MAGIC: [u8; 8] = *b"RDBFRM01";
const FOOTER_MAGIC: [u8; 8] = *b"RDBEND01";
const FORMAT_VERSION: u16 = 1;
const HASH_BYTES: usize = 32;
const FILE_HEADER_BYTES: usize = 8 + 2 + 16 + 8 + HASH_BYTES + HASH_BYTES;
const FRAME_HEADER_BYTES: usize = 8 + 2 + 16 + 8 + 8 + 2 + 4 + 4 + HASH_BYTES;
const FRAME_FOOTER_BYTES: usize = 8 + 8 + HASH_BYTES;
const MUTATION_HEADER_BYTES: usize = 1 + 1 + 1 + 1 + 4 + 4 + HASH_BYTES;

pub(crate) const MAX_JOURNAL_COMMANDS: usize = 256;
pub(crate) const MAX_JOURNAL_FRAME_BYTES: usize = 16 * 1024 * 1024;
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JournalFileHeader {
    database_id: DatabaseId,
    checkpoint_sequence: Option<CommitSequence>,
    checkpoint_frame_hash: [u8; HASH_BYTES],
}

impl JournalFileHeader {
    pub(crate) fn new(
        database_id: DatabaseId,
        checkpoint_sequence: Option<CommitSequence>,
        checkpoint_frame_hash: [u8; HASH_BYTES],
    ) -> Self {
        Self {
            database_id,
            checkpoint_sequence,
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
        bytes[34..66].copy_from_slice(&self.checkpoint_frame_hash);
        let checksum = digest(&bytes[..66]);
        bytes[66..].copy_from_slice(&checksum);
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, JournalCodecError> {
        if bytes.len() != FILE_HEADER_BYTES {
            return Err(JournalCodecError::Truncated);
        }
        if bytes[..8] != FILE_MAGIC || read_u16(bytes, 8)? != FORMAT_VERSION {
            return Err(JournalCodecError::UnknownVersion);
        }
        if digest(&bytes[..66]).as_slice() != &bytes[66..] {
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
        Ok(Self {
            database_id,
            checkpoint_sequence,
            checkpoint_frame_hash: read_array::<HASH_BYTES>(bytes, 34)?,
        })
    }

    pub(crate) fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    pub(crate) fn checkpoint_sequence(&self) -> Option<CommitSequence> {
        self.checkpoint_sequence
    }

    pub(crate) fn checkpoint_frame_hash(&self) -> [u8; HASH_BYTES] {
        self.checkpoint_frame_hash
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JournalFrame {
    database_id: DatabaseId,
    first_sequence: CommitSequence,
    last_sequence: CommitSequence,
    command_count: u16,
    previous_hash: [u8; HASH_BYTES],
    mutations: Vec<JournalMutation>,
}

impl JournalFrame {
    pub(crate) fn new(
        database_id: DatabaseId,
        first_sequence: CommitSequence,
        last_sequence: CommitSequence,
        command_count: u16,
        previous_hash: [u8; HASH_BYTES],
        mutations: Vec<JournalMutation>,
    ) -> Result<Self, JournalCodecError> {
        if command_count == 0
            || usize::from(command_count) > MAX_JOURNAL_COMMANDS
            || mutations.is_empty()
            || last_sequence
                .get()
                .saturating_sub(first_sequence.get())
                .saturating_add(1)
                != u64::from(command_count)
        {
            return Err(JournalCodecError::InvalidValue);
        }
        let frame = Self {
            database_id,
            first_sequence,
            last_sequence,
            command_count,
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
        bytes.extend_from_slice(&self.first_sequence.get().to_be_bytes());
        bytes.extend_from_slice(&self.last_sequence.get().to_be_bytes());
        bytes.extend_from_slice(&self.command_count.to_be_bytes());
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
            bytes,
            frame_hash,
            command_count: self.command_count,
            last_sequence: self.last_sequence,
        })
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<(Self, [u8; HASH_BYTES]), JournalCodecError> {
        if bytes.len() < FRAME_HEADER_BYTES + FRAME_FOOTER_BYTES {
            return Err(JournalCodecError::Truncated);
        }
        if bytes[..8] != FRAME_MAGIC || read_u16(bytes, 8)? != FORMAT_VERSION {
            return Err(JournalCodecError::UnknownVersion);
        }
        let mutation_count =
            usize::try_from(read_u32(bytes, 44)?).map_err(|_| JournalCodecError::LimitExceeded)?;
        let payload_len =
            usize::try_from(read_u32(bytes, 48)?).map_err(|_| JournalCodecError::LimitExceeded)?;
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
        let first_sequence =
            CommitSequence::new(read_u64(bytes, 26)?).ok_or(JournalCodecError::InvalidValue)?;
        let last_sequence =
            CommitSequence::new(read_u64(bytes, 34)?).ok_or(JournalCodecError::InvalidValue)?;
        let command_count = read_u16(bytes, 42)?;
        let frame = Self::new(
            database_id,
            first_sequence,
            last_sequence,
            command_count,
            read_array::<HASH_BYTES>(bytes, 52)?,
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

    pub(crate) fn first_sequence(&self) -> CommitSequence {
        self.first_sequence
    }

    pub(crate) fn last_sequence(&self) -> CommitSequence {
        self.last_sequence
    }

    pub(crate) fn previous_hash(&self) -> [u8; HASH_BYTES] {
        self.previous_hash
    }

    pub(crate) fn mutations(&self) -> &[JournalMutation] {
        &self.mutations
    }
}

pub(crate) struct EncodedJournalFrame {
    bytes: Vec<u8>,
    frame_hash: [u8; HASH_BYTES],
    command_count: u16,
    last_sequence: CommitSequence,
}

impl EncodedJournalFrame {
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn frame_hash(&self) -> [u8; HASH_BYTES] {
        self.frame_hash
    }

    fn command_count(&self) -> u16 {
        self.command_count
    }

    fn last_sequence(&self) -> CommitSequence {
        self.last_sequence
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct JournalFence {
    pub(crate) last_sequence: CommitSequence,
    pub(crate) frame_hash: [u8; HASH_BYTES],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum JournalIoError {
    Corrupt,
    Io,
    Stopped,
}

struct JournalSubmission {
    frame: EncodedJournalFrame,
    completion: mpsc::Sender<Result<JournalFence, JournalIoError>>,
}

pub(crate) struct JournalFenceReceipt {
    completion: mpsc::Receiver<Result<JournalFence, JournalIoError>>,
}

impl JournalFenceReceipt {
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
        initialize_or_validate_file(path, header)?;
        let file = OpenOptions::new()
            .append(true)
            .open(path)
            .map_err(|_| JournalIoError::Io)?;
        let (sender, receiver) = mpsc::sync_channel(MAX_JOURNAL_COMMANDS);
        let durable_flushes = Arc::new(AtomicU64::new(0));
        let worker_flushes = Arc::clone(&durable_flushes);
        let worker = thread::Builder::new()
            .name("riffdb-journal".to_string())
            .spawn(move || journal_worker(file, receiver, &worker_flushes))
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
) -> Result<(), JournalIoError> {
    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(mut file) => {
            file.write_all(&expected.encode())
                .and_then(|()| file.sync_data())
                .map_err(|_| JournalIoError::Io)?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let mut file = File::open(path).map_err(|_| JournalIoError::Io)?;
            let mut bytes = [0_u8; FILE_HEADER_BYTES];
            file.read_exact(&mut bytes)
                .map_err(|_| JournalIoError::Corrupt)?;
            let actual = JournalFileHeader::decode(&bytes).map_err(|_| JournalIoError::Corrupt)?;
            if actual != *expected {
                return Err(JournalIoError::Corrupt);
            }
            Ok(())
        }
        Err(_) => Err(JournalIoError::Io),
    }
}

fn journal_worker(
    mut file: File,
    receiver: mpsc::Receiver<JournalSubmission>,
    durable_flushes: &AtomicU64,
) {
    let mut deferred = None;
    loop {
        let first = match deferred.take() {
            Some(value) => value,
            None => match receiver.recv() {
                Ok(value) => value,
                Err(_) => return,
            },
        };
        let mut command_count = usize::from(first.frame.command_count());
        let mut byte_count = first.frame.as_bytes().len();
        let mut batch = vec![first];
        while batch.len() < MAX_JOURNAL_COMMANDS {
            let next = match receiver.try_recv() {
                Ok(value) => value,
                Err(mpsc::TryRecvError::Empty | mpsc::TryRecvError::Disconnected) => break,
            };
            let next_commands =
                command_count.saturating_add(usize::from(next.frame.command_count()));
            let next_bytes = byte_count.saturating_add(next.frame.as_bytes().len());
            if next_commands > MAX_JOURNAL_COMMANDS || next_bytes > MAX_JOURNAL_FRAME_BYTES {
                deferred = Some(next);
                break;
            }
            command_count = next_commands;
            byte_count = next_bytes;
            batch.push(next);
        }

        let write = batch
            .iter()
            .try_for_each(|submission| file.write_all(submission.frame.as_bytes()))
            .and_then(|()| file.sync_data());
        if write.is_ok() {
            durable_flushes.fetch_add(1, Ordering::Relaxed);
        }
        for submission in batch {
            let result = if write.is_ok() {
                Ok(JournalFence {
                    last_sequence: submission.frame.last_sequence(),
                    frame_hash: submission.frame.frame_hash(),
                })
            } else {
                Err(JournalIoError::Io)
            };
            let _ = submission.completion.send(result);
        }
        if write.is_err() {
            for submission in receiver.try_iter() {
                let _ = submission.completion.send(Err(JournalIoError::Io));
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
    pub(crate) last_hash: [u8; HASH_BYTES],
    pub(crate) incomplete_tail: bool,
    pub(crate) complete_bytes: usize,
    pub(crate) command_count: usize,
}

pub(crate) fn journal_path(database_path: &Path) -> PathBuf {
    let mut path = database_path.as_os_str().to_os_string();
    path.push(".riffjournal");
    PathBuf::from(path)
}

pub(crate) fn scan_journal(
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
    let mut complete_bytes = FILE_HEADER_BYTES;
    let mut command_count = 0_usize;
    loop {
        let mut frame_header = [0_u8; FRAME_HEADER_BYTES];
        let read = read_until_full_or_eof(&mut file, &mut frame_header)?;
        if read == 0 {
            return Ok(Some((
                header,
                JournalScanTail {
                    last_sequence: previous_sequence,
                    last_hash: previous_hash,
                    incomplete_tail: false,
                    complete_bytes,
                    command_count,
                },
            )));
        }
        if read != FRAME_HEADER_BYTES {
            return Ok(Some((
                header,
                JournalScanTail {
                    last_sequence: previous_sequence,
                    last_hash: previous_hash,
                    incomplete_tail: true,
                    complete_bytes,
                    command_count,
                },
            )));
        }
        let payload_len =
            usize::try_from(read_u32(&frame_header, 48).map_err(|_| JournalIoError::Corrupt)?)
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
                    last_hash: previous_hash,
                    incomplete_tail: true,
                    complete_bytes,
                    command_count,
                },
            )));
        }
        encoded.extend_from_slice(&rest);
        let (frame, frame_hash) =
            JournalFrame::decode(&encoded).map_err(|_| JournalIoError::Corrupt)?;
        if frame.database_id() != expected_database
            || frame.previous_hash() != previous_hash
            || previous_sequence
                .and_then(CommitSequence::checked_next)
                .is_some_and(|next| next != frame.first_sequence())
            || previous_sequence.is_none()
                && header.checkpoint_sequence().is_none()
                && frame.first_sequence() != CommitSequence::first()
        {
            return Err(JournalIoError::Corrupt);
        }
        complete_bytes = complete_bytes
            .checked_add(total)
            .ok_or(JournalIoError::Corrupt)?;
        command_count = command_count
            .checked_add(usize::from(frame.command_count))
            .ok_or(JournalIoError::Corrupt)?;
        if complete_bytes.saturating_sub(FILE_HEADER_BYTES) > MAX_JOURNAL_FRAME_BYTES
            || command_count > MAX_JOURNAL_COMMANDS
        {
            return Err(JournalIoError::Corrupt);
        }
        visit(&frame)?;
        previous_sequence = Some(frame.last_sequence());
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
    let path = journal_path(database_path);
    let mut frames = Vec::new();
    let Some((header, tail)) = scan_journal(&path, database_id, |frame| {
        frames.push(frame.clone());
        Ok(())
    })?
    else {
        return Ok(());
    };
    let redb_sequence = read_redb_tail(database)?;
    if redb_sequence == header.checkpoint_sequence() {
        if !frames.is_empty() {
            replay_frames(database, &frames)?;
        }
    } else if redb_sequence == tail.last_sequence && !frames.is_empty() {
        verify_replayed_tail(database, &frames)?;
    } else {
        return Err(JournalIoError::Corrupt);
    }
    reset_journal(
        &path,
        &JournalFileHeader::new(database_id, tail.last_sequence, tail.last_hash),
    )
}

fn read_redb_tail(database: &Database) -> Result<Option<CommitSequence>, JournalIoError> {
    let transaction = database.begin_read().map_err(|_| JournalIoError::Io)?;
    let table = transaction
        .open_table(COMMITS)
        .map_err(|_| JournalIoError::Corrupt)?;
    table
        .last()
        .map_err(|_| JournalIoError::Corrupt)?
        .map(|(key, _)| decode_application_sequence_key(key.value()))
        .transpose()
        .map_err(|_| JournalIoError::Corrupt)
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

fn apply_mutation(
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

fn read_value(
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

fn reset_journal(path: &Path, header: &JournalFileHeader) -> Result<(), JournalIoError> {
    let parent = path.parent().ok_or(JournalIoError::Io)?;
    let file_name = path.file_name().ok_or(JournalIoError::Io)?;
    let mut replacement_name = file_name.to_os_string();
    replacement_name.push(".rewrite");
    let replacement = parent.join(replacement_name);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&replacement)
        .map_err(|_| JournalIoError::Io)?;
    file.write_all(&header.encode())
        .and_then(|()| file.sync_data())
        .map_err(|_| JournalIoError::Io)?;
    std::fs::rename(&replacement, path).map_err(|_| JournalIoError::Io)?;
    File::open(parent)
        .and_then(|directory| directory.sync_data())
        .map_err(|_| JournalIoError::Io)
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
            ],
        )
        .expect("frame")
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
        tail_bytes: Option<&[u8]>,
    ) {
        let path = journal_path(database_path);
        let header = JournalFileHeader::new(database_id, None, [0; HASH_BYTES]);
        initialize_or_validate_file(&path, &header).expect("initialize recovery journal");
        let mut file = OpenOptions::new()
            .append(true)
            .open(path)
            .expect("open recovery journal");
        file.write_all(frame.as_bytes()).expect("write frame");
        if let Some(tail) = tail_bytes {
            file.write_all(tail).expect("write tail");
        }
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
                receipt.wait().expect("durable frame").last_sequence,
                sequence(u64::try_from(index + 1).expect("bounded index"))
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
        let mut file = OpenOptions::new()
            .append(true)
            .open(&path.0)
            .expect("open journal");
        file.write_all(first.as_bytes()).expect("write first");
        file.write_all(second.as_bytes()).expect("write second");
        file.sync_data().expect("sync journal");

        let mut visited = Vec::new();
        let (actual_header, tail) = scan_journal(&path.0, database_id, |frame| {
            visited.push((frame.first_sequence(), frame.last_sequence()));
            Ok(())
        })
        .expect("scan")
        .expect("journal exists");
        assert_eq!(actual_header, header);
        assert_eq!(
            visited,
            vec![(sequence(1), sequence(1)), (sequence(2), sequence(2))]
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
        let mut file = OpenOptions::new()
            .append(true)
            .open(&path.0)
            .expect("open journal");
        file.write_all(first.as_bytes()).expect("write first");
        file.write_all(&second.as_bytes()[..second.as_bytes().len() / 2])
            .expect("write torn second");
        file.sync_data().expect("sync journal");

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
        let mut file = OpenOptions::new()
            .append(true)
            .open(&path.0)
            .expect("open journal");
        file.write_all(frame.as_bytes()).expect("write frame");
        file.sync_data().expect("sync journal");

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
        assert_eq!(tail.complete_bytes, FILE_HEADER_BYTES);
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
    fn recovery_discards_only_a_torn_terminal_tail() {
        let database_path = TestPath::new("recovery-torn");
        let database = create_database(&database_path.0);
        let database_id = database_id(15);
        let frame = recovery_frame(database_id).encode().expect("encode frame");
        let torn = recovery_frame(database_id)
            .encode()
            .expect("encode torn frame");
        write_recovery_journal(
            &database_path.0,
            database_id,
            &frame,
            Some(&torn.as_bytes()[..torn.as_bytes().len() / 2]),
        );

        recover_journal(&database, &database_path.0, database_id).expect("recover complete prefix");
        let (_, tail) = scan_journal(&journal_path(&database_path.0), database_id, |_| Ok(()))
            .expect("scan reclaimed journal")
            .expect("journal exists");
        assert_eq!(tail.command_count, 0);
        assert!(!tail.incomplete_tail);
    }
}
