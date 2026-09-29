//! History stubs (ADR-0263 Amendment 3): fixed-size locators that stand in
//! the COMMITS and HISTORY tables for a segment or receipt a standard-profile
//! journal frame made durable. The segment and receipt bytes live only in the
//! retained journal extent; a stub keeps the table's keys, order, tails, and
//! row counts intact, and every reader resolves it back to the exact value.
//!
//! A stub is a representation, never replicated state (C3): a caller hashes,
//! compares, or copies the resolved value, which is byte-identical to what the
//! table held before stubs existed.

use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};

use riffdb_storage_api::{StorageError, StorageErrorKind};
use riffdb_types::{AdministrationSequence, CommitSequence, DatabaseId};

use crate::error::storage_error;
use crate::journal::{FramePlacement, JournalFrame, JournalTable};
use crate::media::JournalMedia;

const STUB_MAGIC: [u8; 8] = *b"RDBHST01";
const STUB_VERSION: u16 = 1;
/// Exact encoded size of every stub.
pub(crate) const STUB_BYTES: usize =
    8 + 2 + 1 + 5 + 16 + 8 + 8 + 4 + 4 + 32 + 8 + 8 + 8 + 4 + 4 + 32 + 32;

/// Which table value a stub stands for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum StubKind {
    /// A command segment under its first commit sequence in COMMITS.
    Segment = 1,
    /// A V3 receipt under its changelog sequence in HISTORY.
    Receipt = 2,
}

/// Summary a segment stub carries so tails, frontiers, and the segment chain
/// are answered without reading the extent.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct SegmentSummary {
    pub(crate) first_commit_sequence: Option<CommitSequence>,
    pub(crate) last_commit_sequence: Option<CommitSequence>,
    pub(crate) terminal_administration_sequence: Option<AdministrationSequence>,
    pub(crate) command_count: u32,
    pub(crate) segment_digest: [u8; 32],
}

/// One decoded stub.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct HistoryStub {
    pub(crate) kind: StubKind,
    pub(crate) database_id: DatabaseId,
    pub(crate) generation: u64,
    pub(crate) placement: FramePlacement,
    pub(crate) summary: SegmentSummary,
}

impl HistoryStub {
    /// Encodes the stub; a placement beyond the stub's field widths is refused.
    pub(crate) fn encode(&self) -> Result<Vec<u8>, StorageError> {
        let limit = || storage_error(StorageErrorKind::LimitExceeded);
        let position = u64::try_from(self.placement.position).map_err(|_| limit())?;
        let padded_len = u32::try_from(self.placement.padded_len).map_err(|_| limit())?;
        let mut bytes = Vec::with_capacity(STUB_BYTES);
        bytes.extend_from_slice(&STUB_MAGIC);
        bytes.extend_from_slice(&STUB_VERSION.to_be_bytes());
        bytes.push(self.kind as u8);
        bytes.extend_from_slice(&[0; 5]);
        bytes.extend_from_slice(self.database_id.as_bytes());
        bytes.extend_from_slice(&self.generation.to_be_bytes());
        bytes.extend_from_slice(&position.to_be_bytes());
        bytes.extend_from_slice(&padded_len.to_be_bytes());
        bytes.extend_from_slice(&[0; 4]);
        bytes.extend_from_slice(&self.placement.frame_hash);
        bytes.extend_from_slice(
            &self
                .summary
                .first_commit_sequence
                .map_or(0, CommitSequence::get)
                .to_be_bytes(),
        );
        bytes.extend_from_slice(
            &self
                .summary
                .last_commit_sequence
                .map_or(0, CommitSequence::get)
                .to_be_bytes(),
        );
        bytes.extend_from_slice(
            &self
                .summary
                .terminal_administration_sequence
                .map_or(0, AdministrationSequence::get)
                .to_be_bytes(),
        );
        bytes.extend_from_slice(&self.summary.command_count.to_be_bytes());
        bytes.extend_from_slice(&[0; 4]);
        bytes.extend_from_slice(&self.summary.segment_digest);
        let checksum = crate::journal::content_digest(&bytes);
        bytes.extend_from_slice(&checksum);
        debug_assert_eq!(bytes.len(), STUB_BYTES);
        Ok(bytes)
    }

    /// Decodes `bytes` if they are a stub. `Ok(None)` means an ordinary table
    /// value; a value that claims the stub magic but fails any check is
    /// corruption, never an ordinary value.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Option<Self>, StorageError> {
        let Ok(bytes) = <&[u8; STUB_BYTES]>::try_from(bytes) else {
            return Ok(None);
        };
        if bytes[..8] != STUB_MAGIC {
            return Ok(None);
        }
        let corrupt = || storage_error(StorageErrorKind::CorruptData);
        let u64_at = |at: usize| u64::from_be_bytes(fixed(bytes, at));
        let u32_at = |at: usize| u32::from_be_bytes(fixed(bytes, at));
        let body = &bytes[..STUB_BYTES - 32];
        let checksum = crate::journal::content_digest(body);
        if checksum.as_slice() != &bytes[STUB_BYTES - 32..]
            || u16::from_be_bytes([bytes[8], bytes[9]]) != STUB_VERSION
            || bytes[11..16].iter().any(|byte| *byte != 0)
            || bytes[52..56].iter().any(|byte| *byte != 0)
            || bytes[116..120].iter().any(|byte| *byte != 0)
        {
            return Err(corrupt());
        }
        let kind = match bytes[10] {
            1 => StubKind::Segment,
            2 => StubKind::Receipt,
            _ => return Err(corrupt()),
        };
        let database_id = DatabaseId::from_bytes(fixed(bytes, 16)).map_err(|_| corrupt())?;
        Ok(Some(Self {
            kind,
            database_id,
            generation: u64_at(32),
            placement: FramePlacement {
                position: usize::try_from(u64_at(40)).map_err(|_| corrupt())?,
                padded_len: usize::try_from(u32_at(48)).map_err(|_| corrupt())?,
                frame_hash: fixed(bytes, 56),
            },
            summary: SegmentSummary {
                first_commit_sequence: CommitSequence::new(u64_at(88)),
                last_commit_sequence: CommitSequence::new(u64_at(96)),
                terminal_administration_sequence: AdministrationSequence::new(u64_at(104)),
                command_count: u32_at(112),
                segment_digest: fixed(bytes, 120),
            },
        }))
    }
}

/// Copies the `N` bytes at `at` out of the fixed stub layout.
fn fixed<const N: usize>(bytes: &[u8; STUB_BYTES], at: usize) -> [u8; N] {
    let mut out = [0; N];
    out.copy_from_slice(&bytes[at..at + N]);
    out
}

/// The summary of a segment stub, or `None` for an ordinary COMMITS value.
pub(crate) fn segment_summary(value: &[u8]) -> Result<Option<SegmentSummary>, StorageError> {
    match HistoryStub::decode(value)? {
        Some(stub) if stub.kind == StubKind::Segment => Ok(Some(stub.summary)),
        Some(_) => Err(storage_error(StorageErrorKind::CorruptData)),
        None => Ok(None),
    }
}

/// Where one open store's retained extents live.
#[derive(Debug)]
pub(crate) struct HistoryResolver {
    database_id: DatabaseId,
    database_path: PathBuf,
    media: Arc<dyn JournalMedia>,
}

static RESOLVERS: Mutex<Vec<Weak<HistoryResolver>>> = Mutex::new(Vec::new());

impl HistoryResolver {
    /// Registers the extents of an open store. Stubs are resolved through
    /// every live registration for their database identity; only a frame whose
    /// hash matches the stub is accepted, so a same-identity copy (a restored
    /// backup) can never supply another copy's bytes.
    pub(crate) fn register(
        database_id: DatabaseId,
        database_path: &Path,
        media: Arc<dyn JournalMedia>,
    ) -> Arc<Self> {
        let resolver = Arc::new(Self {
            database_id,
            database_path: database_path.to_path_buf(),
            media,
        });
        if let Ok(mut resolvers) = RESOLVERS.lock() {
            resolvers.retain(|entry| entry.strong_count() > 0);
            resolvers.push(Arc::downgrade(&resolver));
        }
        resolver
    }
}

fn read_stub_frame(stub: &HistoryStub) -> Result<JournalFrame, StorageError> {
    let resolvers = RESOLVERS
        .lock()
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
        .iter()
        .filter_map(Weak::upgrade)
        .filter(|resolver| resolver.database_id == stub.database_id)
        .collect::<Vec<_>>();
    if resolvers.is_empty() {
        // No open store can reach the extent this stub names.
        return Err(storage_error(StorageErrorKind::Unavailable));
    }
    for resolver in resolvers {
        if let Ok(frame) = crate::journal::read_retained_frame_with_media(
            resolver.media.as_ref(),
            &resolver.database_path,
            stub.database_id,
            stub.placement,
            stub.generation,
        ) {
            return Ok(frame);
        }
    }
    Err(storage_error(StorageErrorKind::CorruptData))
}

/// Resolves a COMMITS value: an ordinary segment is returned as is; a stub is
/// replaced by the exact segment bytes its frame holds under the stub's first
/// commit sequence, which is the COMMITS key.
pub(crate) fn resolve_commits_value(value: &[u8]) -> Result<Cow<'_, [u8]>, StorageError> {
    let Some(stub) = HistoryStub::decode(value)? else {
        return Ok(Cow::Borrowed(value));
    };
    let first = stub
        .summary
        .first_commit_sequence
        .filter(|_| stub.kind == StubKind::Segment)
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    let key = crate::keys::encode_application_sequence_key(first);
    let frame = read_stub_frame(&stub)?;
    frame
        .mutations()
        .iter()
        .find(|mutation| {
            mutation.table() == JournalTable::Commits && mutation.key() == key.as_slice()
        })
        .and_then(|mutation| mutation.value())
        .map(|value| Cow::Owned(value.to_vec()))
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))
}

fn codec_corrupt() -> riffdb_storage_api::DurableCodecError {
    riffdb_storage_api::DurableCodecError::new(
        riffdb_storage_api::DurableCodecErrorKind::CorruptData,
    )
}

/// Decodes a COMMITS row's segment, resolving a stub first. A stub that cannot
/// be resolved is corrupt data, never an unexpected (legacy) record type, so
/// no caller skips it as a non-segment row.
pub(crate) fn decode_segment_value(
    value: &[u8],
) -> Result<
    riffdb_storage_api::EncodedPageItem<riffdb_storage_api::StoredCommandSegmentV1>,
    riffdb_storage_api::DurableCodecError,
> {
    let resolved = resolve_commits_value(value).map_err(|_| codec_corrupt())?;
    riffdb_storage_api::decode_command_segment_v1(&resolved)
}

/// [`crate::command_prefix::decode_segment`] over a COMMITS row, resolving a
/// stub first.
pub(crate) fn decode_retained_segment(
    value: &[u8],
) -> Result<
    riffdb_storage_api::EncodedPageItem<riffdb_storage_api::StoredCommandSegmentV1>,
    riffdb_storage_api::DurableCodecError,
> {
    let resolved = resolve_commits_value(value).map_err(|_| codec_corrupt())?;
    crate::command_prefix::decode_segment(&resolved)
}

/// Resolves a HISTORY value: an ordinary receipt is returned as is; a stub is
/// replaced by the exact receipt re-derived from its frame and the binding
/// the frame records.
pub(crate) fn resolve_history_value(value: &[u8]) -> Result<Cow<'_, [u8]>, StorageError> {
    let Some(stub) = HistoryStub::decode(value)? else {
        return Ok(Cow::Borrowed(value));
    };
    if stub.kind != StubKind::Receipt {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    let frame = read_stub_frame(&stub)?;
    let recorded = frame
        .changelog()
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    let receipt = crate::changelog_v3::receipt_from_journal_for_catalog(
        &frame,
        recorded.binding(&frame),
        recorded.catalog_digest(),
    )
    .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
    receipt
        .encode()
        .map(Cow::Owned)
        .map_err(|_| storage_error(StorageErrorKind::CorruptData))
}
