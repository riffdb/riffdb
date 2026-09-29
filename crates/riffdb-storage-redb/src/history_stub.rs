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

impl SegmentSummary {
    /// The summary a stub carries for `segment`.
    pub(crate) fn of(
        segment: &riffdb_storage_api::StoredCommandSegmentV1,
    ) -> Result<Self, StorageError> {
        Ok(Self {
            first_commit_sequence: Some(segment.first_commit_sequence()),
            last_commit_sequence: Some(segment.last_commit_sequence()),
            terminal_administration_sequence: Some(segment.last_administration_sequence()),
            command_count: u32::try_from(segment.commands().len())
                .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?,
            segment_digest: *segment.segment_digest().as_bytes(),
        })
    }
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

/// Where one retained frame lives: every stub for that frame names it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StubLocator {
    pub(crate) database_id: DatabaseId,
    pub(crate) generation: u64,
    pub(crate) placement: FramePlacement,
}

impl StubLocator {
    fn stub(self, kind: StubKind, summary: SegmentSummary) -> HistoryStub {
        HistoryStub {
            kind,
            database_id: self.database_id,
            generation: self.generation,
            placement: self.placement,
            summary,
        }
    }

    /// The encoded receipt stub for this frame.
    pub(crate) fn receipt_stub(self) -> Result<Vec<u8>, StorageError> {
        self.stub(StubKind::Receipt, SegmentSummary::default())
            .encode()
    }

    /// The encoded segment stub standing for `value`, a COMMITS value this
    /// frame inserts, or `None` when `value` is not a command segment. Such a
    /// value keeps its exact bytes, as every COMMITS value did before stubs.
    pub(crate) fn segment_stub(self, value: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let Ok(segment) = riffdb_storage_api::decode_command_segment_v1(value) else {
            return Ok(None);
        };
        self.summary_stub(SegmentSummary::of(&segment.into_parts().0)?)
            .map(Some)
    }

    /// The encoded segment stub for a segment whose summary the writer
    /// recorded when it built the segment, without decoding it again.
    pub(crate) fn summary_stub(self, summary: SegmentSummary) -> Result<Vec<u8>, StorageError> {
        self.stub(StubKind::Segment, summary).encode()
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

/// Where one open store's, or one backup's, retained extents live.
#[derive(Debug)]
pub(crate) struct HistoryResolver {
    database_id: DatabaseId,
    location: ExtentLocation,
    media: Arc<dyn JournalMedia>,
    /// The mutations of this store's uncheckpointed frames by frame hash. A
    /// stub in the overlay may name a frame the journal worker has not yet
    /// written, and the writer must not read the file for its own recent
    /// segments. Entries are weak: the checkpoint suffix owns the mutations,
    /// and once it drops them the frame is durable and read from its extent.
    pending: Mutex<PendingFrames>,
}

#[derive(Debug, Default)]
struct PendingFrames {
    frames: std::collections::HashMap<[u8; 32], Weak<[riffdb_storage_api::CompositeMutationV1]>>,
    /// Size at which dropped entries are next swept, keeping inserts O(1).
    sweep_at: usize,
}

#[derive(Debug)]
enum ExtentLocation {
    /// Extents named beside a database file.
    Database(PathBuf),
    /// A backup directory's journal and sealed extent artifacts.
    Backup(PathBuf),
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
        Self::register_at(
            database_id,
            ExtentLocation::Database(database_path.to_path_buf()),
            media,
        )
    }

    /// Registers the extents beside a database file opened directly rather
    /// than through a store, if the database has an identity; one without
    /// can hold no stubs.
    pub(crate) fn register_beside(
        database_path: &Path,
        read: &redb::ReadTransaction,
    ) -> Option<Arc<Self>> {
        crate::store::read_identity_from_read_transaction(read)
            .ok()
            .map(|database_id| {
                Self::register(
                    database_id,
                    database_path,
                    Arc::new(crate::media::RealJournalMedia),
                )
            })
    }

    /// Registers a verified backup's artifacts, for reading its history in
    /// place while it stays open.
    pub(crate) fn register_backup(database_id: DatabaseId, directory: &Path) -> Arc<Self> {
        Self::register_at(
            database_id,
            ExtentLocation::Backup(directory.to_path_buf()),
            Arc::new(crate::media::RealJournalMedia),
        )
    }

    fn register_at(
        database_id: DatabaseId,
        location: ExtentLocation,
        media: Arc<dyn JournalMedia>,
    ) -> Arc<Self> {
        let resolver = Arc::new(Self {
            database_id,
            location,
            media,
            pending: Mutex::new(PendingFrames::default()),
        });
        if let Ok(mut resolvers) = RESOLVERS.lock() {
            resolvers.retain(|entry| entry.strong_count() > 0);
            resolvers.push(Arc::downgrade(&resolver));
        }
        resolver
    }
}

impl HistoryResolver {
    /// Records the mutations of an uncheckpointed frame the writer just sealed,
    /// before any reader can see its stubs.
    pub(crate) fn note_pending_frame(
        &self,
        frame_hash: [u8; 32],
        mutations: &Arc<[riffdb_storage_api::CompositeMutationV1]>,
    ) {
        let Ok(mut pending) = self.pending.lock() else {
            return;
        };
        if pending.frames.len() >= pending.sweep_at {
            pending.frames.retain(|_, frame| frame.strong_count() > 0);
            pending.sweep_at = pending.frames.len().saturating_mul(2).max(64);
        }
        pending.frames.insert(frame_hash, Arc::downgrade(mutations));
    }

    fn pending_segment(&self, frame_hash: &[u8; 32], key: &[u8]) -> Option<Vec<u8>> {
        let mutations = self
            .pending
            .lock()
            .ok()?
            .frames
            .get(frame_hash)?
            .upgrade()?;
        mutations
            .iter()
            .find(|mutation| {
                mutation.table() == riffdb_storage_api::CompositeTableV1::Commits
                    && mutation.key() == key
            })
            .and_then(riffdb_storage_api::CompositeMutationV1::value)
            .map(<[u8]>::to_vec)
    }
}

fn live_resolvers(database_id: DatabaseId) -> Result<Vec<Arc<HistoryResolver>>, StorageError> {
    Ok(RESOLVERS
        .lock()
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
        .iter()
        .filter_map(Weak::upgrade)
        .filter(|resolver| resolver.database_id == database_id)
        .collect())
}

fn read_stub_frame(stub: &HistoryStub) -> Result<JournalFrame, StorageError> {
    let resolvers = live_resolvers(stub.database_id)?;
    if resolvers.is_empty() {
        // No open store can reach the extent this stub names.
        return Err(storage_error(StorageErrorKind::Unavailable));
    }
    for resolver in resolvers {
        let frame = match &resolver.location {
            ExtentLocation::Database(path) => crate::journal::read_retained_frame_with_media(
                resolver.media.as_ref(),
                path,
                stub.database_id,
                stub.placement,
                stub.generation,
            ),
            ExtentLocation::Backup(directory) => {
                crate::journal::read_retained_frame_from_with_media(
                    resolver.media.as_ref(),
                    &crate::backup::extent_artifacts(directory, stub.generation),
                    stub.database_id,
                    stub.placement,
                    stub.generation,
                )
            }
        };
        if let Ok(frame) = frame {
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
    for resolver in live_resolvers(stub.database_id)? {
        if let Some(value) = resolver.pending_segment(&stub.placement.frame_hash, &key) {
            return Ok(Cow::Owned(value));
        }
    }
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

/// Rows one hydration transaction rewrites.
const HYDRATION_BATCH_ROWS: usize = 256;

/// Rewrites every history stub in `database` as the exact value it resolves
/// to, for a copy that leaves its extents behind: a staged maintenance restore
/// becomes self-contained like a follower (ADR-0263 Amendment 1). The caller
/// holds a resolver for the extents. Batches are bounded and each commits
/// durably; a partly hydrated database is valid, since a stub and its value
/// are interchangeable representations (C3).
pub(crate) fn hydrate_history_stubs(database: &redb::Database) -> Result<(), StorageError> {
    use redb::ReadableDatabase as _;

    for (definition, resolve) in [
        (
            crate::layout::COMMITS,
            resolve_commits_value as fn(&[u8]) -> Result<Cow<'_, [u8]>, StorageError>,
        ),
        (
            crate::changelog_v3_activation::HISTORY,
            resolve_history_value,
        ),
    ] {
        let mut after: Option<Vec<u8>> = None;
        loop {
            let mut batch = Vec::new();
            {
                let read = database
                    .begin_read()
                    .map_err(crate::error::transaction_error)?;
                let table = match read.open_table(definition) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => break,
                    Err(error) => return Err(crate::error::table_error(error)),
                };
                let rows = match after.as_deref() {
                    Some(key) => table.range::<&[u8]>((
                        std::ops::Bound::Excluded(key),
                        std::ops::Bound::Unbounded,
                    )),
                    None => table.range::<&[u8]>(..),
                }
                .map_err(crate::error::precommit_storage_error)?;
                for row in rows {
                    let (key, value) = row.map_err(crate::error::precommit_storage_error)?;
                    after = Some(key.value().to_vec());
                    if HistoryStub::decode(value.value())?.is_some() {
                        let resolved = resolve(value.value())?.into_owned();
                        batch.push((key.value().to_vec(), value.value().to_vec(), resolved));
                        if batch.len() == HYDRATION_BATCH_ROWS {
                            break;
                        }
                    }
                }
            }
            if batch.is_empty() {
                break;
            }
            let mut write = database
                .begin_write()
                .map_err(crate::error::transaction_error)?;
            write.set_two_phase_commit(true);
            write
                .set_durability(redb::Durability::Immediate)
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
            {
                let mut table = write
                    .open_table(definition)
                    .map_err(crate::error::table_error)?;
                for (key, stub, resolved) in &batch {
                    let previous = table
                        .insert(key.as_slice(), resolved.as_slice())
                        .map_err(crate::error::precommit_storage_error)?;
                    // Nothing else writes a staged copy; the row must be the
                    // stub just resolved.
                    if previous.map(|previous| previous.value() == stub.as_slice()) != Some(true) {
                        return Err(storage_error(StorageErrorKind::CorruptData));
                    }
                }
            }
            write.commit().map_err(crate::error::commit_error)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // req: REC-005
    #[test]
    fn an_uncheckpointed_frame_resolves_from_memory_until_its_suffix_drops_it() {
        let database_id =
            DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x51; 10])
                .expect("database id");
        // No extent exists at this path: only the pending frame can answer.
        let resolver = HistoryResolver::register(
            database_id,
            Path::new("absent/pending.redb"),
            Arc::new(crate::media::RealJournalMedia),
        );
        let first = CommitSequence::new(7).expect("sequence");
        let key = crate::keys::encode_application_sequence_key(first);
        let segment = b"segment bytes the frame carries".to_vec();
        let mutations: Arc<[riffdb_storage_api::CompositeMutationV1]> = Arc::from(vec![
            riffdb_storage_api::CompositeMutationV1::put(
                riffdb_storage_api::CompositeTableV1::Commits,
                key.to_vec(),
                segment.clone(),
            )
            .expect("mutation"),
        ]);
        let frame_hash = [0x5a; 32];
        let stub = StubLocator {
            database_id,
            generation: 3,
            placement: FramePlacement {
                position: 8192,
                padded_len: 4096,
                frame_hash,
            },
        }
        .summary_stub(SegmentSummary {
            first_commit_sequence: Some(first),
            last_commit_sequence: Some(first),
            terminal_administration_sequence: None,
            command_count: 1,
            segment_digest: [0; 32],
        })
        .expect("stub");

        resolver.note_pending_frame(frame_hash, &mutations);
        assert_eq!(
            resolve_commits_value(&stub).expect("pending").as_ref(),
            segment.as_slice()
        );
        drop(mutations);
        assert_eq!(
            resolve_commits_value(&stub)
                .map(|_| ())
                .map_err(|error| error.kind()),
            Err(StorageErrorKind::CorruptData),
            "once the suffix drops the frame, only its extent can answer"
        );
    }
}
