//! Compiled recovery windows beyond engine caches; no public admission knob.
use super::*;
use riffdb_storage_api::{StorageError, StorageErrorKind};

// One original physical/decoded frame, without any suffix-sized owner. Each
// mutation has at least its 44-byte header. Use that weaker framing bound
// before semantic validation has proved nonempty canonical keys.
fn scanner_window() -> Option<usize> {
    let count = MAX_JOURNAL_FRAME_BYTES / MUTATION_HEADER_BYTES;
    let physical = extent_frame_bytes(MAX_JOURNAL_FRAME_BYTES)?;
    physical
        .checked_add(MAX_JOURNAL_FRAME_BYTES)?
        .checked_add(count.checked_mul(
            std::mem::size_of::<JournalMutation>().saturating_sub(MUTATION_HEADER_BYTES),
        )?)?
        .checked_add(256 * 1024)
}

pub(super) fn legacy_retained_limit() -> Result<usize, JournalIoError> {
    crate::startup::MAX_RECOVERY_OWNED_STATE_BYTES
        .checked_sub(scanner_window().ok_or(JournalIoError::Capacity)?)
        .ok_or(JournalIoError::Capacity)
}

pub(super) fn frame_owned_bound(frame: &JournalFrame) -> Result<usize, JournalIoError> {
    frame.mutations().iter().try_fold(
        std::mem::size_of::<JournalFrame>()
            .checked_add(
                frame
                    .mutations()
                    .len()
                    .checked_mul(std::mem::size_of::<JournalMutation>())
                    .ok_or(JournalIoError::Capacity)?,
            )
            .ok_or(JournalIoError::Capacity)?,
        |bytes, row| {
            bytes
                .checked_add(row.key().len())
                .and_then(|bytes| bytes.checked_add(row.value().map_or(0, <[u8]>::len)))
                .ok_or(JournalIoError::Capacity)
        },
    )
}

/// Recovery can resolve one prior/terminal stub while its original source and
/// folded receipt remain alive. Admit that nested window before the first fold
/// or write. The bound uses framing, not trusted semantic counts, and includes
/// decoded descriptors, one index vector, final payloads and canonical scratch.
/// This is a compiled internal-defect ceiling, never a public size selector.
pub(crate) fn admit_native_frame(frame: &JournalFrame) -> Result<(), StorageError> {
    let capacity = || crate::error::storage_error(StorageErrorKind::LimitExceeded);
    let payload = frame.mutations().iter().try_fold(0_usize, |bytes, row| {
        bytes
            .checked_add(row.key().len())
            .and_then(|bytes| bytes.checked_add(row.value().map_or(0, <[u8]>::len)))
            .ok_or_else(capacity)
    })?;
    admit_native_layout(frame.mutations().len(), payload).map_err(|_| capacity())
}

pub(super) fn admit_native_layout(count: usize, payload: usize) -> Result<(), JournalCodecError> {
    let capacity = || JournalCodecError::LimitExceeded;
    let source_count = count;
    let count = MAX_JOURNAL_FRAME_BYTES / MUTATION_HEADER_BYTES;
    let per_row = std::mem::size_of::<JournalMutation>()
        + std::mem::size_of::<usize>()
        + std::mem::size_of::<riffdb_storage_api::AuthoritativeMutationV3>();
    let nested_fold = 2 * MAX_JOURNAL_FRAME_BYTES
        + count * per_row.saturating_sub(2 * MUTATION_HEADER_BYTES)
        + 256 * 1024;
    let canonical_decode = riffdb_storage_api::MAX_CHANGELOG_FRAME_BYTES
        + riffdb_storage_api::MAX_CHANGELOG_FRAME_ENTRIES
            * std::mem::size_of::<riffdb_storage_api::AuthoritativeMutationV3>()
                .saturating_sub(MUTATION_HEADER_BYTES)
        + 256 * 1024;
    let nested = nested_fold
        .max(canonical_decode)
        .max(scanner_window().ok_or_else(capacity)?);
    let fold = source_count
        .checked_mul(
            std::mem::size_of::<riffdb_storage_api::AuthoritativeMutationV3>()
                + std::mem::size_of::<usize>(),
        )
        .and_then(|bytes| bytes.checked_add(payload))
        .ok_or_else(capacity)?;
    // A test/direct materializer can retain canonical bytes even for a recorded
    // binding; admission cannot assume that a physical locator will be supplied.
    let canonical = payload
        .checked_add(
            source_count
                .checked_mul(MUTATION_HEADER_BYTES)
                .ok_or_else(capacity)?,
        )
        .and_then(|bytes| bytes.checked_add(256))
        .ok_or_else(capacity)?;
    source_count
        .checked_mul(std::mem::size_of::<JournalMutation>())
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<JournalFrame>()))
        .and_then(|bytes| bytes.checked_add(payload))
        .and_then(|bytes| bytes.checked_add(fold))
        .and_then(|bytes| bytes.checked_add(canonical))
        .and_then(|bytes| bytes.checked_add(nested))
        .and_then(|bytes| bytes.checked_add(512 * 1024))
        .filter(|bytes| *bytes <= crate::startup::MAX_RECOVERY_OWNED_STATE_BYTES)
        .map(|_| ())
        .ok_or_else(capacity)
}

/// The complete physical checksum has already passed. Validate the bounded
/// logical framing and recorded hash before admitting any decoded allocations.
/// Retained stub reads have their separate one-window owner and do not call this.
pub(super) fn admit_physical_recovery(
    logical: &[u8],
    recorded: &[u8; HASH_BYTES],
) -> Result<(), JournalIoError> {
    let corrupt = || JournalIoError::Corrupt;
    if logical.len() < FRAME_HEADER_BYTES_V2 + FRAME_FOOTER_BYTES || logical[..8] != FRAME_MAGIC {
        return Err(corrupt());
    }
    match read_u16(logical, 8).map_err(|_| corrupt())? {
        FRAME_VERSION_V2 => return Ok(()),
        FRAME_VERSION_V3 | FRAME_VERSION_V4 | FRAME_VERSION_V5 => (),
        _ => return Err(corrupt()),
    }
    if logical.len() < FRAME_HEADER_BYTES + FRAME_FOOTER_BYTES {
        return Err(corrupt());
    }
    if FrameChangelogBinding::decode(&logical[FRAME_HEADER_BYTES_V2..FRAME_HEADER_BYTES])
        .map_err(|_| corrupt())?
        .is_none()
    {
        return Ok(());
    }
    let count =
        usize::try_from(read_u32(logical, 68).map_err(|_| corrupt())?).map_err(|_| corrupt())?;
    let payload =
        usize::try_from(read_u32(logical, 72).map_err(|_| corrupt())?).map_err(|_| corrupt())?;
    let footer = FRAME_HEADER_BYTES
        .checked_add(payload)
        .ok_or_else(corrupt)?;
    if footer.checked_add(FRAME_FOOTER_BYTES) != Some(logical.len())
        || logical[footer..footer + 8] != FOOTER_MAGIC
        || usize::try_from(read_u64(logical, footer + 8).map_err(|_| corrupt())?).ok()
            != Some(logical.len())
        || digest(&logical[..footer]) != *recorded
        || read_array::<HASH_BYTES>(logical, footer + 16).map_err(|_| corrupt())? != *recorded
    {
        return Err(corrupt());
    }
    let payload = payload
        .checked_sub(
            count
                .checked_mul(MUTATION_HEADER_BYTES)
                .ok_or_else(corrupt)?,
        )
        .ok_or_else(corrupt)?;
    admit_native_layout(count, payload).map_err(|_| JournalIoError::Capacity)
}

pub(super) fn verify_legacy_tail(
    database: &Database,
    frames: Vec<JournalFrame>,
) -> Result<(), JournalIoError> {
    let limit = legacy_retained_limit()?;
    let mut final_values = BTreeMap::<(u8, Vec<u8>), Option<Vec<u8>>>::new();
    // Reserve a complete internal B-tree node per distinct entry, matching
    // first-party ownership admission rather than guessing node occupancy.
    let node = 11 * (std::mem::size_of::<(u8, Vec<u8>)>() + std::mem::size_of::<Option<Vec<u8>>>())
        + 256
        + 2 * std::mem::align_of::<Vec<u8>>();
    let mut held = node;
    for mutation in frames.iter().flat_map(JournalFrame::mutations) {
        let key = (mutation.table() as u8, mutation.key().to_vec());
        let old = final_values.get(&key);
        let previous_payload = old.and_then(Option::as_ref).map_or(0, Vec::len);
        let next = held
            .checked_sub(previous_payload)
            .and_then(|bytes| bytes.checked_add(mutation.value().map_or(0, <[u8]>::len)))
            .and_then(|bytes| {
                bytes.checked_add(if old.is_none() {
                    node + mutation.key().len()
                } else {
                    0
                })
            })
            .filter(|bytes| *bytes <= limit)
            .ok_or(JournalIoError::Capacity)?;
        final_values.insert(key, mutation.value().map(<[u8]>::to_vec));
        held = next;
    }
    // The comparison window cannot retain both the entire original suffix and
    // a newly resolved extent. The exact final-value map owns all needed bytes.
    drop(frames);
    let transaction = database.begin_read().map_err(|_| JournalIoError::Io)?;
    for ((table, key), expected) in final_values {
        let table = JournalTable::decode(table).map_err(|_| JournalIoError::Corrupt)?;
        let mut actual =
            read_value_inner_bounded(&transaction, table, &key, MAX_JOURNAL_FRAME_BYTES)?;
        if table == JournalTable::Commits
            && let Some(stored) = actual.as_deref()
        {
            actual = Some(
                crate::history_stub::resolve_commits_value(stored)
                    .map_err(|_| JournalIoError::Corrupt)?
                    .into_owned(),
            );
        }
        if actual != expected {
            return Err(JournalIoError::Corrupt);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::ReadableTableMetadata;

    fn legacy_database(path: &Path) -> Database {
        let database = redb::Builder::new().create(path).unwrap();
        let tx = database.begin_write().unwrap();
        crate::layout::create_all_tables(&tx).unwrap();
        tx.commit().unwrap();
        database
    }

    fn legacy_frame(
        database_id: DatabaseId,
        ordinal: u64,
        previous: [u8; HASH_BYTES],
        payload: usize,
    ) -> JournalFrame {
        let sequence = CommitSequence::new(ordinal).unwrap();
        JournalFrame::new(
            database_id,
            sequence,
            sequence,
            1,
            previous,
            vec![
                JournalMutation::put(
                    JournalTable::Commits,
                    crate::keys::encode_application_sequence_key(sequence),
                    [0x41],
                )
                .unwrap(),
                JournalMutation::put(
                    JournalTable::Entities,
                    ordinal.to_be_bytes(),
                    vec![0x62; payload],
                )
                .unwrap(),
            ],
        )
        .unwrap()
    }

    // req: REC-001, REC-005
    #[test]
    fn legacy_recovery_refuses_an_over_budget_suffix_before_mutation() {
        let scope = crate::test_path::ScopedDirectory::new("legacy-recovery-capacity");
        let database = legacy_database(&scope.join("db.redb"));
        let database_id =
            DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x73; 10]).unwrap();
        let path = scope.join("source.journal");
        let header = JournalFileHeader::new(database_id, None, [0; HASH_BYTES]);
        let lane = JournalLane::open(&path, &header).unwrap();
        let mut previous = [0; HASH_BYTES];
        for ordinal in 1..=3 {
            let encoded = legacy_frame(database_id, ordinal, previous, 8 * 1024 * 1024)
                .encode()
                .unwrap();
            previous = encoded.frame_hash();
            lane.submit(encoded).unwrap().wait().unwrap();
        }
        drop(lane);
        let before = content_digest(&std::fs::read(&path).unwrap());
        assert_eq!(
            recover_journal_path(&database, &path, database_id),
            Err(JournalIoError::Capacity)
        );
        assert_eq!(content_digest(&std::fs::read(&path).unwrap()), before);
        assert_eq!(read_redb_frontier(&database).unwrap(), (None, None));
        let pin = database.begin_read().unwrap();
        assert!(pin.open_table(ENTITIES).unwrap().is_empty().unwrap());
        assert!(pin.open_table(COMMITS).unwrap().is_empty().unwrap());
    }

    // req: REC-001, REC-005
    #[test]
    fn legacy_recovery_admits_a_single_maximum_frame_and_exact_overlap() {
        let scope = crate::test_path::ScopedDirectory::new("legacy-recovery-maximum");
        let database = legacy_database(&scope.join("db.redb"));
        let database_id =
            DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x74; 10]).unwrap();
        let overhead = legacy_frame(database_id, 1, [0; HASH_BYTES], 1)
            .encode()
            .unwrap()
            .as_bytes()
            .len()
            - 1;
        let payload = MAX_JOURNAL_FRAME_BYTES - overhead;
        let encoded = legacy_frame(database_id, 1, [0; HASH_BYTES], payload)
            .encode()
            .unwrap();
        assert_eq!(encoded.as_bytes().len(), MAX_JOURNAL_FRAME_BYTES);
        let path = scope.join("source.journal");
        let header = JournalFileHeader::new(database_id, None, [0; HASH_BYTES]);
        let lane = JournalLane::open(&path, &header).unwrap();
        lane.submit(encoded).unwrap().wait().unwrap();
        drop(lane);
        // Simulate an engine checkpoint committed before journal reanchoring.
        scan_journal(&path, database_id, |frame| {
            replay_frames(&database, std::slice::from_ref(frame), &[None])
        })
        .unwrap();
        recover_journal_path(&database, &path, database_id).unwrap();
        recover_journal_path(&database, &path, database_id).unwrap();
        assert_eq!(
            read_redb_frontier(&database).unwrap().0,
            CommitSequence::new(1)
        );
        let pin = database.begin_read().unwrap();
        let entities = pin.open_table(ENTITIES).unwrap();
        let actual = entities
            .get(1_u64.to_be_bytes().as_slice())
            .unwrap()
            .unwrap();
        assert_eq!(actual.value().len(), payload);
        assert!(actual.value().iter().all(|byte| *byte == 0x62));
    }

    // req: REC-005, STO-030, STO-032
    #[test]
    fn source_cut_recovery_windows_fit_the_compiled_owned_state_ceiling() {
        let count = MAX_JOURNAL_FRAME_BYTES / MUTATION_HEADER_BYTES;
        // The indexed fold owns the original decoded frame, one index vector,
        // one exact-capacity net vector, and at most two copies of key/value
        // payloads. Fixed mutation headers consume those same encoded bytes.
        let per_row = std::mem::size_of::<JournalMutation>()
            + std::mem::size_of::<usize>()
            + std::mem::size_of::<riffdb_storage_api::AuthoritativeMutationV3>();
        let fold_window = 2 * MAX_JOURNAL_FRAME_BYTES
            + count * per_row.saturating_sub(2 * MUTATION_HEADER_BYTES)
            + 256 * 1024;
        let cut_and_fixed_roots = 4 * 64 * 1024;
        assert!(
            fold_window + cut_and_fixed_roots <= crate::startup::MAX_RECOVERY_OWNED_STATE_BYTES
        );
        assert!(
            scanner_window().unwrap() + cut_and_fixed_roots
                <= crate::startup::MAX_RECOVERY_OWNED_STATE_BYTES
        );
        assert!(
            riffdb_storage_api::MAX_CHANGELOG_FRAME_BYTES + cut_and_fixed_roots
                <= crate::startup::MAX_RECOVERY_OWNED_STATE_BYTES
        );
        assert!(legacy_retained_limit().unwrap() > 0);
    }
}
