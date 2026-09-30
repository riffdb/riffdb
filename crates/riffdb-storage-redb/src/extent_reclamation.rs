//! ADR-0263 Amendment 4: reclaim only fully unreferenced sealed extents.
//! Logical pruning and source receipt reclamation remain independent owners.
//! The reader reservation excludes captures, not the ordered writer. Older
//! captured roots and background scrubs conservatively keep all extents alive.

use super::*;
use crate::journal;

pub(super) const MAX_EXTENT_READ_ROOTS: usize = 4096;
const GENERATIONS_PER_PASS: usize = 64;

#[derive(Clone, Copy, Default)]
pub(crate) struct Observation {
    pub(crate) deleted: u64,
    pub(crate) recycled: u64,
    pub(crate) held_references: u64,
    pub(crate) held_readers: u64,
}

pub(crate) struct ScrubPin(Arc<SharedRedb>);

impl Drop for ScrubPin {
    fn drop(&mut self) {
        self.0.extent_scrubs.fetch_sub(1, Ordering::AcqRel);
    }
}

impl SharedRedb {
    pub(crate) fn pin_extent_scrub(self: &Arc<Self>) -> Result<ScrubPin, StorageError> {
        let _roots = self.extent_roots.lock().map_err(|_| integrity())?;
        self.extent_scrubs.fetch_add(1, Ordering::AcqRel);
        Ok(ScrubPin(Arc::clone(self)))
    }

    pub(crate) fn segment_extent_is_sealed(&self, value: &[u8]) -> Result<bool, StorageError> {
        let Some(stub) = crate::history_stub::HistoryStub::decode(value)? else {
            return Ok(true);
        };
        if stub.kind != crate::history_stub::StubKind::Segment {
            return Err(integrity());
        }
        self.journal_media
            .try_exists(&journal::sealed_journal_path(&self.path, stub.generation))
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))
    }

    /// At most 64 name probes, one bounded footer/frame validation and one
    /// whole-file retirement. A process-local cursor makes held extents fair;
    /// restart needs no persisted progress because eligibility is authoritative.
    pub(crate) fn reclaim_one_extent(&self) -> Result<Observation, StorageError> {
        if self.is_follower_mode() {
            return Ok(Observation::default());
        }
        let mut cursor = self.extent_cursor.lock().map_err(|_| integrity())?;
        let media = self.journal_media.as_ref();
        let Some(active) =
            journal::extent_generation_with_media(media, &journal::journal_path(&self.path))
                .map_err(journal_io_error)?
        else {
            return Ok(Observation::default());
        };
        let mut candidate = None;
        for _ in 0..GENERATIONS_PER_PASS {
            if *cursor >= active {
                *cursor = 1;
                break;
            }
            let generation = *cursor;
            *cursor = cursor.checked_add(1).ok_or_else(integrity)?;
            let path = journal::sealed_journal_path(&self.path, generation);
            if media
                .try_exists(&path)
                .map_err(|_| storage_error(StorageErrorKind::Unavailable))?
            {
                candidate = Some((generation, path));
                break;
            }
        }
        let Some((generation, path)) = candidate else {
            return Ok(Observation::default());
        };
        let database_id = read_identity_from_read_transaction(
            &self.database.begin_read().map_err(transaction_error)?,
        )?;
        // Full validation is bounded by one extent and runs outside the reader
        // reservation as well as the writer gate. It never licenses deletion
        // without the subsequent authoritative-reference and reader checks.
        let index = journal::read_sealed_index_with_media(media, &path, database_id)
            .map_err(journal_io_error)?;
        {
            let read = self.database.begin_read().map_err(transaction_error)?;
            if !eligible(&read, &index)? {
                return Ok(Observation {
                    held_references: 1,
                    ..Observation::default()
                });
            }
        }
        let footer = journal::read_sealed_extent_with_media(media, &path, database_id)
            .map_err(journal_io_error)?;
        if footer.generation != generation || footer != index {
            return Err(integrity());
        }
        // A cached frontier-free root from an earlier durable publication can
        // never be handed out again. Retire that internal owner before checking
        // external pins; acquire cache locks before the capture reservation.
        {
            let mut cached = self.current_read_root.write().map_err(|_| integrity())?;
            let current = self.durable_root_publication.load(Ordering::Acquire);
            if current.is_multiple_of(2)
                && cached
                    .as_ref()
                    .is_some_and(|(version, _)| *version != current)
            {
                *cached = None;
                self.current_read_root_live.store(false, Ordering::Release);
            }
        }
        let mut roots = self.extent_roots.lock().map_err(|_| integrity())?;
        roots.retain(|root| root.strong_count() != 0);
        if self.extent_scrubs.load(Ordering::Acquire) != 0 {
            return Ok(Observation {
                held_readers: 1,
                ..Observation::default()
            });
        }
        let version = self.durable_root_publication.load(Ordering::Acquire);
        if !version.is_multiple_of(2) {
            return Ok(Observation::default());
        }
        if roots
            .iter()
            .filter_map(std::sync::Weak::upgrade)
            .any(|root| root.identity() < version)
        {
            return Ok(Observation {
                held_readers: 1,
                ..Observation::default()
            });
        }
        let read = self.database.begin_read().map_err(transaction_error)?;
        if self.durable_root_publication.load(Ordering::Acquire) != version {
            return Ok(Observation::default());
        }
        if self.mutation_gate.is_busy()? || !eligible(&read, &footer)? {
            return Ok(Observation {
                held_references: 1,
                ..Observation::default()
            });
        }
        // The minimum resume is a source-controlled, cross-bound root. Check
        // the exact frame keys too; a lingering or malformed stub must refuse
        // rather than be hidden by an inconsistent floor.
        let commits = read.open_table(COMMITS).map_err(table_error)?;
        let receipts = read
            .open_table(crate::changelog_v3_activation::HISTORY)
            .map_err(table_error)?;
        for entry in &footer.entries {
            if let Some(binding) = entry.changelog
                && receipts
                    .get(binding.assigned_sequence().get().to_be_bytes().as_slice())
                    .map_err(precommit_storage_error)?
                    .is_some()
            {
                return Err(integrity());
            }
            if entry.covered_sequence > entry.predecessor_sequence {
                let first = entry
                    .predecessor_sequence
                    .map_or(1, |sequence| sequence.get().saturating_add(1));
                let last = entry.covered_sequence.ok_or_else(integrity)?.get();
                for row in commits
                    .range::<&[u8]>(first.to_be_bytes().as_slice()..=last.to_be_bytes().as_slice())
                    .map_err(precommit_storage_error)?
                {
                    let (_, value) = row.map_err(precommit_storage_error)?;
                    if let Some(stub) = crate::history_stub::HistoryStub::decode(value.value())?
                        && stub.generation == generation
                    {
                        return Err(integrity());
                    }
                }
            }
        }
        drop(receipts);
        drop(commits);
        drop(read);
        let _spare = self.journal_spare.lock().map_err(|_| integrity())?;
        let recycled = journal::recycled_journal_path(&self.path);
        self.before_test_commit(RedbTestOperation::RetentionExtentRetire)?;
        let mut observation = Observation::default();
        if media
            .try_exists(&recycled)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?
        {
            observation.deleted = 1;
            media
                .remove_file(&path)
                .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        } else {
            observation.recycled = 1;
            media
                .rename(&path, &recycled)
                .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        }
        // Namespace removal is complete and references cannot be resurrected.
        // Release both reservations before syncing the directory: foreground
        // capture and spare rotation must never wait on reclamation's sync.
        drop(_spare);
        drop(roots);
        self.after_test_commit(RedbTestOperation::RetentionExtentRetire)?;
        self.before_test_commit(RedbTestOperation::RetentionExtentDirectory)?;
        journal::sync_parent_directory_with_media(media, &path).map_err(journal_io_error)?;
        self.after_test_commit(RedbTestOperation::RetentionExtentDirectory)?;
        Ok(observation)
    }
}

fn integrity() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

fn eligible(
    read: &redb::ReadTransaction,
    footer: &journal::SealedExtentFooter,
) -> Result<bool, StorageError> {
    let retained_watermark = crate::retention::load_watermark(read)?;
    let watermark = retained_watermark
        .as_ref()
        .map_or(0, |watermark| watermark.watermark_sequence());
    if footer.entries.iter().any(|entry| {
        entry
            .covered_sequence
            .is_some_and(|head| head.get() > watermark)
    }) {
        return Ok(false);
    }
    let history = crate::changelog_v3_roots::read_checkpoint_roots(read)?.ok_or_else(integrity)?;
    if retained_watermark.is_some_and(|watermark| {
        watermark.history_incarnation() != history.lineage().history_incarnation()
    }) {
        return Err(integrity());
    }
    for entry in &footer.entries {
        let binding = entry.changelog.ok_or_else(integrity)?;
        if binding.assigned_sequence().get() >= history.minimum_resume().sequence().get() {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    // req: STO-032
    #[test]
    fn reader_capture_waits_for_the_extent_retirement_reservation() {
        let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
        let shared = Arc::clone(&ports.shared);
        let reservation = shared.extent_roots.lock().unwrap();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (captured_tx, captured_rx) = std::sync::mpsc::channel();
        let reader = Arc::clone(&shared);
        let worker = std::thread::spawn(move || {
            ready_tx.send(()).unwrap();
            captured_tx
                .send(reader.capture_checkpoint_root().unwrap())
                .unwrap();
        });
        ready_rx.recv().unwrap();
        assert!(matches!(
            captured_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        drop(reservation);
        let root = captured_rx.recv().unwrap();
        worker.join().unwrap();
        assert_eq!(
            root.identity(),
            shared.durable_root_publication.load(Ordering::Acquire)
        );
        assert!(
            shared
                .extent_roots
                .lock()
                .unwrap()
                .iter()
                .any(|pin| pin.upgrade().is_some_and(|pin| Arc::ptr_eq(&pin, &root)))
        );
    }

    // req: STO-032
    #[test]
    fn extent_reader_tracking_is_bounded_and_releases_dropped_roots() {
        let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
        let mut roots = Vec::new();
        for _ in 0..MAX_EXTENT_READ_ROOTS {
            roots.push(ports.shared.capture_checkpoint_root().unwrap());
        }
        assert_eq!(
            ports.shared.capture_checkpoint_root().err().unwrap().kind(),
            StorageErrorKind::LimitExceeded
        );
        roots.clear();
        assert!(ports.shared.capture_checkpoint_root().is_ok());
        assert_eq!(ports.shared.extent_roots.lock().unwrap().len(), 1);
    }
}
