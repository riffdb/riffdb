//! ADR-0263 Amendment 4: reclaim only fully unreferenced sealed extents.
//! Logical pruning and source receipt reclamation remain independent owners.
//! The reader reservation excludes captures, not the ordered writer. Older
//! captured roots and background scrubs conservatively keep all extents alive.

use super::*;
use crate::journal;

pub(super) const MAX_EXTENT_READ_ROOTS: usize = 4096;
const DIRECTORY_ENTRIES_PER_PASS: usize = 64;

#[path = "extent_reclamation/physical_bytes.rs"]
pub(crate) mod physical_bytes;

#[derive(Default)]
pub(crate) struct Cursor {
    directory: Option<Box<dyn crate::media::JournalMediaDirectory>>,
    retry: Option<u64>,
}

impl Cursor {
    fn candidate(
        &mut self,
        media: &dyn crate::media::JournalMedia,
        database_path: &std::path::Path,
        active: u64,
    ) -> Result<Option<(u64, PathBuf)>, StorageError> {
        let mut candidate = self.retry.take().map(|generation| {
            (
                generation,
                journal::sealed_journal_path(database_path, generation),
            )
        });
        if candidate.is_none() {
            if self.directory.is_none() {
                let parent = database_path
                    .parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                    .unwrap_or_else(|| std::path::Path::new("."));
                self.directory = Some(
                    media
                        .open_directory(parent)
                        .map_err(|_| storage_error(StorageErrorKind::Unavailable))?,
                );
            }
            let zero = journal::sealed_journal_path(database_path, 0);
            let name = zero.file_name().ok_or_else(integrity)?.as_encoded_bytes();
            let prefix = name
                .get(..name.len().checked_sub(20).ok_or_else(integrity)?)
                .ok_or_else(integrity)?;
            for _ in 0..DIRECTORY_ENTRIES_PER_PASS {
                let next = self
                    .directory
                    .as_mut()
                    .ok_or_else(integrity)?
                    .next_entry()
                    .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
                let Some(next) = next else {
                    self.directory = None;
                    break;
                };
                let Some(suffix) = next
                    .as_encoded_bytes()
                    .strip_prefix(prefix)
                    .filter(|suffix| suffix.len() == 20 && suffix.iter().all(u8::is_ascii_digit))
                else {
                    continue;
                };
                let Some(generation) = std::str::from_utf8(suffix)
                    .ok()
                    .and_then(|suffix| suffix.parse::<u64>().ok())
                    .filter(|generation| *generation > 0 && *generation < active)
                else {
                    continue;
                };
                candidate = Some((
                    generation,
                    journal::sealed_journal_path(database_path, generation),
                ));
                break;
            }
        }
        Ok(candidate)
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct Observation {
    // Present only after namespace retirement and parent-directory durability.
    pub(crate) retired_generation: Option<u64>,
    pub(crate) deleted: u64,
    pub(crate) recycled: u64,
    pub(crate) held_references: u64,
    pub(crate) held_readers: u64,
}

pub(crate) struct ScrubPin(Arc<SharedRedb>);

#[cfg(test)]
pub(crate) fn reserve_writer_for_test(
    shared: &SharedRedb,
) -> Result<crate::gate::ExclusiveLease, StorageError> {
    shared.mutation_gate.acquire()
}

impl Drop for ScrubPin {
    fn drop(&mut self) {
        self.0.extent_scrubs.fetch_sub(1, Ordering::AcqRel);
    }
}

impl SharedRedb {
    pub(crate) fn retire_stale_current_read_root_for_extent(&self) -> Result<(), StorageError> {
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
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn measure_physical_bytes_for_test(
        self: &Arc<Self>,
    ) -> Result<Option<physical_bytes::Measurement>, StorageError> {
        Ok(self
            .finished_physical_probe_for_test()?
            .map(|(measured, _)| measured))
    }

    #[cfg(test)]
    pub(crate) fn finished_physical_probe_for_test(
        self: &Arc<Self>,
    ) -> Result<Option<(physical_bytes::Measurement, physical_bytes::Probe)>, StorageError> {
        let Some(mut probe) = physical_bytes::Probe::capture(self)? else {
            return Ok(None);
        };
        for _ in 0..(8192 + 4096 + 1) {
            if let Some(measured) = probe.advance()? {
                return Ok(Some((measured, probe)));
            }
        }
        Err(storage_error(StorageErrorKind::LimitExceeded))
    }

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

    /// At most 64 actual directory entries, one bounded footer/frame validation and one
    /// whole-file retirement. Authoritatively referenced extents yield to the
    /// cursor. A global reader/writer/publication barrier retains the otherwise
    /// eligible candidate: it blocks every extent, and retry must not depend on
    /// scanning absent generation names. Restart needs no persisted progress.
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
        let candidate = cursor.candidate(media, &self.path, active)?;
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
        self.retire_stale_current_read_root_for_extent()?;
        let mut roots = self.extent_roots.lock().map_err(|_| integrity())?;
        roots.retain(|root| root.strong_count() != 0);
        if self.extent_scrubs.load(Ordering::Acquire) != 0 {
            cursor.retry = Some(generation);
            return Ok(Observation {
                held_readers: 1,
                ..Observation::default()
            });
        }
        let version = self.durable_root_publication.load(Ordering::Acquire);
        if !version.is_multiple_of(2) {
            cursor.retry = Some(generation);
            return Ok(Observation::default());
        }
        if roots
            .iter()
            .filter_map(std::sync::Weak::upgrade)
            .any(|root| root.identity() < version)
        {
            cursor.retry = Some(generation);
            return Ok(Observation {
                held_readers: 1,
                ..Observation::default()
            });
        }
        let read = self.database.begin_read().map_err(transaction_error)?;
        if self.durable_root_publication.load(Ordering::Acquire) != version {
            cursor.retry = Some(generation);
            return Ok(Observation::default());
        }
        if self.mutation_gate.is_busy()? {
            cursor.retry = Some(generation);
            return Ok(Observation {
                held_references: 1,
                ..Observation::default()
            });
        }
        if !eligible(&read, &footer)? {
            return Ok(Observation {
                held_references: 1,
                ..Observation::default()
            });
        }
        // The minimum resume is a source-controlled, cross-bound root. Check
        // the exact frame keys too; a lingering or malformed stub must refuse
        // rather than be hidden by an inconsistent floor.
        validate_absent_extent_references(&read, &footer, generation)?;
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
        observation.retired_generation = Some(generation);
        if let Some(generation) = observation.retired_generation {
            eprintln!(
                "riffdb-retention-extent-retired-v1\troot={version}\tgeneration={generation}\tdeleted={}\trecycled={}",
                observation.deleted, observation.recycled
            );
        }
        Ok(observation)
    }
}

fn validate_absent_extent_references(
    read: &redb::ReadTransaction,
    footer: &journal::SealedExtentFooter,
    generation: u64,
) -> Result<(), StorageError> {
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
    Ok(())
}

fn integrity() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

fn application_reference_survives(
    predecessor: Option<riffdb_types::CommitSequence>,
    covered: Option<riffdb_types::CommitSequence>,
    watermark: u64,
) -> bool {
    covered > predecessor && covered.is_some_and(|head| head.get() > watermark)
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
        application_reference_survives(
            entry.predecessor_sequence,
            entry.covered_sequence,
            watermark,
        )
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
    use std::collections::VecDeque;
    use std::ffi::OsString;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    #[derive(Debug)]
    struct CountingDirectory {
        entries: VecDeque<OsString>,
        calls: std::sync::Arc<AtomicUsize>,
        fail: bool,
    }

    impl crate::media::JournalMediaDirectory for CountingDirectory {
        fn next_entry(&mut self) -> std::io::Result<Option<OsString>> {
            self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            if self.fail {
                return Err(std::io::Error::other("isolated directory refusal"));
            }
            Ok(self.entries.pop_front())
        }
    }

    // req: STO-030, STO-031, END-008
    #[test]
    fn discovery_bounds_real_entries_and_never_walks_absent_generations() {
        let calls = std::sync::Arc::new(AtomicUsize::new(0));
        let mut entries = (0..128)
            .map(|index| OsString::from(format!("unrelated-{index}")))
            .collect::<VecDeque<_>>();
        let database = std::path::Path::new("/virtual/database.redb");
        let expected = crate::journal::sealed_journal_path(database, 1_000_000);
        entries.push_back(expected.file_name().unwrap().to_owned());
        let mut cursor = super::Cursor {
            directory: Some(Box::new(CountingDirectory {
                entries,
                calls: calls.clone(),
                fail: false,
            })),
            retry: None,
        };
        for count in [64, 128] {
            assert!(
                cursor
                    .candidate(&crate::media::RealJournalMedia, database, 2_000_000)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(calls.load(AtomicOrdering::SeqCst), count);
        }
        assert_eq!(
            cursor
                .candidate(&crate::media::RealJournalMedia, database, 2_000_000)
                .unwrap(),
            Some((1_000_000, expected))
        );
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 129);
        cursor.retry = Some(1_000_000);
        assert!(
            cursor
                .candidate(&crate::media::RealJournalMedia, database, 2_000_000)
                .unwrap()
                .is_some()
        );
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 129);
    }

    // req: STO-030, STO-031, END-008
    #[test]
    fn directory_discovery_errors_refuse_instead_of_becoming_exact_end() {
        let mut cursor = super::Cursor {
            directory: Some(Box::new(CountingDirectory {
                entries: VecDeque::new(),
                calls: Default::default(),
                fail: true,
            })),
            retry: None,
        };
        let error = cursor
            .candidate(
                &crate::media::RealJournalMedia,
                std::path::Path::new("/virtual/database.redb"),
                10,
            )
            .unwrap_err();
        assert_eq!(
            error.kind(),
            riffdb_storage_api::StorageErrorKind::Unavailable
        );
    }
    use super::*;

    // req: STO-030, STO-032
    #[test]
    fn administration_only_extent_does_not_inherit_unpruned_command_references() {
        use riffdb_types::CommitSequence;
        let live_head = CommitSequence::new(3);
        assert!(
            !application_reference_survives(live_head, live_head, 2),
            "an audit or prune frame covers the current head without owning a command"
        );
        assert!(
            application_reference_survives(CommitSequence::new(2), live_head, 2),
            "the actual command transition still holds the extent"
        );
        assert!(
            !application_reference_survives(CommitSequence::new(2), live_head, 3),
            "a durably pruned command releases only its application reference"
        );
        assert!(!application_reference_survives(None, None, 0));
    }

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
