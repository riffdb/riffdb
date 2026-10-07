//! Complete bounded census of the original sealed-generation prefix.
//! The diagnostic scrub pin keeps immutable files present while the census is
//! read. Its own pin is disclosed separately and confers no deletion authority.
use super::*;
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

const MAX_ENTRIES: usize = 8192;
const MAX_EXTENTS: usize = 4096;
const MAX_AGE: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Measurement {
    pub(crate) root: u64,
    pub(crate) incarnation: u64,
    pub(crate) watermark: u64,
    pub(crate) minimum_resume: u64,
    pub(crate) active_generation: u64,
    pub(crate) sealed_extents: u64,
    pub(crate) eligible_allocated_bytes: u64,
    pub(crate) protected_allocated_bytes: u64,
    pub(crate) reference_protected_allocated_bytes: u64,
    pub(crate) reader_protected_allocated_bytes: u64,
    pub(crate) reader_held: bool,
    pub(crate) capture_age_us: u64,
    pub(crate) pages: u64,
}

pub(crate) struct Probe {
    shared: Arc<SharedRedb>,
    pin: Option<ScrubPin>,
    read: Option<redb::ReadTransaction>,
    directory: Option<Box<dyn crate::media::JournalMediaDirectory>>,
    database_id: riffdb_types::DatabaseId,
    prefix: Vec<u8>,
    generations: BTreeSet<u64>,
    entries: usize,
    started: Instant,
    complete: bool,
    measurement: Measurement,
}

impl Probe {
    pub(crate) fn capture(shared: &Arc<SharedRedb>) -> Result<Option<Self>, StorageError> {
        if shared.is_follower_mode() || shared.mutation_gate.is_busy()? {
            return Ok(None);
        }
        let started = Instant::now();
        shared.retire_stale_current_read_root_for_extent()?;
        let pin = shared.pin_extent_scrub()?;
        let mut roots = shared.extent_roots.lock().map_err(|_| integrity())?;
        roots.retain(|root| root.strong_count() != 0);
        let root = shared.durable_root_publication.load(Ordering::Acquire);
        if root == 0 || !root.is_multiple_of(2) || shared.mutation_gate.is_busy()? {
            return Ok(None);
        }
        let media = shared.journal_media.as_ref();
        let Some(active) =
            journal::extent_generation_with_media(media, &journal::journal_path(&shared.path))
                .map_err(journal_io_error)?
        else {
            return Ok(None);
        };
        let read = shared.database.begin_read().map_err(transaction_error)?;
        let database_id = read_identity_from_read_transaction(&read)?;
        let history =
            crate::changelog_v3_roots::read_checkpoint_roots(&read)?.ok_or_else(integrity)?;
        let retained = crate::retention::load_watermark(&read)?;
        if retained.as_ref().is_some_and(|watermark| {
            watermark.history_incarnation() != history.lineage().history_incarnation()
        }) {
            return Err(integrity());
        }
        let reader_held = shared.extent_scrubs.load(Ordering::Acquire) > 1
            || roots
                .iter()
                .filter_map(std::sync::Weak::upgrade)
                .any(|captured| captured.identity() < root);
        if shared.durable_root_publication.load(Ordering::Acquire) != root
            || shared.mutation_gate.is_busy()?
            || journal::extent_generation_with_media(media, &journal::journal_path(&shared.path))
                .map_err(journal_io_error)?
                != Some(active)
        {
            return Ok(None);
        }
        drop(roots);
        let zero = journal::sealed_journal_path(&shared.path, 0);
        let name = zero.file_name().ok_or_else(integrity)?.as_encoded_bytes();
        let prefix = name
            .get(..name.len().checked_sub(20).ok_or_else(integrity)?)
            .ok_or_else(integrity)?
            .to_vec();
        let parent = shared
            .path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| std::path::Path::new("."));
        let directory = media
            .open_directory(parent)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        Ok(Some(Self {
            shared: Arc::clone(shared),
            pin: Some(pin),
            read: Some(read),
            directory: Some(directory),
            database_id,
            prefix,
            generations: BTreeSet::new(),
            entries: 0,
            started,
            complete: false,
            measurement: Measurement {
                root,
                incarnation: history.lineage().history_incarnation(),
                watermark: retained
                    .as_ref()
                    .map_or(0, |watermark| watermark.watermark_sequence()),
                minimum_resume: history.minimum_resume().sequence().get(),
                active_generation: active,
                reader_held,
                ..Measurement::default()
            },
        }))
    }

    fn retire(&mut self) {
        self.complete = true;
        self.pin = None;
        self.read = None;
        self.directory = None;
        self.generations.clear();
    }

    pub(crate) fn advance(&mut self) -> Result<Option<Measurement>, StorageError> {
        let result = self.advance_inner();
        if result.is_err() || matches!(result, Ok(Some(_))) {
            self.retire();
        }
        result
    }

    fn advance_inner(&mut self) -> Result<Option<Measurement>, StorageError> {
        if self.complete || self.started.elapsed() >= MAX_AGE {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        self.measurement.pages = self
            .measurement
            .pages
            .checked_add(1)
            .ok_or_else(integrity)?;
        for _ in 0..DIRECTORY_ENTRIES_PER_PASS {
            let next = self
                .directory
                .as_mut()
                .ok_or_else(integrity)?
                .next_entry()
                .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
            let Some(name) = next else {
                if self.started.elapsed() >= MAX_AGE {
                    return Err(storage_error(StorageErrorKind::LimitExceeded));
                }
                self.complete = true;
                self.measurement.capture_age_us =
                    crate::store::elapsed_us(self.started, Instant::now());
                return Ok(Some(self.measurement));
            };
            self.entries = self.entries.checked_add(1).ok_or_else(integrity)?;
            if self.entries > MAX_ENTRIES {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
            let Some(suffix) = name.as_encoded_bytes().strip_prefix(self.prefix.as_slice()) else {
                continue;
            };
            if suffix.len() != 20 || !suffix.iter().all(u8::is_ascii_digit) {
                return Err(integrity());
            }
            let generation = std::str::from_utf8(suffix)
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|value| *value > 0)
                .ok_or_else(integrity)?;
            if generation >= self.measurement.active_generation {
                continue;
            }
            if self.generations.len() >= MAX_EXTENTS {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
            if !self.generations.insert(generation) {
                return Err(integrity());
            }
            self.measure_file(generation)?;
            // Full validation of at most one bounded sealed extent per page.
            return Ok(None);
        }
        Ok(None)
    }

    fn measure_file(&mut self, generation: u64) -> Result<(), StorageError> {
        let media = self.shared.journal_media.as_ref();
        let path = journal::sealed_journal_path(&self.shared.path, generation);
        let mut file = media
            .open_read(&path)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        let before = allocated(&mut file)?;
        let footer = journal::read_sealed_extent_with_media(media, &path, self.database_id)
            .map_err(journal_io_error)?;
        if footer.generation != generation || allocated(&mut file)? != before {
            return Err(integrity());
        }
        let read = self.read.as_ref().ok_or_else(integrity)?;
        let references_released = eligible(read, &footer)?;
        if references_released {
            validate_absent_extent_references(read, &footer, generation)?;
        }
        let bytes = if references_released && !self.measurement.reader_held {
            &mut self.measurement.eligible_allocated_bytes
        } else {
            &mut self.measurement.protected_allocated_bytes
        };
        *bytes = bytes.checked_add(before.0).ok_or_else(integrity)?;
        let protected = if !references_released {
            Some(&mut self.measurement.reference_protected_allocated_bytes)
        } else if self.measurement.reader_held {
            Some(&mut self.measurement.reader_protected_allocated_bytes)
        } else {
            None
        };
        if let Some(protected) = protected {
            *protected = protected.checked_add(before.0).ok_or_else(integrity)?;
        }
        self.measurement.sealed_extents = self
            .measurement
            .sealed_extents
            .checked_add(1)
            .ok_or_else(integrity)?;
        Ok(())
    }
}

fn allocated(file: &mut crate::media::MediaFile) -> Result<(u64, u64), StorageError> {
    #[cfg(unix)]
    if let crate::media::MediaFile::Fs(file) = file {
        use std::os::unix::fs::MetadataExt;
        let metadata = file
            .metadata()
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        let bytes = metadata.blocks().checked_mul(512).ok_or_else(integrity)?;
        if !metadata.is_file() || bytes == 0 || metadata.len() == 0 {
            return Err(integrity());
        }
        return Ok((bytes, metadata.len()));
    }
    let _ = file;
    // A simulated logical length is not evidence of physical allocated bytes.
    Err(storage_error(StorageErrorKind::Unavailable))
}

#[derive(Default)]
pub(crate) struct Observer {
    probe: Option<Probe>,
    next_capture: Option<Instant>,
    #[cfg(test)]
    pub(crate) completed_for_test: u64,
}

impl Observer {
    pub(crate) fn retire(&mut self) {
        self.probe = None;
    }

    pub(crate) fn tick(&mut self, shared: &Arc<SharedRedb>) {
        if let Err(error) = self.advance(shared) {
            self.probe = None;
            eprintln!(
                "riffdb-retention-physical-bytes-refusal-v1\tkind={:?}",
                error.kind()
            );
        }
    }

    fn advance(&mut self, shared: &Arc<SharedRedb>) -> Result<(), StorageError> {
        let now = Instant::now();
        if self.probe.is_none() && self.next_capture.is_none_or(|next| now >= next) {
            self.probe = Probe::capture(shared)?;
            if self.probe.is_some() {
                self.next_capture = Some(now + Duration::from_secs(60));
            }
        }
        let Some(probe) = &mut self.probe else {
            return Ok(());
        };
        if let Some(measured) = probe.advance()? {
            // Release diagnostic protection before emitting completion. This
            // census never authorizes the independent native retirement plan.
            self.probe = None;
            #[cfg(test)]
            {
                self.completed_for_test += 1;
            }
            let Measurement {
                root,
                incarnation,
                watermark,
                minimum_resume,
                active_generation,
                sealed_extents,
                eligible_allocated_bytes,
                protected_allocated_bytes,
                reference_protected_allocated_bytes,
                reader_protected_allocated_bytes,
                reader_held,
                capture_age_us,
                pages,
            } = measured;
            let reader_held = u64::from(reader_held);
            eprintln!(
                "riffdb-retention-physical-bytes-v1\troot={root}\tincarnation={incarnation}\twatermark={watermark}\tminimum_resume={minimum_resume}\tactive_generation={active_generation}\tsealed_extents={sealed_extents}\teligible_allocated_bytes={eligible_allocated_bytes}\tprotected_allocated_bytes={protected_allocated_bytes}\treference_protected_allocated_bytes={reference_protected_allocated_bytes}\treader_protected_allocated_bytes={reader_protected_allocated_bytes}\treader_held={reader_held}\tcapture_age_us={capture_age_us}\tpages={pages}\tdiagnostic_pin_excluded=1"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn forced_refusal_for_test(
    shared: &Arc<SharedRedb>,
    mode: &str,
) -> Result<(StorageErrorKind, Probe), StorageError> {
    #[derive(Debug)]
    struct OversizedDirectory(usize);
    impl crate::media::JournalMediaDirectory for OversizedDirectory {
        fn next_entry(&mut self) -> std::io::Result<Option<std::ffi::OsString>> {
            if self.0 == 0 {
                return Ok(None);
            }
            self.0 -= 1;
            Ok(Some(std::ffi::OsString::from("unrelated-entry")))
        }
    }
    let mut probe = Probe::capture(shared)?.ok_or_else(integrity)?;
    match mode {
        "expiry" => {
            probe.started = Instant::now().checked_sub(MAX_AGE).ok_or_else(integrity)?;
        }
        "directory" => probe.directory = Some(Box::new(OversizedDirectory(MAX_ENTRIES + 1))),
        _ => return Err(integrity()),
    }
    for _ in 0..=MAX_ENTRIES {
        match probe.advance() {
            Err(error) => return Ok((error.kind(), probe)),
            Ok(None) => {}
            Ok(Some(_)) => return Err(integrity()),
        }
    }
    Err(integrity())
}
