//! ADR-0300: unfinished private frame inputs under the actual original owner.
use super::*;
use crate::media::OwnedRestoreReads;
use std::sync::atomic::AtomicUsize;

const MAX_FRAMES: usize = 256;
const MAX_PHYSICAL_BYTES: usize = 64 * 1024 * 1024;

struct ReadState {
    backup: Arc<OwnedBackupHistory>,
    stage: Arc<PinnedDirectory>,
    database: PathBuf,
    stage_observation: DirectoryObservation,
    thread: std::thread::ThreadId,
    frames: AtomicUsize,
    bytes: AtomicUsize,
    active: AtomicBool,
    finished: AtomicBool,
}

impl ReadState {
    fn check(&self) -> io::Result<()> {
        self.backup
            .checked(|| {
                if self.thread != std::thread::current().id()
                    || self.finished.load(Ordering::Acquire)
                {
                    return Err(storage_error(StorageErrorKind::Unavailable));
                }
                Ok(())
            })
            .map_err(unavailable)
    }

    fn verify_custody(&self) -> io::Result<()> {
        self.backup
            .checked(|| {
                self.backup.verify()?;
                self.stage.verify()?;
                let parent = self.database.parent().ok_or_else(corrupt)?;
                if directory_observation(parent)? != self.stage_observation {
                    return Err(corrupt());
                }
                let zero = crate::journal::sealed_journal_path(&self.database, 0);
                let name = zero.file_name().ok_or_else(corrupt)?.as_encoded_bytes();
                let prefix = &name[..name.len() - 20];
                // Stream the bounded namespace; no population-sized names vector.
                for (index, entry) in fs::read_dir(parent).map_err(io_unavailable)?.enumerate() {
                    if index >= riffdb_storage_api::MAX_BACKUP_ARTIFACT_CHECKSUMS {
                        return Err(limit_exceeded());
                    }
                    if entry
                        .map_err(io_unavailable)?
                        .file_name()
                        .as_encoded_bytes()
                        .starts_with(prefix)
                    {
                        return Err(corrupt());
                    }
                }
                self.stage.verify()?;
                if directory_observation(parent)? != self.stage_observation {
                    return Err(corrupt());
                }
                Ok(())
            })
            .map_err(unavailable)
    }

    fn begin_frame(&self, physical_bytes: usize) -> io::Result<()> {
        self.check()?;
        if physical_bytes == 0
            || physical_bytes > MAX_PHYSICAL_BYTES
            || self.active.load(Ordering::Acquire)
        {
            self.backup.invalidate();
            return Err(refused());
        }
        let frames = self.frames.load(Ordering::Acquire);
        let bytes = self.bytes.load(Ordering::Acquire);
        if frames == MAX_FRAMES || bytes > MAX_PHYSICAL_BYTES - physical_bytes {
            self.verify_custody()?;
            self.frames.store(0, Ordering::Release);
            self.bytes.store(0, Ordering::Release);
        }
        if self.frames.load(Ordering::Acquire) == 0 {
            self.verify_custody()?;
        }
        self.frames.fetch_add(1, Ordering::AcqRel);
        self.bytes.fetch_add(physical_bytes, Ordering::AcqRel);
        self.active.store(true, Ordering::Release);
        Ok(())
    }

    fn end_frame(&self) -> io::Result<()> {
        self.check()?;
        if !self.active.swap(false, Ordering::AcqRel) {
            self.backup.invalidate();
            return Err(refused());
        }
        Ok(())
    }
}

pub(super) struct PrivateReads {
    media: Arc<IntervalMedia>,
}

impl PrivateReads {
    pub(super) fn acquire(source: &BorrowedHistoryMedia) -> io::Result<Box<dyn OwnedRestoreReads>> {
        source.backup.verify().map_err(unavailable)?;
        source.stage.verify().map_err(unavailable)?;
        let state = Arc::new(ReadState {
            backup: Arc::clone(&source.backup),
            stage: Arc::clone(&source.stage),
            database: source.database.clone(),
            stage_observation: directory_observation(source.database.parent().ok_or_else(refused)?)
                .map_err(unavailable)?,
            thread: std::thread::current().id(),
            frames: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            active: AtomicBool::new(false),
            finished: AtomicBool::new(false),
        });
        state.verify_custody()?;
        Ok(Box::new(Self {
            media: Arc::new(IntervalMedia { state }),
        }))
    }
}

impl OwnedRestoreReads for PrivateReads {
    fn media(&self) -> Arc<dyn JournalMedia> {
        Arc::clone(&self.media) as _
    }
    fn finish(&mut self) -> io::Result<()> {
        let state = &self.media.state;
        state.check()?;
        if state.active.load(Ordering::Acquire) {
            state.backup.invalidate();
            return Err(refused());
        }
        state.verify_custody()?;
        state.finished.store(true, Ordering::Release);
        Ok(())
    }
    fn verify_finished(&self) -> io::Result<()> {
        let state = &self.media.state;
        if state.thread != std::thread::current().id()
            || !state.finished.load(Ordering::Acquire)
            || state.active.load(Ordering::Acquire)
        {
            state.backup.invalidate();
            return Err(refused());
        }
        state.verify_custody()
    }
}

impl Drop for PrivateReads {
    fn drop(&mut self) {
        if !self.media.state.finished.load(Ordering::Acquire) {
            self.media.state.backup.invalidate();
        }
    }
}

struct IntervalMedia {
    state: Arc<ReadState>,
}
impl std::fmt::Debug for IntervalMedia {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PrivateRestoreReadInterval([REDACTED])")
    }
}

impl IntervalMedia {
    fn source(&self, path: &Path) -> io::Result<MediaFile> {
        let state = &self.state;
        let source = BorrowedHistoryMedia::new(
            Arc::clone(&state.backup),
            state.database.clone(),
            Arc::clone(&state.stage),
        );
        if let Some(original) = source.original(path) {
            let index = OriginalMedia(Arc::clone(&state.backup)).index(&original)?;
            return Ok(MediaFile::Port(Box::new(OriginalCursor {
                owner: Arc::clone(&state.backup),
                index,
                offset: 0,
                read_span: ReadSpan::CompleteArtifact,
                finished: false,
                stage: None,
            })));
        }
        if source.canonical_absent_sealed(path).is_some() {
            return Err(io::ErrorKind::NotFound.into());
        }
        if path != crate::journal::journal_path(&state.database) {
            return Err(refused());
        }
        RealJournalMedia.open_read(path)
    }
    fn refuse<T>(&self) -> io::Result<T> {
        self.state.backup.invalidate();
        Err(refused())
    }
}

impl JournalMedia for IntervalMedia {
    fn open_owned_retained_frame_read(
        &self,
        path: &Path,
        physical_bytes: usize,
    ) -> io::Result<MediaFile> {
        self.state.begin_frame(physical_bytes)?;
        match self.source(path) {
            Ok(file) => Ok(MediaFile::Port(Box::new(IntervalFile {
                file,
                state: Arc::clone(&self.state),
                remaining: physical_bytes,
                finished: false,
            }))),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.state.end_frame()?;
                Err(error)
            }
            Err(error) => {
                self.state.backup.invalidate();
                Err(error)
            }
        }
    }
    fn open_directory(&self, _: &Path) -> io::Result<Box<dyn JournalMediaDirectory>> {
        self.refuse()
    }
    fn try_exists(&self, _: &Path) -> io::Result<bool> {
        self.refuse()
    }
    fn metadata(&self, _: &Path) -> io::Result<MediaFileMetadata> {
        self.refuse()
    }
    fn open_read(&self, _: &Path) -> io::Result<MediaFile> {
        self.refuse()
    }
    fn open_read_write(&self, _: &Path) -> io::Result<MediaFile> {
        self.refuse()
    }
    fn create_new_read_write(&self, _: &Path) -> io::Result<MediaFile> {
        self.refuse()
    }
    fn create_new_write_only(&self, _: &Path) -> io::Result<MediaFile> {
        self.refuse()
    }
    fn rename(&self, _: &Path, _: &Path) -> io::Result<()> {
        self.refuse()
    }
    fn remove_file(&self, _: &Path) -> io::Result<()> {
        self.refuse()
    }
    fn sync_parent_data(&self, _: &Path) -> io::Result<()> {
        self.refuse()
    }
    fn sync_parent_all(&self, _: &Path) -> io::Result<()> {
        self.refuse()
    }
}

struct IntervalFile {
    file: MediaFile,
    state: Arc<ReadState>,
    remaining: usize,
    finished: bool,
}
impl std::fmt::Debug for IntervalFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PrivateRestoreFrameInput([REDACTED])")
    }
}
impl IntervalFile {
    fn reserve(&mut self, bytes: usize) -> io::Result<()> {
        self.state.check()?;
        if self.finished || bytes > self.remaining {
            self.state.backup.invalidate();
            return Err(refused());
        }
        self.remaining -= bytes;
        Ok(())
    }
    fn observe<T>(&self, result: io::Result<T>) -> io::Result<T> {
        if result.is_err() {
            self.state.backup.invalidate();
        }
        self.state.check()?;
        result
    }
}
impl JournalMediaFile for IntervalFile {
    fn len(&mut self) -> io::Result<u64> {
        self.reserve(0)?;
        let result = self.file.len();
        self.observe(result)
    }
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        self.reserve(out.len())?;
        let result = self.file.read(out);
        self.observe(result)
    }
    fn read_exact(&mut self, out: &mut [u8]) -> io::Result<()> {
        self.reserve(out.len())?;
        let result = self.file.read_exact(out);
        self.observe(result)
    }
    fn read_exact_at(&mut self, out: &mut [u8], offset: usize) -> io::Result<()> {
        self.reserve(out.len())?;
        let result = self.file.read_exact_at(out, offset);
        self.observe(result)
    }
    fn finish_retained_frame_read(&mut self) -> io::Result<()> {
        self.reserve(0)?;
        let result = self.file.finish_retained_frame_read();
        self.observe(result)?;
        self.state.end_frame()?;
        self.finished = true;
        Ok(())
    }
    fn write_all(&mut self, _: &[u8]) -> io::Result<()> {
        self.state.backup.invalidate();
        Err(refused())
    }
    fn write_all_at(&mut self, _: &[u8], _: usize) -> io::Result<()> {
        self.write_all(&[])
    }
    fn sync_data(&mut self) -> io::Result<()> {
        self.write_all(&[])
    }
    fn sync_all(&mut self) -> io::Result<()> {
        self.write_all(&[])
    }
    fn set_len(&mut self, _: u64) -> io::Result<()> {
        self.write_all(&[])
    }
}
impl Drop for IntervalFile {
    fn drop(&mut self) {
        if !self.finished {
            self.state.backup.invalidate();
        }
    }
}
