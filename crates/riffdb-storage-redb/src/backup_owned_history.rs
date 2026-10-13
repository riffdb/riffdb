//! ADR-0299: operation-local immutable backup descriptors, never an engine.
use super::*;
use crate::maintenance::PinnedDirectory;
use crate::media::{
    JournalMedia, JournalMediaDirectory, JournalMediaFile, MediaFile, MediaFileMetadata,
    RealJournalMedia,
};
use std::io;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct Artifact {
    name: OsString,
    file: Arc<File>,
    checksum: BackupIntegrityChecksumV1,
    continuity: ArtifactContinuity,
    generation: Option<u64>,
}

pub(crate) struct OwnedBackupHistory {
    directory: Arc<PinnedDirectory>,
    path: PathBuf,
    directory_observation: DirectoryObservation,
    manifest_file: File,
    manifest_continuity: ArtifactContinuity,
    manifest_digest: BackupIntegrityChecksumV1,
    artifacts: Vec<Artifact>,
    cancelled: Arc<AtomicBool>,
    invalid: AtomicBool,
}

impl std::fmt::Debug for OwnedBackupHistory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedBackupHistory")
            .field("artifacts", &self.artifacts.len())
            .finish_non_exhaustive()
    }
}

impl OwnedBackupHistory {
    pub(crate) fn acquire(
        path: &Path,
        manifest: &OfflineBackupManifestV1,
        cancelled: Arc<AtomicBool>,
    ) -> Result<Arc<Self>, StorageError> {
        validate_backup_inventory(path)?;
        validate_manifest_inventory(path, manifest)?;
        let directory = Arc::new(PinnedDirectory::open(path)?);
        let manifest_file = directory
            .open_file(OsStr::new(MANIFEST_FILE_NAME))?
            .into_std();
        let manifest_continuity =
            ArtifactContinuity::read(&manifest_file)?.ok_or_else(incompatible)?;
        let manifest_bytes = read_bounded_file(&path.join(MANIFEST_FILE_NAME), MANIFEST_MAX_BYTES)?;
        if decode_manifest(&manifest_bytes)? != *manifest
            || manifest_file.metadata().map_err(io_unavailable)?.nlink() != 1
        {
            return Err(corrupt());
        }
        let mut selected = vec![
            (
                OsString::from(DATABASE_ARTIFACT_FILE_NAME),
                manifest_checksum(manifest)?,
                None,
            ),
            (
                OsString::from(JOURNAL_ARTIFACT_FILE_NAME),
                manifest_journal_checksum(manifest)?.ok_or_else(corrupt)?,
                None,
            ),
            (
                OsString::from(FORMAT_MARKER_ARTIFACT_FILE_NAME),
                manifest_format_marker_checksum(manifest)?.ok_or_else(corrupt)?,
                None,
            ),
        ];
        for (ordinal, path) in sealed_artifact_ordinals(&sealed_artifacts_in(path)?)? {
            let name = path.file_name().ok_or_else(corrupt)?;
            selected.push((
                name.to_os_string(),
                manifest_artifact_checksum(manifest, ordinal)?,
                Some(sealed_artifact_generation(name).ok_or_else(corrupt)?),
            ));
        }
        // One original slot replaces each copied extent slot. Include the
        // manifest, three original fixed artifacts and three private artifacts
        // in the existing bounded artifact capacity before opening any extent.
        if selected
            .len()
            .checked_add(4)
            .is_none_or(|n| n > riffdb_storage_api::MAX_BACKUP_ARTIFACT_CHECKSUMS)
        {
            return Err(limit_exceeded());
        }
        let mut artifacts = Vec::with_capacity(selected.len());
        for (name, checksum, generation) in selected {
            let file = Arc::new(directory.open_file(&name)?.into_std());
            let continuity = ArtifactContinuity::read(&file)?.ok_or_else(incompatible)?;
            artifacts.push(Artifact {
                name,
                file,
                checksum,
                continuity,
                generation,
            });
        }
        let owner = Arc::new(Self {
            directory,
            path: path.to_path_buf(),
            directory_observation: directory_observation(path)?,
            manifest_file,
            manifest_continuity,
            manifest_digest: BackupIntegrityChecksumV1::new(
                Sha256::digest(&manifest_bytes).to_vec(),
            )
            .map_err(value_error)?,
            artifacts,
            cancelled,
            invalid: AtomicBool::new(false),
        });
        owner.verify()?;
        owner.verify_full()?;
        // Frame/footer parsing uses the same closed, positional descriptors.
        let media = OriginalMedia(Arc::clone(&owner));
        owner.verify_artifact(1)?;
        let (header, tail) = crate::journal::scan_journal_with_media(
            &media,
            &path.join(JOURNAL_ARTIFACT_FILE_NAME),
            manifest.database_id(),
            |_| Ok(()),
        )
        .map_err(backup_journal_error)?
        .ok_or_else(corrupt)?;
        owner.verify_artifact(1)?;
        if header.checkpoint_sequence() != manifest.last_commit_sequence()
            || tail.last_sequence != manifest.last_commit_sequence()
            || tail.incomplete_tail
            || tail.transition_count != 0
        {
            return Err(corrupt());
        }
        let marker = &owner.artifacts[2];
        let mut bytes = [0; riffdb_storage_api::DURABLE_FORMAT_MARKER_BYTES];
        PositionalReader {
            file: &marker.file,
            offset: 0,
        }
        .read_exact(&mut bytes)
        .map_err(io_unavailable)?;
        validate_manifest_format_marker_bytes(&bytes, manifest)?;
        for (index, artifact) in owner.artifacts.iter().enumerate() {
            if let Some(generation) = artifact.generation {
                owner.verify_artifact(index)?;
                let footer = crate::journal::read_sealed_extent_with_media(
                    &media,
                    &path.join(&artifact.name),
                    manifest.database_id(),
                )
                .map_err(backup_journal_error)?;
                #[cfg(test)]
                COMPLETE_ARTIFACT_READ_HOOK.with(|hook| {
                    if let Some(action) = hook.borrow_mut().take() {
                        action();
                    }
                });
                owner.verify_artifact(index)?;
                if footer.generation != generation {
                    owner.invalidate();
                    return Err(corrupt());
                }
            }
        }
        owner.verify()?;
        Ok(owner)
    }

    fn invalidate(&self) {
        self.invalid.store(true, Ordering::Release);
    }

    fn checked<T>(&self, f: impl FnOnce() -> Result<T, StorageError>) -> Result<T, StorageError> {
        let result =
            if self.invalid.load(Ordering::Acquire) || self.cancelled.load(Ordering::Acquire) {
                Err(storage_error(StorageErrorKind::Unavailable))
            } else {
                f()
            };
        if result.is_err() {
            self.invalidate();
        }
        result
    }

    fn verify_artifact(&self, index: usize) -> Result<(), StorageError> {
        #[cfg(test)]
        ORIGINAL_ARTIFACT_READ_CHECKS.with(|count| count.set(count.get() + 1));
        self.checked(|| {
            self.directory.verify()?;
            if directory_observation(&self.path)? != self.directory_observation {
                return Err(corrupt());
            }
            let artifact = self.artifacts.get(index).ok_or_else(corrupt)?;
            if artifact.file.metadata().map_err(io_unavailable)?.nlink() != 1
                || ArtifactContinuity::read(&artifact.file)? != Some(artifact.continuity)
                || !self
                    .directory
                    .regular_file_matches(&artifact.name, &artifact.file)?
            {
                return Err(corrupt());
            }
            self.directory.verify()
        })
    }

    /// Reuse the acquired closed inventory for private-file operations. The
    /// pinned namespace and manifest must remain unchanged; original artifact
    /// reads and ownership handoffs still verify their own complete seals.
    fn verify_namespace(&self) -> Result<(), StorageError> {
        self.checked(|| {
            self.directory.verify()?;
            if directory_observation(&self.path)? != self.directory_observation
                || self
                    .manifest_file
                    .metadata()
                    .map_err(io_unavailable)?
                    .nlink()
                    != 1
                || ArtifactContinuity::read(&self.manifest_file)? != Some(self.manifest_continuity)
                || !self
                    .directory
                    .regular_file_matches(OsStr::new(MANIFEST_FILE_NAME), &self.manifest_file)?
            {
                return Err(corrupt());
            }
            self.directory.verify()
        })
    }

    pub(crate) fn verify(&self) -> Result<(), StorageError> {
        self.checked(|| {
            self.directory.verify()?;
            #[cfg(test)]
            OWNED_BACKUP_INVENTORY_PASSES.with(|count| count.set(count.get() + 1));
            validate_backup_inventory(&self.path)?;
            // Closed exact inventory: manifest plus only our recorded artifacts.
            let mut count = 0;
            for entry in fs::read_dir(&self.path).map_err(io_unavailable)? {
                let name = entry.map_err(io_unavailable)?.file_name();
                count += 1;
                if count > self.artifacts.len() + 1
                    || (name != MANIFEST_FILE_NAME
                        && !self.artifacts.iter().any(|a| a.name == name))
                {
                    return Err(corrupt());
                }
            }
            if count != self.artifacts.len() + 1
                || self
                    .manifest_file
                    .metadata()
                    .map_err(io_unavailable)?
                    .nlink()
                    != 1
                || ArtifactContinuity::read(&self.manifest_file)? != Some(self.manifest_continuity)
                || !self
                    .directory
                    .regular_file_matches(OsStr::new(MANIFEST_FILE_NAME), &self.manifest_file)?
            {
                return Err(corrupt());
            }
            for index in 0..self.artifacts.len() {
                self.verify_artifact(index)?;
            }
            self.directory.verify()
        })
    }

    pub(super) fn verify_full(&self) -> Result<(), StorageError> {
        self.checked(|| {
            self.verify()?;
            if sha256_reader(PositionalReader {
                file: &self.manifest_file,
                offset: 0,
            })? != self.manifest_digest
            {
                return Err(corrupt());
            }
            for (index, artifact) in self.artifacts.iter().enumerate() {
                self.verify_artifact(index)?;
                if sha256_reader(PositionalReader {
                    file: &artifact.file,
                    offset: 0,
                })? != artifact.checksum
                {
                    return Err(corrupt());
                }
                self.verify_artifact(index)?;
            }
            self.verify()
        })
    }

    pub(super) fn copy_into(
        &self,
        name: &OsStr,
        target: &MaterializationArtifact,
    ) -> Result<(), StorageError> {
        self.checked(|| {
            self.verify()?;
            target.verify()?;
            let index = self
                .artifacts
                .iter()
                .position(|a| a.name == name && a.generation.is_none())
                .ok_or_else(corrupt)?;
            let source = &self.artifacts[index];
            let (_, output) = target.identity.as_ref().ok_or_else(corrupt)?;
            let mut buffer = [0; COPY_BUFFER_BYTES];
            let mut offset = 0;
            loop {
                if self.cancelled.load(Ordering::Acquire) {
                    return Err(storage_error(StorageErrorKind::Unavailable));
                }
                let n = source
                    .file
                    .read_at(&mut buffer, offset)
                    .map_err(io_unavailable)?;
                if n == 0 {
                    break;
                }
                output
                    .write_all_at(&buffer[..n], offset)
                    .map_err(io_unavailable)?;
                offset = offset.checked_add(n as u64).ok_or_else(limit_exceeded)?;
            }
            if offset != source.continuity.length {
                return Err(corrupt());
            }
            output
                .set_len(offset)
                .and_then(|()| output.sync_all())
                .map_err(io_unavailable)?;
            target.verify()?;
            self.verify()
        })
    }

    pub(super) fn seals(&self) -> Vec<Arc<OwnedExtentSeal>> {
        self.artifacts
            .iter()
            .filter_map(|artifact| {
                artifact.generation.map(|generation| {
                    Arc::new(OwnedExtentSeal {
                        file: Arc::clone(&artifact.file),
                        copied_immutable: true,
                        generation,
                        checksum: artifact.checksum.clone(),
                        continuity: Some(artifact.continuity),
                        original: Some((Arc::clone(&self.directory), artifact.name.clone())),
                    })
                })
            })
            .collect()
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct DirectoryObservation {
    modified: (i64, i64),
    changed: (i64, i64),
}
fn directory_observation(path: &Path) -> Result<DirectoryObservation, StorageError> {
    let m = fs::symlink_metadata(path).map_err(io_unavailable)?;
    if !m.is_dir() {
        return Err(corrupt());
    }
    Ok(DirectoryObservation {
        modified: (m.mtime(), m.mtime_nsec()),
        changed: (m.ctime(), m.ctime_nsec()),
    })
}

struct PositionalReader<'a> {
    file: &'a File,
    offset: u64,
}
impl Read for PositionalReader<'_> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let n = self.file.read_at(out, self.offset)?;
        self.offset = self.offset.checked_add(n as u64).ok_or_else(refused)?;
        Ok(n)
    }
}

fn refused() -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        "immutable restore source unavailable",
    )
}
fn unavailable(_: StorageError) -> io::Error {
    refused()
}

/// Complete artifact verification owns one read interval around the entire
/// bounded parser, exactly as full-file hashing does. Its cursor cannot escape
/// acquisition. Raw borrowed reads retain individual checks; a retained frame
/// has one checked interval that must finish before its decoded value escapes.
#[derive(Clone, Copy)]
enum ReadSpan {
    CompleteArtifact,
    IndividualRead,
    RetainedFrame,
}

struct OriginalCursor {
    owner: Arc<OwnedBackupHistory>,
    index: usize,
    offset: u64,
    read_span: ReadSpan,
    finished: bool,
    stage: Option<(Arc<PinnedDirectory>, OsString)>,
}
impl std::fmt::Debug for OriginalCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OriginalCursor")
            .field("index", &self.index)
            .finish_non_exhaustive()
    }
}
impl OriginalCursor {
    fn check(&self) -> io::Result<()> {
        if self.finished {
            self.owner.invalidate();
            return Err(refused());
        }
        if matches!(
            self.read_span,
            ReadSpan::CompleteArtifact | ReadSpan::RetainedFrame
        ) {
            return self.owner.checked(|| Ok(())).map_err(unavailable);
        }
        self.verify_read_custody()
    }

    fn verify_read_custody(&self) -> io::Result<()> {
        self.owner
            .verify_artifact(self.index)
            .map_err(unavailable)?;
        if let Some((parent, name)) = &self.stage {
            let result = (|| {
                parent.verify()?;
                if parent.regular_file_length(name)?.is_some() {
                    return Err(corrupt());
                }
                parent.verify()
            })();
            if result.is_err() {
                self.owner.invalidate();
            }
            result.map_err(unavailable)?;
        }
        Ok(())
    }
}
impl Drop for OriginalCursor {
    fn drop(&mut self) {
        if matches!(self.read_span, ReadSpan::RetainedFrame) && !self.finished {
            self.owner.invalidate();
        }
    }
}

impl JournalMediaFile for OriginalCursor {
    fn finish_retained_frame_read(&mut self) -> io::Result<()> {
        if !matches!(self.read_span, ReadSpan::RetainedFrame) {
            return self.check();
        }
        self.check()?;
        #[cfg(test)]
        RETAINED_FRAME_FINISH_HOOK.with(|hook| {
            if let Some(action) = hook.borrow_mut().take() {
                action();
            }
        });
        self.verify_read_custody()?;
        self.finished = true;
        Ok(())
    }
    fn len(&mut self) -> io::Result<u64> {
        self.check()?;
        Ok(self.owner.artifacts[self.index].continuity.length)
    }
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        self.check()?;
        let result = self.owner.artifacts[self.index]
            .file
            .read_at(out, self.offset);
        if result.is_err() {
            self.owner.invalidate();
        }
        self.check()?;
        let n = result?;
        self.offset = self.offset.checked_add(n as u64).ok_or_else(refused)?;
        Ok(n)
    }
    fn read_exact(&mut self, mut out: &mut [u8]) -> io::Result<()> {
        while !out.is_empty() {
            let n = self.read(out)?;
            if n == 0 {
                self.owner.invalidate();
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            out = &mut out[n..];
        }
        Ok(())
    }
    fn read_exact_at(&mut self, out: &mut [u8], offset: usize) -> io::Result<()> {
        self.check()?;
        let result = self.owner.artifacts[self.index]
            .file
            .read_exact_at(out, offset as u64);
        if result.is_err() {
            self.owner.invalidate();
        }
        self.check()?;
        result
    }
    fn write_all(&mut self, _: &[u8]) -> io::Result<()> {
        Err(refused())
    }
    fn write_all_at(&mut self, _: &[u8], _: usize) -> io::Result<()> {
        Err(refused())
    }
    fn sync_data(&mut self) -> io::Result<()> {
        Err(refused())
    }
    fn sync_all(&mut self) -> io::Result<()> {
        Err(refused())
    }
    fn set_len(&mut self, _: u64) -> io::Result<()> {
        Err(refused())
    }
}

#[derive(Debug)]
struct OriginalMedia(Arc<OwnedBackupHistory>);
impl OriginalMedia {
    fn index(&self, path: &Path) -> io::Result<usize> {
        if path.parent() != Some(self.0.path.as_path()) {
            return Err(refused());
        }
        self.0
            .artifacts
            .iter()
            .position(|a| Some(a.name.as_os_str()) == path.file_name())
            .ok_or_else(|| io::ErrorKind::NotFound.into())
    }
}
impl JournalMedia for OriginalMedia {
    fn open_directory(&self, _: &Path) -> io::Result<Box<dyn JournalMediaDirectory>> {
        Err(refused())
    }
    fn try_exists(&self, path: &Path) -> io::Result<bool> {
        match self.index(path) {
            Ok(i) => {
                self.0.verify_artifact(i).map_err(unavailable)?;
                Ok(true)
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }
    fn metadata(&self, path: &Path) -> io::Result<MediaFileMetadata> {
        let i = self.index(path)?;
        self.0.verify_artifact(i).map_err(unavailable)?;
        Ok(MediaFileMetadata {
            is_file: true,
            len: self.0.artifacts[i].continuity.length,
        })
    }
    fn open_read(&self, path: &Path) -> io::Result<MediaFile> {
        let index = self.index(path)?;
        self.0.verify_artifact(index).map_err(unavailable)?;
        Ok(MediaFile::Port(Box::new(OriginalCursor {
            owner: Arc::clone(&self.0),
            index,
            offset: 0,
            read_span: ReadSpan::CompleteArtifact,
            finished: false,
            stage: None,
        })))
    }
    fn open_retained_frame_read(&self, path: &Path) -> io::Result<MediaFile> {
        let index = self.index(path)?;
        self.0.verify_artifact(index).map_err(unavailable)?;
        Ok(MediaFile::Port(Box::new(OriginalCursor {
            owner: Arc::clone(&self.0),
            index,
            offset: 0,
            read_span: ReadSpan::RetainedFrame,
            finished: false,
            stage: None,
        })))
    }
    fn open_read_write(&self, _: &Path) -> io::Result<MediaFile> {
        Err(refused())
    }
    fn create_new_read_write(&self, _: &Path) -> io::Result<MediaFile> {
        Err(refused())
    }
    fn create_new_write_only(&self, _: &Path) -> io::Result<MediaFile> {
        Err(refused())
    }
    fn rename(&self, _: &Path, _: &Path) -> io::Result<()> {
        Err(refused())
    }
    fn remove_file(&self, _: &Path) -> io::Result<()> {
        Err(refused())
    }
    fn sync_parent_data(&self, _: &Path) -> io::Result<()> {
        Err(refused())
    }
    fn sync_parent_all(&self, _: &Path) -> io::Result<()> {
        Err(refused())
    }
}

/// Only exact virtual sealed names map to original descriptors. All other
/// operations remain confined to the private stage, never the backup directory.
pub(super) struct BorrowedHistoryMedia {
    backup: Arc<OwnedBackupHistory>,
    database: PathBuf,
    stage: Arc<PinnedDirectory>,
}
impl std::fmt::Debug for BorrowedHistoryMedia {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BorrowedHistoryMedia")
            .finish_non_exhaustive()
    }
}
impl BorrowedHistoryMedia {
    pub(super) fn new(
        backup: Arc<OwnedBackupHistory>,
        database: PathBuf,
        stage: Arc<PinnedDirectory>,
    ) -> Self {
        Self {
            backup,
            database,
            stage,
        }
    }
    fn open_source_read(&self, path: &Path, read_span: ReadSpan) -> io::Result<MediaFile> {
        if self.absent_sealed(path)? {
            return Err(io::ErrorKind::NotFound.into());
        }
        match self.original(path) {
            Some(p) => {
                let index = OriginalMedia(Arc::clone(&self.backup)).index(&p)?;
                let cursor = OriginalCursor {
                    owner: Arc::clone(&self.backup),
                    index,
                    offset: 0,
                    read_span,
                    finished: false,
                    stage: Some((
                        Arc::clone(&self.stage),
                        path.file_name().ok_or_else(refused)?.to_os_string(),
                    )),
                };
                cursor.verify_read_custody()?;
                Ok(MediaFile::Port(Box::new(cursor)))
            }
            None => {
                self.private(path)?;
                RealJournalMedia.open_read(path)
            }
        }
    }
    fn original(&self, path: &Path) -> Option<PathBuf> {
        self.backup
            .artifacts
            .iter()
            .filter_map(|a| a.generation.map(|g| (g, a)))
            .find(|(g, _)| crate::journal::sealed_journal_path(&self.database, *g) == path)
            .map(|(_, a)| self.backup.path.join(&a.name))
    }
    /// A missing canonical sealed candidate is an authoritative absence in
    /// this closed inventory. Journal readers may then try the copied active
    /// extent, whose anchored prefix can still contain retained frames.
    fn absent_sealed(&self, path: &Path) -> io::Result<bool> {
        if path.parent() != self.database.parent() || self.original(path).is_some() {
            return Ok(false);
        }
        let zero = crate::journal::sealed_journal_path(&self.database, 0);
        let name = zero.file_name().ok_or_else(refused)?.as_encoded_bytes();
        let prefix = &name[..name.len() - 20];
        let Some(digits) = path
            .file_name()
            .and_then(|n| n.as_encoded_bytes().strip_prefix(prefix))
        else {
            return Ok(false);
        };
        if digits.len() != 20 || !digits.iter().all(u8::is_ascii_digit) {
            return Ok(false);
        }
        let generation = std::str::from_utf8(digits)
            .ok()
            .and_then(|n| n.parse::<u64>().ok())
            .filter(|n| *n > 0);
        let Some(generation) = generation else {
            return Ok(false);
        };
        if crate::journal::sealed_journal_path(&self.database, generation) != path {
            return Ok(false);
        }
        self.backup.verify_namespace().map_err(unavailable)?;
        let result = (|| {
            self.stage.verify()?;
            if self
                .stage
                .regular_file_length(path.file_name().ok_or_else(corrupt)?)?
                .is_some()
            {
                return Err(corrupt());
            }
            self.stage.verify()
        })();
        if result.is_err() {
            self.backup.invalidate();
        }
        result.map_err(unavailable)?;
        Ok(true)
    }

    fn private(&self, path: &Path) -> io::Result<()> {
        let prefix = crate::journal::sealed_journal_path(&self.database, 0)
            .file_name()
            .ok_or_else(refused)?
            .as_encoded_bytes()
            .to_vec();
        let stem = &prefix[..prefix.len() - 20];
        if path.parent() != self.database.parent()
            || self.original(path).is_some()
            || path
                .file_name()
                .is_some_and(|n| n.as_encoded_bytes().starts_with(stem))
        {
            return Err(refused());
        }
        self.backup.verify_namespace().map_err(unavailable)
    }
}
impl JournalMedia for BorrowedHistoryMedia {
    fn open_directory(&self, path: &Path) -> io::Result<Box<dyn JournalMediaDirectory>> {
        if Some(path) != self.database.parent() {
            return Err(refused());
        }
        self.backup.verify_namespace().map_err(unavailable)?;
        // These private restore consumers never reclaim sealed history; frame
        // resolution uses exact generation names, not directory discovery.
        RealJournalMedia.open_directory(path)
    }
    fn try_exists(&self, path: &Path) -> io::Result<bool> {
        if self.absent_sealed(path)? {
            return Ok(false);
        }
        match self.original(path) {
            Some(p) => OriginalMedia(Arc::clone(&self.backup)).try_exists(&p),
            None => {
                self.private(path)?;
                RealJournalMedia.try_exists(path)
            }
        }
    }
    fn metadata(&self, path: &Path) -> io::Result<MediaFileMetadata> {
        if self.absent_sealed(path)? {
            return Err(io::ErrorKind::NotFound.into());
        }
        match self.original(path) {
            Some(p) => OriginalMedia(Arc::clone(&self.backup)).metadata(&p),
            None => {
                self.private(path)?;
                RealJournalMedia.metadata(path)
            }
        }
    }
    fn open_read(&self, path: &Path) -> io::Result<MediaFile> {
        self.open_source_read(path, ReadSpan::IndividualRead)
    }
    fn open_retained_frame_read(&self, path: &Path) -> io::Result<MediaFile> {
        self.open_source_read(path, ReadSpan::RetainedFrame)
    }
    fn open_read_write(&self, path: &Path) -> io::Result<MediaFile> {
        self.private(path)?;
        RealJournalMedia.open_read_write(path)
    }
    fn create_new_read_write(&self, path: &Path) -> io::Result<MediaFile> {
        self.private(path)?;
        RealJournalMedia.create_new_read_write(path)
    }
    fn create_new_write_only(&self, path: &Path) -> io::Result<MediaFile> {
        self.private(path)?;
        RealJournalMedia.create_new_write_only(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.private(from)?;
        self.private(to)?;
        RealJournalMedia.rename(from, to)
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.private(path)?;
        RealJournalMedia.remove_file(path)
    }
    fn sync_parent_data(&self, path: &Path) -> io::Result<()> {
        self.private(path)?;
        RealJournalMedia.sync_parent_data(path)
    }
    fn sync_parent_all(&self, path: &Path) -> io::Result<()> {
        self.private(path)?;
        RealJournalMedia.sync_parent_all(path)
    }
}

#[cfg(test)]
type ArtifactReadHook = Box<dyn FnOnce()>;
#[cfg(test)]
std::thread_local! {
    static ORIGINAL_ARTIFACT_READ_CHECKS: std::cell::Cell<usize> = const {std::cell::Cell::new(0)};
    static OWNED_BACKUP_INVENTORY_PASSES: std::cell::Cell<usize> = const {std::cell::Cell::new(0)};
    static RETAINED_FRAME_FINISH_HOOK: std::cell::RefCell<Option<ArtifactReadHook>> = const {std::cell::RefCell::new(None)};
    static COMPLETE_ARTIFACT_READ_HOOK: std::cell::RefCell<Option<ArtifactReadHook>> = const { std::cell::RefCell::new(None) };
}
#[cfg(test)]
#[path = "backup_owned_history_tests.rs"]
mod tests;
