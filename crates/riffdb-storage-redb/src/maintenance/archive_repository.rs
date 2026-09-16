//! Append-only external archives; no database writer or acknowledgement port.
use super::path_guard::PinnedDirectory;
use riffdb_storage_api::{
    ARCHIVE_MANIFEST_V1_BYTES, ArchiveConsumerErrorV1 as Error, ArchiveEncryptionPostureV1,
    ArchiveFrameSinkV1, ArchiveFrameV1, ArchiveManifestV1, ArchiveRestoreSelectionV3,
    ArchiveRestoreSuffixV3, ChangelogHistoryPointV3, ChangelogLineageV3, MAX_CHANGELOG_FRAME_BYTES,
    StorageError, StorageErrorKind,
};
use std::{
    ffi::OsStr,
    fs::File,
    io::{Read, Write},
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
};

const LOCK: &str = "inventory.lock";
const CURRENT: &str = "CURRENT";
const FRAME_TEMP: &str = "frame.tmp";
const MANIFEST_TEMP: &str = "manifest.tmp";
const CURRENT_TEMP: &str = "current.tmp";

/// Exclusively owned filesystem archive. The caller supplies the independently
/// verified full-backup digest, original lineage/fence and operator encryption
/// policy. This repository neither encrypts bytes nor attests external encryption.
/// All reads and writes stay beneath one private retained directory capability.
pub struct RedbArchiveRepository {
    directory: PinnedDirectory,
    lock: File,
    lineage: ChangelogLineageV3,
    backup_fence: ChangelogHistoryPointV3,
    backup_digest: [u8; 32],
    encryption: ArchiveEncryptionPostureV1,
    head: Option<ArchiveManifestV1>,
    #[cfg(test)]
    crash_at: Option<String>,
    #[cfg(test)]
    fail_at: std::cell::Cell<Option<&'static str>>,
}

struct ReadPair {
    manifest: ArchiveManifestV1,
    bytes: Vec<u8>,
    frame_file: File,
    manifest_file: File,
    frame_name: String,
    manifest_name: String,
}

/// A read-only borrow of the exclusively owned archive. Each step validates one
/// complete bounded frame and its descriptor; the first failure fuses the reader.
/// The caller owns cancellation between steps and must drain each returned frame.
pub struct RedbArchiveFrames<'a> {
    repository: &'a RedbArchiveRepository,
    previous: Option<ArchiveManifestV1>,
    terminal: Option<ArchiveManifestV1>,
    finished: bool,
}

impl Iterator for RedbArchiveFrames<'_> {
    type Item = Result<(ArchiveManifestV1, Vec<u8>), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let result = (|| {
            self.repository.verify()?;
            if self
                .repository
                .read_current()?
                .map(|(manifest, _)| manifest)
                != self.repository.head
            {
                return Err(Error::ResyncRequired);
            }
            let Some(head) = self.terminal else {
                return Ok(None);
            };
            let pair = self.repository.read_pair(self.previous.as_ref(), head)?;
            self.repository.verify()?;
            self.previous = Some(pair.manifest);
            self.finished = pair.manifest == head;
            Ok(Some((pair.manifest, pair.bytes)))
        })();
        match result {
            Ok(Some(frame)) => Some(Ok(frame)),
            Ok(None) => {
                self.finished = true;
                None
            }
            Err(error) => {
                self.finished = true;
                Some(Err(error))
            }
        }
    }
}
impl std::iter::FusedIterator for RedbArchiveFrames<'_> {}

impl RedbArchiveRepository {
    /// Validates the entire selected prefix with one frame in memory before
    /// returning progress. Creates only a missing direct child of an existing
    /// parent; an existing unowned nonempty directory is never claimed.
    pub fn open(
        path: &Path,
        lineage: ChangelogLineageV3,
        backup_fence: ChangelogHistoryPointV3,
        backup_digest: [u8; 32],
        encryption: ArchiveEncryptionPostureV1,
    ) -> Result<Self, Error> {
        Self::open_cancellable(
            path,
            lineage,
            backup_fence,
            backup_digest,
            encryption,
            &AtomicBool::new(false),
        )
    }

    /// Cancellation is checked before each bounded frame validation. Cancelled
    /// recovery grants no progress and does not remove unselected material.
    pub fn open_cancellable(
        path: &Path,
        lineage: ChangelogLineageV3,
        backup_fence: ChangelogHistoryPointV3,
        backup_digest: [u8; 32],
        encryption: ArchiveEncryptionPostureV1,
        cancellation: &AtomicBool,
    ) -> Result<Self, Error> {
        Self::open_mode(
            path,
            lineage,
            backup_fence,
            backup_digest,
            encryption,
            cancellation,
            true,
        )
    }

    /// Restore may consume only an existing owned archive; missing evidence
    /// never initializes an empty sink as a side effect of a restore request.
    pub(in crate::maintenance) fn open_existing(
        path: &Path,
        lineage: ChangelogLineageV3,
        backup_fence: ChangelogHistoryPointV3,
        backup_digest: [u8; 32],
        encryption: ArchiveEncryptionPostureV1,
    ) -> Result<Self, Error> {
        Self::open_mode(
            path,
            lineage,
            backup_fence,
            backup_digest,
            encryption,
            &AtomicBool::new(false),
            false,
        )
    }

    fn open_mode(
        path: &Path,
        lineage: ChangelogLineageV3,
        backup_fence: ChangelogHistoryPointV3,
        backup_digest: [u8; 32],
        encryption: ArchiveEncryptionPostureV1,
        cancellation: &AtomicBool,
        create_missing: bool,
    ) -> Result<Self, Error> {
        cancelled(cancellation)?;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .ok_or(Error::InvalidManifest)?;
        let parent = PinnedDirectory::open(parent).map_err(storage)?;
        let name = path.file_name().ok_or(Error::InvalidManifest)?;
        let directory = match parent.child_directory(name).map_err(storage)? {
            Some(directory) => directory,
            None if create_missing => parent.create_private_child(name).map_err(storage)?,
            None => return Err(Error::InvalidManifest),
        };
        directory.verify_private().map_err(storage)?;
        let lock = match directory
            .regular_file_length(OsStr::new(LOCK))
            .map_err(storage)?
        {
            Some(0) => directory
                .open_file_read_write(OsStr::new(LOCK))
                .map_err(storage)?
                .into_std(),
            None if create_missing => {
                directory.bounded_entries(0).map_err(storage)?;
                directory
                    .create_new_file(OsStr::new(LOCK))
                    .map_err(storage)?
                    .into_std()
            }
            _ => return Err(Error::InvalidManifest),
        };
        lock.try_lock().map_err(|_| Error::SinkUnavailable)?;
        lock.sync_all().map_err(io)?;
        directory.sync().map_err(storage)?;
        let mut repository = Self {
            directory,
            lock,
            lineage,
            backup_fence,
            backup_digest,
            encryption,
            head: None,
            #[cfg(test)]
            crash_at: None,
            #[cfg(test)]
            fail_at: std::cell::Cell::new(None),
        };
        repository.verify()?;
        repository.recover(cancellation)?;
        Ok(repository)
    }

    /// Last fully validated, durable archive fence; never a source acknowledgement.
    pub fn position(&self) -> ChangelogHistoryPointV3 {
        self.head
            .map_or(self.backup_fence, |manifest| manifest.covered())
    }

    /// The exact selected external descriptor, absent for an empty archive.
    pub const fn head(&self) -> Option<ArchiveManifestV1> {
        self.head
    }

    pub(in crate::maintenance) fn bound_to(
        &self,
        lineage: ChangelogLineageV3,
        fence: ChangelogHistoryPointV3,
        digest: [u8; 32],
    ) -> bool {
        self.lineage == lineage && self.backup_fence == fence && self.backup_digest == digest
    }

    /// Streams the selected prefix in order without granting mutation or source
    /// acknowledgement authority. Construction performs no population read.
    pub fn frames(&self) -> RedbArchiveFrames<'_> {
        RedbArchiveFrames {
            repository: self,
            previous: None,
            terminal: self.head,
            finished: false,
        }
    }

    /// Streams only the receipt's frozen prefix, even if a later reopen observes
    /// an advanced CURRENT. The caller must validate the complete selected stream
    /// before changing a restore stage. Construction checks binding and bounds;
    /// iteration checks each exact frame and the terminal manifest bytes.
    pub fn frames_for_selection(
        &self,
        selection: &ArchiveRestoreSelectionV3,
    ) -> Result<RedbArchiveFrames<'_>, Error> {
        self.verify()?;
        if selection.lineage() != self.lineage {
            return Err(Error::ForeignLineage);
        }
        if selection.backup_fence() != self.backup_fence
            || selection.backup().manifest_checksum().as_bytes() != self.backup_digest
        {
            return Err(Error::InvalidManifest);
        }
        let terminal = match selection.suffix() {
            ArchiveRestoreSuffixV3::Empty => None,
            ArchiveRestoreSuffixV3::Terminal(manifest) => {
                self.check_binding(manifest)?;
                if self
                    .head
                    .is_none_or(|head| manifest.covered().sequence() > head.covered().sequence())
                {
                    return Err(Error::InvalidPosition);
                }
                Some(**manifest)
            }
        };
        Ok(RedbArchiveFrames {
            repository: self,
            previous: None,
            terminal,
            finished: false,
        })
    }

    fn verify(&self) -> Result<(), Error> {
        self.directory.verify_private().map_err(storage)?;
        if self.lock.metadata().map_err(io)?.len() != 0
            || !self
                .directory
                .regular_file_matches(OsStr::new(LOCK), &self.lock)
                .map_err(storage)?
        {
            return Err(Error::ResyncRequired);
        }
        Ok(())
    }

    fn check_binding(&self, manifest: &ArchiveManifestV1) -> Result<(), Error> {
        if manifest.lineage() != self.lineage {
            return Err(Error::ForeignLineage);
        }
        if manifest.backup_fence() != self.backup_fence
            || manifest.full_backup_manifest_digest() != self.backup_digest
            || manifest.encryption_posture() != self.encryption
        {
            return Err(Error::InvalidManifest);
        }
        Ok(())
    }

    fn recover(&mut self, cancellation: &AtomicBool) -> Result<(), Error> {
        let selected = self.read_current()?;
        if let Some((head, current_file)) = selected {
            self.check_binding(&head)?;
            let mut previous = None;
            loop {
                cancelled(cancellation)?;
                self.verify()?;
                let pair = self.read_pair(previous.as_ref(), head)?;
                self.sync_retained(&pair.frame_name, &pair.frame_file)?;
                self.sync_retained(&pair.manifest_name, &pair.manifest_file)?;
                if pair.manifest == head {
                    break;
                }
                previous = Some(pair.manifest);
            }
            cancelled(cancellation)?;
            self.sync_retained(CURRENT, &current_file)?;
            self.directory.sync().map_err(storage)?;
            self.verify()?;
            self.head = Some(head);
        }
        cancelled(cancellation)?;
        // Validation precedes cleanup. Only one unconfirmed successor pair can
        // exist under this owner; arbitrary files are neither adopted nor deleted.
        self.discard_unselected()?;
        self.verify()
    }

    fn read_pair(
        &self,
        previous: Option<&ArchiveManifestV1>,
        head: ArchiveManifestV1,
    ) -> Result<ReadPair, Error> {
        let before = previous.map_or(self.backup_fence, ArchiveManifestV1::covered);
        let (frame_name, manifest_name) = names(before)?;
        let (manifest_bytes, manifest_file) =
            self.read_required(&manifest_name, ARCHIVE_MANIFEST_V1_BYTES)?;
        let manifest = ArchiveManifestV1::decode(&manifest_bytes)?;
        self.check_binding(&manifest)?;
        manifest.verify_predecessor(previous)?;
        if manifest.before() != before || manifest.covered().sequence() > head.covered().sequence()
        {
            return Err(Error::InvalidPosition);
        }
        if manifest.covered().sequence() == head.covered().sequence() && manifest != head {
            return Err(Error::InvalidManifest);
        }
        let (bytes, frame_file) = self.read_required(&frame_name, MAX_CHANGELOG_FRAME_BYTES)?;
        manifest.verify_frame(&bytes)?;
        Ok(ReadPair {
            manifest,
            bytes,
            frame_file,
            manifest_file,
            frame_name,
            manifest_name,
        })
    }

    fn read_current(&self) -> Result<Option<(ArchiveManifestV1, File)>, Error> {
        match self.read_optional(CURRENT, ARCHIVE_MANIFEST_V1_BYTES)? {
            Some((bytes, file)) => Ok(Some((ArchiveManifestV1::decode(&bytes)?, file))),
            None => Ok(None),
        }
    }

    fn read_optional(&self, name: &str, maximum: usize) -> Result<Option<(Vec<u8>, File)>, Error> {
        let Some(length) = self
            .directory
            .regular_file_length(OsStr::new(name))
            .map_err(storage)?
        else {
            return Ok(None);
        };
        if length > maximum as u64 {
            return Err(Error::InvalidManifest);
        }
        let mut file = self
            .directory
            .open_file(OsStr::new(name))
            .map_err(storage)?
            .into_std();
        let mut bytes = Vec::with_capacity(length as usize);
        (&mut file)
            .take(maximum as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(io)?;
        if bytes.len() as u64 != length
            || !self
                .directory
                .regular_file_matches(OsStr::new(name), &file)
                .map_err(storage)?
        {
            return Err(Error::InvalidManifest);
        }
        Ok(Some((bytes, file)))
    }
    fn read_required(&self, name: &str, maximum: usize) -> Result<(Vec<u8>, File), Error> {
        self.read_optional(name, maximum)?
            .ok_or(Error::InvalidManifest)
    }
    fn sync_retained(&self, name: &str, file: &File) -> Result<(), Error> {
        file.sync_all().map_err(io)?;
        if !self
            .directory
            .regular_file_matches(OsStr::new(name), file)
            .map_err(storage)?
        {
            return Err(Error::ResyncRequired);
        }
        Ok(())
    }

    fn remove_bounded(&self, name: &str, maximum: usize) -> Result<(), Error> {
        self.verify()?;
        if self
            .directory
            .regular_file_length(OsStr::new(name))
            .map_err(storage)?
            .is_some_and(|length| length > maximum as u64)
        {
            return Err(Error::InvalidManifest);
        }
        self.directory
            .remove_file_if_present(OsStr::new(name))
            .map_err(storage)?;
        Ok(())
    }
    fn discard_unselected(&self) -> Result<(), Error> {
        let mut removals = vec![
            (FRAME_TEMP.to_owned(), MAX_CHANGELOG_FRAME_BYTES),
            (MANIFEST_TEMP.to_owned(), ARCHIVE_MANIFEST_V1_BYTES),
            (CURRENT_TEMP.to_owned(), ARCHIVE_MANIFEST_V1_BYTES),
        ];
        if self.position().sequence().checked_next().is_some() {
            let (frame, manifest) = names(self.position())?;
            removals.push((frame, MAX_CHANGELOG_FRAME_BYTES));
            removals.push((manifest, ARCHIVE_MANIFEST_V1_BYTES));
        }
        // Check the fixed five-name inventory before removing any member.
        for (name, maximum) in &removals {
            if self
                .directory
                .regular_file_length(OsStr::new(name))
                .map_err(storage)?
                .is_some_and(|length| length > *maximum as u64)
            {
                return Err(Error::InvalidManifest);
            }
        }
        for (name, maximum) in &removals {
            self.remove_bounded(name, *maximum)?;
        }
        self.directory.sync().map_err(storage)
    }

    fn compare_and_sync(&self, name: &str, expected: &[u8]) -> Result<(), Error> {
        if self
            .directory
            .regular_file_length(OsStr::new(name))
            .map_err(storage)?
            != Some(expected.len() as u64)
        {
            return Err(Error::InvalidFrame);
        }
        let mut file = self
            .directory
            .open_file(OsStr::new(name))
            .map_err(storage)?
            .into_std();
        let mut buffer = [0u8; 8192];
        for chunk in expected.chunks(buffer.len()) {
            file.read_exact(&mut buffer[..chunk.len()]).map_err(io)?;
            if &buffer[..chunk.len()] != chunk {
                return Err(Error::InvalidFrame);
            }
        }
        if file.read(&mut buffer[..1]).map_err(io)? != 0 {
            return Err(Error::InvalidFrame);
        }
        self.sync_retained(name, &file)
    }

    fn write_temporary(&self, name: &str, bytes: &[u8], maximum: usize) -> Result<File, Error> {
        if bytes.len() > maximum {
            return Err(Error::InvalidFrame);
        }
        self.remove_bounded(name, maximum)?;
        let mut file = self
            .directory
            .create_new_file(OsStr::new(name))
            .map_err(storage)?
            .into_std();
        file.write_all(bytes).map_err(io)?;
        self.sync_retained(name, &file)?;
        Ok(file)
    }

    fn immutable(
        &self,
        name: &str,
        temporary: &str,
        bytes: &[u8],
        maximum: usize,
        edges: [&str; 3],
    ) -> Result<(), Error> {
        self.verify()?;
        if self
            .directory
            .regular_file_length(OsStr::new(name))
            .map_err(storage)?
            .is_some()
        {
            return self.compare_and_sync(name, bytes);
        }
        let file = self.write_temporary(temporary, bytes, maximum)?;
        self.boundary(edges[0])?;
        self.verify()?;
        self.directory
            .hard_link_to(OsStr::new(temporary), &self.directory, OsStr::new(name))
            .map_err(storage)?;
        self.sync_retained(name, &file)?;
        self.boundary(edges[1])?;
        self.directory.sync().map_err(storage)?;
        self.remove_bounded(temporary, maximum)?;
        self.directory.sync().map_err(storage)?;
        self.boundary(edges[2])?;
        self.verify()
    }

    fn confirm_pair(
        &self,
        manifest: ArchiveManifestV1,
        frame: &ArchiveFrameV1,
    ) -> Result<(), Error> {
        let (frame_name, manifest_name) = names(manifest.before())?;
        self.compare_and_sync(&frame_name, frame.as_bytes())?;
        self.compare_and_sync(&manifest_name, &manifest.encode())?;
        self.compare_and_sync(CURRENT, &manifest.encode())?;
        self.directory.sync().map_err(storage)?;
        self.verify()
    }

    fn boundary(&self, _edge: &str) -> Result<(), Error> {
        #[cfg(test)]
        if self.crash_at.as_deref() == Some(_edge) {
            std::process::exit(97);
        }
        #[cfg(test)]
        if self.fail_at.get() == Some(_edge) {
            self.fail_at.set(None);
            return Err(Error::SinkUnavailable);
        }
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn fail_once_at(&self, edge: &'static str) {
        self.fail_at.set(Some(edge));
    }
    #[cfg(test)]
    pub(super) fn crash_at(&mut self, edge: String) {
        self.crash_at = Some(edge);
    }
}

impl ArchiveFrameSinkV1 for RedbArchiveRepository {
    fn persist(&mut self, frame: &ArchiveFrameV1) -> Result<(), Error> {
        self.verify()?;
        if frame.lineage() != self.lineage {
            return Err(Error::ForeignLineage);
        }
        if let Some(head) = self.head
            && head.before() == frame.before()
            && head.covered() == frame.covered()
        {
            head.verify_frame(frame.as_bytes())?;
            self.confirm_pair(head, frame)?;
            return Ok(());
        }
        if frame.before() != self.position() {
            return Err(Error::InvalidPosition);
        }
        let expected = match self.head {
            Some(head) => head.next(frame)?,
            None => ArchiveManifestV1::first(frame, self.backup_digest, self.encryption)?,
        };
        let observed = self.read_current()?.map(|(manifest, _)| manifest);
        if observed == Some(expected) {
            // A prior rename may have succeeded while its sync/result was lost.
            // Compare exact bytes and establish durability before acknowledging.
            self.confirm_pair(expected, frame)?;
            self.head = Some(expected);
            return Ok(());
        }
        if observed != self.head {
            return Err(Error::ResyncRequired);
        }
        let (frame_name, manifest_name) = names(frame.before())?;
        self.immutable(
            &frame_name,
            FRAME_TEMP,
            frame.as_bytes(),
            MAX_CHANGELOG_FRAME_BYTES,
            ["frame-staged", "frame-linked", "frame-published"],
        )?;
        let manifest_bytes = expected.encode();
        self.immutable(
            &manifest_name,
            MANIFEST_TEMP,
            &manifest_bytes,
            ARCHIVE_MANIFEST_V1_BYTES,
            ["manifest-staged", "manifest-linked", "manifest-published"],
        )?;
        let current =
            self.write_temporary(CURRENT_TEMP, &manifest_bytes, ARCHIVE_MANIFEST_V1_BYTES)?;
        self.boundary("current-staged")?;
        self.verify()?;
        if self.read_current()?.map(|(manifest, _)| manifest) != self.head {
            return Err(Error::ResyncRequired);
        }
        self.directory
            .rename(OsStr::new(CURRENT_TEMP), OsStr::new(CURRENT))
            .map_err(storage)?;
        self.boundary("current-renamed")?;
        self.sync_retained(CURRENT, &current)?;
        self.directory.sync().map_err(storage)?;
        self.boundary("current-synced")?;
        self.verify()?;
        self.head = Some(expected);
        Ok(())
    }
}
impl std::fmt::Debug for RedbArchiveRepository {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RedbArchiveRepository([redacted])")
    }
}
fn names(before: ChangelogHistoryPointV3) -> Result<(String, String), Error> {
    let sequence = before
        .sequence()
        .checked_next()
        .ok_or(Error::InvalidPosition)?
        .get();
    Ok((
        format!("frame-{sequence:016x}.v3"),
        format!("manifest-{sequence:016x}.v1"),
    ))
}
fn storage(error: StorageError) -> Error {
    match error.kind() {
        StorageErrorKind::CorruptData | StorageErrorKind::IncompatibleFormat => {
            Error::InvalidManifest
        }
        _ => Error::SinkUnavailable,
    }
}
fn io(_: std::io::Error) -> Error {
    Error::SinkUnavailable
}
fn cancelled(cancellation: &AtomicBool) -> Result<(), Error> {
    if cancellation.load(Ordering::Acquire) {
        Err(Error::ResyncRequired)
    } else {
        Ok(())
    }
}
