//! [`JournalMedia`] implementation over one [`SimDisk`] (SIM-B).
//!
//! Drives RiffDB's journal side-file family — durability journal, checkpoint,
//! spare, and durable-format marker — from the simulated disk, with the same
//! durable/volatile/fault semantics as the engine backend adapter and every
//! operation and fault decision folded into the versioned trace chain.
//!
//! Path mapping: media paths are used verbatim (UTF-8 required) as simulated
//! file names, so the engine backend and the side files share one namespace
//! on one disk. Handles are name-keyed and epoch-bound: a handle held across
//! a rename of its name observes the new occupant (production drops handles
//! before renaming, so no production sequence hits this), and handles from
//! before a crash recovery fail closed exactly like backend handles.

use std::io;
use std::path::Path;

use riffdb_storage_redb::{
    JournalMedia, JournalMediaDirectory, JournalMediaFile, MediaFile, MediaFileMetadata,
};

use crate::disk::SimDisk;

/// Journal media port over one simulated disk, bound to no epoch itself:
/// every opened handle binds the disk epoch current at open time.
#[derive(Clone, Debug)]
pub struct SimJournalMedia {
    disk: SimDisk,
}

impl SimJournalMedia {
    /// Creates a media port over `disk`.
    #[must_use]
    pub fn new(disk: &SimDisk) -> Self {
        Self { disk: disk.clone() }
    }
}

fn media_name(path: &Path) -> io::Result<String> {
    path.to_str().map(str::to_owned).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "simulated media paths must be UTF-8",
        )
    })
}

fn parent_name(path: &Path) -> io::Result<String> {
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "simulated media path has no parent directory",
        )
    })?;
    media_name(parent)
}

#[derive(Debug)]
struct SimMediaDirectory {
    disk: SimDisk,
    directory: String,
    after: Option<(String, bool)>,
    epoch: u64,
}

impl JournalMediaDirectory for SimMediaDirectory {
    fn next_entry(&mut self) -> io::Result<Option<std::ffi::OsString>> {
        let next = self.disk.media_directory_next(
            self.epoch,
            &self.directory,
            self.after
                .as_ref()
                .map(|(name, inclusive)| (name.as_str(), *inclusive)),
        )?;
        let Some((entry, boundary, inclusive)) = next else {
            return Ok(None);
        };
        self.after = Some((boundary, inclusive));
        let entry = std::ffi::OsString::from(entry);
        Ok(Some(entry))
    }
}

/// One simulated media file handle: name-keyed, epoch-bound, carrying the
/// sequential cursor that positional operations never move.
#[derive(Debug)]
struct SimMediaFile {
    disk: SimDisk,
    name: String,
    epoch: u64,
    cursor: u64,
}

impl SimMediaFile {
    fn offset(&self, offset: usize) -> io::Result<u64> {
        u64::try_from(offset)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "media offset overflow"))
    }
}

impl JournalMediaFile for SimMediaFile {
    fn len(&mut self) -> io::Result<u64> {
        self.disk.media_len(self.epoch, &self.name)
    }

    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let count = self
            .disk
            .media_read_sequential(self.epoch, &self.name, self.cursor, out)?;
        self.cursor += count as u64;
        Ok(count)
    }

    fn read_exact(&mut self, out: &mut [u8]) -> io::Result<()> {
        let mut filled = 0;
        while filled < out.len() {
            let count = self.read(&mut out[filled..])?;
            if count == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "simulated media file ended before the exact read filled",
                ));
            }
            filled += count;
        }
        Ok(())
    }

    fn write_all(&mut self, data: &[u8]) -> io::Result<()> {
        self.disk
            .media_write_at(self.epoch, &self.name, self.cursor, data)?;
        self.cursor += data.len() as u64;
        Ok(())
    }

    fn write_all_at(&mut self, data: &[u8], offset: usize) -> io::Result<()> {
        let offset = self.offset(offset)?;
        self.disk
            .media_write_at(self.epoch, &self.name, offset, data)
    }

    fn read_exact_at(&mut self, out: &mut [u8], offset: usize) -> io::Result<()> {
        let offset = self.offset(offset)?;
        self.disk
            .media_read_exact_at(self.epoch, &self.name, offset, out)
    }

    fn sync_data(&mut self) -> io::Result<()> {
        self.disk.media_sync(self.epoch, &self.name, false)
    }

    fn sync_all(&mut self) -> io::Result<()> {
        self.disk.media_sync(self.epoch, &self.name, true)
    }

    fn set_len(&mut self, len: u64) -> io::Result<()> {
        self.disk.guarded_set_len(self.epoch, &self.name, len)
    }
}

impl SimJournalMedia {
    fn handle(&self, name: String) -> MediaFile {
        MediaFile::Port(Box::new(SimMediaFile {
            disk: self.disk.clone(),
            epoch: self.disk.epoch(),
            name,
            cursor: 0,
        }))
    }

    fn open(&self, path: &Path, write: bool) -> io::Result<MediaFile> {
        let name = media_name(path)?;
        self.disk.media_open(self.disk.epoch(), &name, write)?;
        Ok(self.handle(name))
    }

    fn create_new(&self, path: &Path) -> io::Result<MediaFile> {
        let name = media_name(path)?;
        self.disk.media_create_new(self.disk.epoch(), &name)?;
        Ok(self.handle(name))
    }
}

impl JournalMedia for SimJournalMedia {
    fn open_directory(&self, path: &Path) -> io::Result<Box<dyn JournalMediaDirectory>> {
        let directory = media_name(path)?;
        let epoch = self.disk.epoch();
        self.disk.media_directory_open(epoch, &directory)?;
        Ok(Box::new(SimMediaDirectory {
            disk: self.disk.clone(),
            directory,
            after: None,
            epoch,
        }))
    }
    fn try_exists(&self, path: &Path) -> io::Result<bool> {
        let name = media_name(path)?;
        self.disk
            .media_probe(self.disk.epoch(), &name)
            .map(|length| length.is_some())
    }

    fn metadata(&self, path: &Path) -> io::Result<MediaFileMetadata> {
        let name = media_name(path)?;
        match self.disk.media_probe(self.disk.epoch(), &name)? {
            // The simulated namespace holds regular files only.
            Some(len) => Ok(MediaFileMetadata { is_file: true, len }),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "simulated media file not found",
            )),
        }
    }

    fn open_read(&self, path: &Path) -> io::Result<MediaFile> {
        self.open(path, false)
    }

    fn open_read_write(&self, path: &Path) -> io::Result<MediaFile> {
        self.open(path, true)
    }

    fn create_new_read_write(&self, path: &Path) -> io::Result<MediaFile> {
        self.create_new(path)
    }

    fn create_new_write_only(&self, path: &Path) -> io::Result<MediaFile> {
        // Access modes are not modeled; exclusivity is (`AlreadyExists`).
        self.create_new(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let from = media_name(from)?;
        let to = media_name(to)?;
        self.disk.media_rename(self.disk.epoch(), &from, &to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        let name = media_name(path)?;
        self.disk.media_remove(self.disk.epoch(), &name)
    }

    fn sync_parent_data(&self, path: &Path) -> io::Result<()> {
        let directory = parent_name(path)?;
        self.disk
            .media_parent_sync(self.disk.epoch(), &directory, false)
    }

    fn sync_parent_all(&self, path: &Path) -> io::Result<()> {
        let directory = parent_name(path)?;
        self.disk
            .media_parent_sync(self.disk.epoch(), &directory, true)
    }
}

#[cfg(test)]
mod directory_tests {
    use super::*;
    use crate::FaultConfig;

    fn populated() -> (SimDisk, SimJournalMedia) {
        let disk = SimDisk::new(FaultConfig::quiet(73));
        let media = SimJournalMedia::new(&disk);
        for name in ["a", "nested/b", "nested/c", "nested0", "z"] {
            drop(
                media
                    .create_new_write_only(Path::new(&format!("/virtual/{name}")))
                    .unwrap(),
            );
        }
        (disk, media)
    }

    // req: SIM-001, SIM-005, STO-031
    #[test]
    fn directory_iteration_matches_real_siblings_and_has_a_reproducible_trace() {
        let scratch = riffdb_testkit::scratch::ScratchDir::new_in(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target"),
            "directory-media",
        )
        .unwrap();
        std::fs::create_dir(scratch.join("nested")).unwrap();
        for name in ["a", "nested/b", "nested/c", "nested0", "z"] {
            drop(std::fs::File::create(scratch.join(name)).unwrap());
        }
        let mut real = riffdb_storage_redb::RealJournalMedia
            .open_directory(&scratch.join(""))
            .unwrap();
        let mut expected = Vec::new();
        while let Some(name) = real.next_entry().unwrap() {
            expected.push(name);
        }
        expected.sort();
        assert_eq!(
            expected,
            ["a", "nested", "nested0", "z"].map(std::ffi::OsString::from)
        );
        let mut traces = Vec::new();
        for _ in 0..2 {
            let (disk, media) = populated();
            let before = disk.trace_digest();
            let mut cursor = media.open_directory(Path::new("/virtual")).unwrap();
            let mut observed = Vec::new();
            while let Some(name) = cursor.next_entry().unwrap() {
                observed.push(name);
            }
            assert_eq!(observed, expected);
            assert!(cursor.next_entry().unwrap().is_none());
            assert_ne!(disk.trace_digest(), before);
            traces.push(disk.trace_digest());
        }
        assert_eq!(traces[0], traces[1]);
    }

    // req: SIM-001, SIM-002, SIM-005
    #[test]
    fn directory_handle_from_before_recovery_refuses_instead_of_serving_new_names() {
        let (disk, media) = populated();
        let mut old = media.open_directory(Path::new("/virtual")).unwrap();
        assert_eq!(old.next_entry().unwrap(), Some("a".into()));
        disk.crash();
        disk.recover_after_crash();
        assert!(old.next_entry().is_err());
        let mut current = media.open_directory(Path::new("/virtual")).unwrap();
        assert_eq!(current.next_entry().unwrap(), Some("a".into()));
    }

    // req: SIM-001, SIM-005, STO-031
    #[test]
    fn directory_open_refuses_missing_parent_and_file_as_directory() {
        let (_, media) = populated();
        assert_eq!(
            media
                .open_directory(Path::new("/absent"))
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            media
                .open_directory(Path::new("/virtual/a"))
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotADirectory
        );
    }
}
