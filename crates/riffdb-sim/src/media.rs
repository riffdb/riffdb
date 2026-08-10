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

use riffdb_storage_redb::{JournalMedia, JournalMediaFile, MediaFile, MediaFileMetadata};

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
