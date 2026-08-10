//! [`redb::StorageBackend`] adapter over one [`SimDisk`] file.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::disk::{self, SimDisk};

/// One simulated engine file exposed to redb through its public storage seam
/// (`Builder::create_with_backend`).
///
/// The adapter honors the `StorageBackend` contract: `set_len` growth
/// zero-initializes, reads or writes beyond the current length error, and
/// every operation after `close` errors. Simulated faults surface as
/// [`std::io::Error`] values, which redb converts to storage errors and
/// fails closed on (`StorageError::PreviousIo`) until the database handle is
/// reopened.
///
/// The handle is bound to the disk epoch it was created in: after a crash and
/// recovery, outstanding backends keep failing closed and the engine must be
/// reopened over a fresh backend.
#[derive(Debug)]
pub struct SimBackend {
    disk: SimDisk,
    file: String,
    epoch: u64,
    closed: AtomicBool,
}

impl SimBackend {
    /// Creates a backend over `file` on `disk`, bound to the current epoch.
    #[must_use]
    pub fn new(disk: &SimDisk, file: &str) -> Self {
        Self {
            disk: disk.clone(),
            file: file.to_owned(),
            epoch: disk.epoch(),
            closed: AtomicBool::new(false),
        }
    }

    /// Refuses an operation on a closed handle. The refusal folds a
    /// self-sufficient trace event (discriminator plus refused operation
    /// kind), so closed-handle behavior feeds the digest directly instead of
    /// being inferred from the traced `close`.
    fn refuse_if_closed(&self, operation: u64) -> Result<(), io::Error> {
        if self.closed.load(Ordering::SeqCst) {
            self.disk.trace_closed_refusal(&self.file, operation);
            return Err(io::Error::other("simulated backend already closed"));
        }
        Ok(())
    }
}

impl redb::StorageBackend for SimBackend {
    fn len(&self) -> Result<u64, io::Error> {
        self.refuse_if_closed(disk::TRACE_LEN)?;
        self.disk.guarded_len(self.epoch, &self.file)
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), io::Error> {
        self.refuse_if_closed(disk::TRACE_READ)?;
        self.disk.guarded_read(self.epoch, &self.file, offset, out)
    }

    fn set_len(&self, len: u64) -> Result<(), io::Error> {
        self.refuse_if_closed(disk::TRACE_SET_LEN)?;
        self.disk.guarded_set_len(self.epoch, &self.file, len)
    }

    fn sync_data(&self) -> Result<(), io::Error> {
        self.refuse_if_closed(disk::TRACE_SYNC)?;
        self.disk.guarded_sync(self.epoch, &self.file)
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        self.refuse_if_closed(disk::TRACE_WRITE)?;
        self.disk
            .guarded_write(self.epoch, &self.file, offset, data)
    }

    fn close(&self) -> Result<(), io::Error> {
        self.refuse_if_closed(disk::TRACE_CLOSE)?;
        self.closed.store(true, Ordering::SeqCst);
        self.disk.trace_close(self.epoch, &self.file)
    }
}
