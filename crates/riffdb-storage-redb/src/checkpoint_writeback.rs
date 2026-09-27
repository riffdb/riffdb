//! Bounded writeback for the physical checkpoint transaction only.
//!
//! Earlier syncs do not publish or install a checkpoint. redb still owns its
//! transaction, growth fences, and unconditional final Immediate commit sync.

use std::io;
use std::sync::{Arc, Mutex, MutexGuard};

use redb::StorageBackend;

/// Internal I/O batching choice, independent of every journal capacity bound.
const WRITEBACK_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Default)]
pub(crate) struct CheckpointWriteback {
    pending: Mutex<Option<usize>>,
}

impl CheckpointWriteback {
    fn lock(&self) -> io::Result<MutexGuard<'_, Option<usize>>> {
        self.pending
            .lock()
            .map_err(|_| io::Error::other("checkpoint writeback state poisoned"))
    }

    /// Enter only after acquiring redb's exclusive write transaction.
    pub(crate) fn enter(&self) -> io::Result<CheckpointWritebackScope<'_>> {
        let mut pending = self.lock()?;
        if pending.is_some() {
            return Err(io::Error::other("checkpoint writeback already active"));
        }
        *pending = Some(0);
        Ok(CheckpointWritebackScope(self))
    }
}

pub(crate) struct CheckpointWritebackScope<'a>(&'a CheckpointWriteback);

impl Drop for CheckpointWritebackScope<'_> {
    fn drop(&mut self) {
        if let Ok(mut pending) = self.0.pending.lock() {
            *pending = None;
        }
    }
}

#[derive(Debug)]
pub(crate) struct CheckpointWritebackBackend<T> {
    inner: T,
    state: Arc<CheckpointWriteback>,
}

impl<T> CheckpointWritebackBackend<T> {
    pub(crate) fn new(inner: T, state: Arc<CheckpointWriteback>) -> Self {
        Self { inner, state }
    }
}

impl<T: StorageBackend> StorageBackend for CheckpointWritebackBackend<T> {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.inner.read(offset, out)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        let mut pending = self.state.lock()?;
        self.inner.sync_data()?;
        if let Some(bytes) = pending.as_mut() {
            *bytes = 0;
        }
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        let mut pending = self.state.lock()?;
        let Some(bytes) = pending.as_mut() else {
            return self.inner.write(offset, data);
        };
        let invalid = || io::Error::new(io::ErrorKind::InvalidInput, "checkpoint write offset");
        let len = u64::try_from(data.len()).map_err(|_| invalid())?;
        offset.checked_add(len).ok_or_else(invalid)?;
        let mut cursor = offset;
        let mut remaining = data;
        while !remaining.is_empty() {
            if *bytes == WRITEBACK_BYTES {
                // Keep the state lock through synchronous I/O so another
                // backend caller cannot exceed the same budget concurrently.
                // This is additional durability, never an engine commit or
                // checkpoint installation. A failure stops before more writes.
                self.inner.sync_data()?;
                *bytes = 0;
            }
            let count = remaining.len().min(WRITEBACK_BYTES - *bytes);
            self.inner.write(cursor, &remaining[..count])?;
            *bytes += count;
            cursor += count as u64; // The complete end offset was checked above.
            remaining = &remaining[count..];
        }
        Ok(())
    }

    fn close(&self) -> io::Result<()> {
        self.inner.close()
    }
}

#[cfg(test)]
#[path = "checkpoint_writeback_tests.rs"]
mod tests;
