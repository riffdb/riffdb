//! Elide byte-identical checkpoint finalization writes without eliding fences.
//!
//! redb 4.2.0's checksum walk borrows leaf pages mutably even when it only
//! reads them. Evicted leaves consequently reach the backend again. The
//! comparison is against the current file, never a retained cache or hash.

use std::io;
use std::sync::{Arc, Mutex, MutexGuard};

use redb::StorageBackend;

#[derive(Debug, Default)]
pub(crate) struct CheckpointRewrite {
    active: Mutex<bool>,
}

impl CheckpointRewrite {
    fn lock(&self) -> io::Result<MutexGuard<'_, bool>> {
        self.active
            .lock()
            .map_err(|_| io::Error::other("checkpoint rewrite state poisoned"))
    }

    /// Called inside the sole redb write transaction, immediately before commit.
    pub(crate) fn enter(&self) -> io::Result<CheckpointRewriteScope<'_>> {
        let mut active = self.lock()?;
        if *active {
            return Err(io::Error::other("checkpoint rewrite already active"));
        }
        *active = true;
        Ok(CheckpointRewriteScope(self))
    }
}

pub(crate) struct CheckpointRewriteScope<'a>(&'a CheckpointRewrite);

impl Drop for CheckpointRewriteScope<'_> {
    fn drop(&mut self) {
        if let Ok(mut active) = self.0.active.lock() {
            *active = false;
        }
    }
}

#[derive(Debug)]
pub(crate) struct CheckpointRewriteBackend<T> {
    inner: T,
    state: Arc<CheckpointRewrite>,
}

impl<T> CheckpointRewriteBackend<T> {
    pub(crate) fn new(inner: T, state: Arc<CheckpointRewrite>) -> Self {
        Self { inner, state }
    }
}

impl<T: StorageBackend> StorageBackend for CheckpointRewriteBackend<T> {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.inner.read(offset, out)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        let _guard = self.state.lock()?;
        self.inner.set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        let _guard = self.state.lock()?;
        self.inner.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        // Hold the same lock across comparison and write, and across every
        // other backend write/resize/sync. Reader-driven cache eviction must
        // not change a page between equality and the omitted write.
        let active = self.state.lock()?;
        if *active && data.len() >= 4096 {
            let len = u64::try_from(data.len()).map_err(io::Error::other)?;
            let end = offset
                .checked_add(len)
                .ok_or_else(|| io::Error::other("checkpoint write offset overflow"))?;
            if end <= self.inner.len()? {
                // One fixed stack buffer: no page-sized allocation, value
                // cache, checksum assumption, or extra durability operation.
                let mut scratch = [0_u8; 4096];
                let mut cursor = offset;
                let mut equal = true;
                for chunk in data.chunks(scratch.len()) {
                    self.inner.read(cursor, &mut scratch[..chunk.len()])?;
                    if scratch[..chunk.len()] != *chunk {
                        equal = false;
                        break;
                    }
                    cursor += chunk.len() as u64; // Complete end checked above.
                }
                if equal {
                    return Ok(());
                }
            }
        }
        self.inner.write(offset, data)
    }

    fn close(&self) -> io::Result<()> {
        let _guard = self.state.lock()?;
        self.inner.close()
    }
}

#[cfg(test)]
#[path = "checkpoint_rewrite_tests.rs"]
mod tests;
