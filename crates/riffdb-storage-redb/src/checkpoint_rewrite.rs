//! Elide byte-identical checkpoint finalization writes without eliding fences.
//!
//! redb 4.2.0's checksum walk borrows leaf pages mutably even when it only
//! reads them. Evicted leaves consequently reach the backend again. The
//! comparison is against the current file, never a retained cache or hash.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
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
    read_probe: bool,
    read_calls: AtomicU64,
    read_bytes: AtomicU64,
}

impl<T> CheckpointRewriteBackend<T> {
    pub(crate) fn new(inner: T, state: Arc<CheckpointRewrite>) -> Self {
        Self::with_read_probe(
            inner,
            state,
            crate::query_diagnostics::query_execute_diagnostics_enabled(),
        )
    }

    fn with_read_probe(inner: T, state: Arc<CheckpointRewrite>, read_probe: bool) -> Self {
        Self {
            inner,
            state,
            read_probe,
            read_calls: AtomicU64::new(0),
            read_bytes: AtomicU64::new(0),
        }
    }
}

impl<T> Drop for CheckpointRewriteBackend<T> {
    fn drop(&mut self) {
        if self.read_probe {
            eprintln!(
                "riffdb-backend-read-probe-v1\t{}\t{}",
                self.read_calls.load(Ordering::Relaxed),
                self.read_bytes.load(Ordering::Relaxed)
            );
        }
    }
}

impl<T: StorageBackend> StorageBackend for CheckpointRewriteBackend<T> {
    fn try_lock_range(
        &self,
        start: std::ops::Bound<u64>,
        end: std::ops::Bound<u64>,
    ) -> Result<bool, redb::BackendError> {
        self.inner.try_lock_range(start, end)
    }

    fn try_lock_shared_range(
        &self,
        start: std::ops::Bound<u64>,
        end: std::ops::Bound<u64>,
    ) -> Result<bool, redb::BackendError> {
        self.inner.try_lock_shared_range(start, end)
    }

    fn lock_range(
        &self,
        start: std::ops::Bound<u64>,
        end: std::ops::Bound<u64>,
    ) -> Result<(), redb::BackendError> {
        self.inner.lock_range(start, end)
    }

    fn lock_shared_range(
        &self,
        start: std::ops::Bound<u64>,
        end: std::ops::Bound<u64>,
    ) -> Result<(), redb::BackendError> {
        self.inner.lock_shared_range(start, end)
    }

    fn unlock_range(
        &self,
        start: std::ops::Bound<u64>,
        end: std::ops::Bound<u64>,
    ) -> Result<(), redb::BackendError> {
        self.inner.unlock_range(start, end)
    }

    fn query_lock_range(
        &self,
        start: std::ops::Bound<u64>,
        end: std::ops::Bound<u64>,
    ) -> Result<bool, redb::BackendError> {
        self.inner.query_lock_range(start, end)
    }

    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        if self.read_probe {
            self.read_calls.fetch_add(1, Ordering::Relaxed);
            self.read_bytes
                .fetch_add(out.len() as u64, Ordering::Relaxed);
        }
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
