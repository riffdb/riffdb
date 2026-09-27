// req: STO-001, REC-001, REC-002

use super::*;

#[derive(Debug)]
struct Memory {
    bytes: Mutex<Vec<u8>>,
    writes: Mutex<Vec<(u64, Vec<u8>)>>,
    syncs: std::sync::atomic::AtomicUsize,
}

impl Memory {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes: Mutex::new(bytes),
            writes: Mutex::default(),
            syncs: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl StorageBackend for Memory {
    fn len(&self) -> io::Result<u64> {
        Ok(self.bytes.lock().unwrap().len() as u64)
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        let bytes = self.bytes.lock().unwrap();
        let start = usize::try_from(offset).map_err(io::Error::other)?;
        let end = start
            .checked_add(out.len())
            .ok_or_else(|| io::Error::other("overflow"))?;
        let value = bytes
            .get(start..end)
            .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
        out.copy_from_slice(value);
        Ok(())
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.bytes
            .lock()
            .unwrap()
            .resize(usize::try_from(len).map_err(io::Error::other)?, 0);
        Ok(())
    }
    fn sync_data(&self) -> io::Result<()> {
        self.syncs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        let start = usize::try_from(offset).map_err(io::Error::other)?;
        let end = start
            .checked_add(data.len())
            .ok_or_else(|| io::Error::other("overflow"))?;
        let mut bytes = self.bytes.lock().unwrap();
        if end > bytes.len() {
            bytes.resize(end, 0);
        }
        bytes[start..end].copy_from_slice(data);
        self.writes.lock().unwrap().push((offset, data.to_vec()));
        Ok(())
    }
}

#[test]
fn identical_checkpoint_bytes_omit_only_the_write_and_keep_every_sync() {
    let state = Arc::new(CheckpointRewrite::default());
    let data = vec![0x62; 8192];
    let backend = CheckpointRewriteBackend::new(Memory::new(data.clone()), state.clone());
    let scope = state.enter().unwrap();
    backend.write(0, &data).unwrap();
    backend.sync_data().unwrap();
    backend.sync_data().unwrap();
    assert!(backend.inner.writes.lock().unwrap().is_empty());
    assert_eq!(
        backend
            .inner
            .syncs
            .load(std::sync::atomic::Ordering::SeqCst),
        2
    );
    drop(scope);
    backend.write(0, &data).unwrap();
    assert_eq!(backend.inner.writes.lock().unwrap().len(), 1);
}

#[test]
fn changed_last_chunk_is_written_in_full_and_extensions_are_not_elided() {
    let state = Arc::new(CheckpointRewrite::default());
    let mut data = vec![0x62; 12_289];
    let backend = CheckpointRewriteBackend::new(Memory::new(data.clone()), state.clone());
    let _scope = state.enter().unwrap();
    data[12_288] ^= 1;
    backend.write(0, &data).unwrap();
    assert_eq!(
        *backend.inner.writes.lock().unwrap(),
        vec![(0, data.clone())]
    );
    assert_eq!(*backend.inner.bytes.lock().unwrap(), data);
    backend.write(12_289, &data).unwrap();
    assert_eq!(backend.inner.bytes.lock().unwrap().len(), 24_578);
    assert_eq!(backend.inner.writes.lock().unwrap().len(), 2);
    assert!(backend.write(u64::MAX - 4095, &[0; 4096]).is_err());
    assert_eq!(backend.inner.writes.lock().unwrap().len(), 2);
}

#[test]
fn short_writes_and_reopened_scopes_keep_original_behavior() {
    let state = Arc::new(CheckpointRewrite::default());
    let backend = CheckpointRewriteBackend::new(Memory::new(vec![0; 8192]), state.clone());
    let scope = state.enter().unwrap();
    assert!(state.enter().is_err());
    backend.write(0, &[0; 8]).unwrap();
    backend.write(0, &[0; 8192]).unwrap();
    assert_eq!(backend.inner.writes.lock().unwrap().len(), 1);
    drop(scope);
    let _scope = state.enter().unwrap();
    backend.set_len(0).unwrap();
    backend.write(0, &[0; 8192]).unwrap();
    assert_eq!(backend.inner.writes.lock().unwrap().len(), 2);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Fault {
    Length,
    Read,
    Write,
    Sync,
}

#[derive(Debug)]
struct FaultBackend {
    inner: Memory,
    fault: Fault,
}

impl StorageBackend for FaultBackend {
    fn len(&self) -> io::Result<u64> {
        if self.fault == Fault::Length {
            Err(io::Error::other("injected length"))
        } else {
            self.inner.len()
        }
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        if self.fault == Fault::Read {
            Err(io::Error::other("injected read"))
        } else {
            self.inner.read(offset, out)
        }
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        if self.fault == Fault::Write {
            Err(io::Error::other("injected write"))
        } else {
            self.inner.write(offset, data)
        }
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
    fn sync_data(&self) -> io::Result<()> {
        if self.fault == Fault::Sync {
            Err(io::Error::other("injected sync"))
        } else {
            self.inner.sync_data()
        }
    }
}

#[test]
fn comparison_and_durability_failures_never_report_a_successful_write_or_fence() {
    for fault in [Fault::Length, Fault::Read, Fault::Write, Fault::Sync] {
        let state = Arc::new(CheckpointRewrite::default());
        let backend = CheckpointRewriteBackend::new(
            FaultBackend {
                inner: Memory::new(vec![0; 8192]),
                fault,
            },
            state.clone(),
        );
        let _scope = state.enter().unwrap();
        if fault == Fault::Sync {
            backend.write(0, &[0; 8192]).unwrap();
            assert!(backend.sync_data().is_err());
        } else {
            assert!(backend.write(0, &[1; 8192]).is_err());
        }
        assert!(backend.inner.inner.writes.lock().unwrap().is_empty());
        assert_eq!(
            backend
                .inner
                .inner
                .syncs
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }
}

#[derive(Debug)]
struct PausingRead {
    inner: Memory,
    entered: std::sync::mpsc::SyncSender<()>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
    pause: std::sync::atomic::AtomicBool,
}

impl StorageBackend for PausingRead {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.inner.read(offset, out)?;
        if self.pause.swap(false, std::sync::atomic::Ordering::SeqCst) {
            self.entered.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
        }
        Ok(())
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.inner.write(offset, data)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
    fn sync_data(&self) -> io::Result<()> {
        self.inner.sync_data()
    }
}

#[test]
fn compare_and_omission_serialize_with_competing_backend_mutations() {
    let state = Arc::new(CheckpointRewrite::default());
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let backend = Arc::new(CheckpointRewriteBackend::new(
        PausingRead {
            inner: Memory::new(vec![0; 8192]),
            entered: entered_tx,
            release: Mutex::new(release_rx),
            pause: std::sync::atomic::AtomicBool::new(true),
        },
        state.clone(),
    ));
    let _scope = state.enter().unwrap();
    std::thread::scope(|threads| {
        let first = Arc::clone(&backend);
        let writer = threads.spawn(move || first.write(0, &[0; 8192]));
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        // The comparison has read its first chunk. Its shared mutation lock
        // must still exclude a write, resize, or sync from another caller.
        assert!(matches!(
            state.active.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        ));
        let second = Arc::clone(&backend);
        let other = threads.spawn(move || {
            second.set_len(0)?;
            second.write(0, &[1; 8192])?;
            second.sync_data()
        });
        release_tx.send(()).unwrap();
        writer.join().unwrap().unwrap();
        other.join().unwrap().unwrap();
    });
    assert_eq!(*backend.inner.inner.bytes.lock().unwrap(), vec![1; 8192]);
    assert_eq!(
        *backend.inner.inner.writes.lock().unwrap(),
        vec![(0, vec![1; 8192])]
    );
}

#[path = "checkpoint_rewrite_engine_tests.rs"]
mod engine;
