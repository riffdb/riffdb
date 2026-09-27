//! The writeback budget bounds submitted bytes, not elapsed I/O time.
// req: STO-001, REC-001, REC-002

use super::*;

#[path = "checkpoint_writeback_engine_tests.rs"]
mod engine;

#[derive(Debug, Default)]
struct RecordedIo {
    bytes: Vec<u8>,
    unsynced: usize,
    largest_unsynced: usize,
    syncs: usize,
    fail_sync: bool,
}

#[derive(Debug, Default, Clone)]
struct RecordingBackend(Arc<Mutex<RecordedIo>>);

impl StorageBackend for RecordingBackend {
    fn len(&self) -> io::Result<u64> {
        Ok(self.0.lock().unwrap().bytes.len() as u64)
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        let record = self.0.lock().unwrap();
        let start = usize::try_from(offset).unwrap();
        out.copy_from_slice(&record.bytes[start..start + out.len()]);
        Ok(())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.0.lock().unwrap().bytes.resize(len as usize, 0);
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        let mut record = self.0.lock().unwrap();
        record.syncs += 1;
        if record.fail_sync {
            return Err(io::Error::other("injected sync failure"));
        }
        record.unsynced = 0;
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        let mut record = self.0.lock().unwrap();
        let start = usize::try_from(offset).unwrap();
        let len = record.bytes.len().max(start + data.len());
        record.bytes.resize(len, 0);
        record.bytes[start..start + data.len()].copy_from_slice(data);
        record.unsynced += data.len();
        record.largest_unsynced = record.largest_unsynced.max(record.unsynced);
        Ok(())
    }
}

#[test]
fn checkpoint_writeback_bounds_submitted_bytes_and_keeps_the_final_fence() {
    let state = Arc::new(CheckpointWriteback::default());
    let inner = RecordingBackend::default();
    let backend = CheckpointWritebackBackend::new(inner.clone(), Arc::clone(&state));
    let _scope = state.enter().unwrap();
    let data = vec![0x6b; WRITEBACK_BYTES * 2 + 1];
    backend.write(0, &data).unwrap();
    backend.sync_data().unwrap();
    let record = inner.0.lock().unwrap();
    assert_eq!(
        record.bytes, data,
        "chunking preserves every byte and offset"
    );
    assert!(record.largest_unsynced <= WRITEBACK_BYTES);
    assert_eq!(
        record.syncs, 3,
        "two early fences plus the final engine fence"
    );
    assert_eq!(record.unsynced, 0);
}

#[test]
fn checkpoint_writeback_leaves_ordinary_writes_unchanged_and_resets_scope() {
    let state = Arc::new(CheckpointWriteback::default());
    let inner = RecordingBackend::default();
    let backend = CheckpointWritebackBackend::new(inner.clone(), Arc::clone(&state));
    let data = vec![0x7c; WRITEBACK_BYTES + 1];
    backend.write(3, &data).unwrap();
    assert_eq!(inner.0.lock().unwrap().syncs, 0);
    assert_eq!(inner.0.lock().unwrap().unsynced, data.len());
    backend.sync_data().unwrap();
    for _ in 0..2 {
        let scope = state.enter().unwrap();
        assert!(state.enter().is_err(), "nested ownership is refused");
        backend.write(3, &data[..WRITEBACK_BYTES]).unwrap();
        backend.sync_data().unwrap();
        backend.sync_data().unwrap();
        drop(scope);
    }
    assert_eq!(inner.0.lock().unwrap().syncs, 5, "no final fence elision");
    let mut read = vec![0; data.len()];
    backend.read(3, &mut read).unwrap();
    assert_eq!(read, data);
    assert_eq!(backend.len().unwrap(), (data.len() + 3) as u64);
}

#[test]
fn checkpoint_writeback_sync_failure_stops_before_the_next_byte() {
    let state = Arc::new(CheckpointWriteback::default());
    let inner = RecordingBackend::default();
    let backend = CheckpointWritebackBackend::new(inner.clone(), Arc::clone(&state));
    let _scope = state.enter().unwrap();
    inner.0.lock().unwrap().fail_sync = true;
    assert!(backend.write(0, &vec![0x2a; WRITEBACK_BYTES + 1]).is_err());
    assert_eq!(inner.0.lock().unwrap().bytes.len(), WRITEBACK_BYTES);
    assert!(backend.write(WRITEBACK_BYTES as u64, &[0x2b]).is_err());
    assert_eq!(inner.0.lock().unwrap().bytes.len(), WRITEBACK_BYTES);
    assert_eq!(inner.0.lock().unwrap().syncs, 2);
}

#[test]
fn checkpoint_writeback_rejects_offset_overflow_without_partial_io() {
    let state = Arc::new(CheckpointWriteback::default());
    let inner = RecordingBackend::default();
    let backend = CheckpointWritebackBackend::new(inner.clone(), Arc::clone(&state));
    let _scope = state.enter().unwrap();
    assert_eq!(
        backend.write(u64::MAX, &[1]).unwrap_err().kind(),
        io::ErrorKind::InvalidInput,
    );
    assert!(inner.0.lock().unwrap().bytes.is_empty());
    assert_eq!(inner.0.lock().unwrap().syncs, 0);
}

#[derive(Debug)]
struct PausedBackend {
    inner: RecordingBackend,
    entered: std::sync::mpsc::SyncSender<()>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
    first: std::sync::atomic::AtomicBool,
}

impl StorageBackend for PausedBackend {
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
        self.inner.sync_data()
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        if self.first.swap(false, std::sync::atomic::Ordering::SeqCst) {
            self.entered.send(()).unwrap();
            self.release
                .lock()
                .unwrap()
                .recv_timeout(std::time::Duration::from_secs(30))
                .unwrap();
        }
        self.inner.write(offset, data)
    }
}

#[test]
fn checkpoint_writeback_serializes_the_budget_across_backend_callers() {
    let state = Arc::new(CheckpointWriteback::default());
    let inner = RecordingBackend::default();
    let (entered, entry) = std::sync::mpsc::sync_channel(1);
    let (release, released) = std::sync::mpsc::sync_channel(1);
    let backend = Arc::new(CheckpointWritebackBackend::new(
        PausedBackend {
            inner: inner.clone(),
            entered,
            release: Mutex::new(released),
            first: std::sync::atomic::AtomicBool::new(true),
        },
        Arc::clone(&state),
    ));
    let _scope = state.enter().unwrap();
    let first_backend = Arc::clone(&backend);
    let first = std::thread::spawn(move || {
        first_backend
            .write(0, &vec![0x1a; WRITEBACK_BYTES])
            .unwrap()
    });
    entry
        .recv_timeout(std::time::Duration::from_secs(30))
        .unwrap();
    let (attempt, attempted) = std::sync::mpsc::sync_channel(1);
    let second_backend = Arc::clone(&backend);
    let second = std::thread::spawn(move || {
        attempt.send(()).unwrap();
        second_backend
            .write(WRITEBACK_BYTES as u64, &vec![0x2b; WRITEBACK_BYTES])
            .unwrap();
    });
    attempted
        .recv_timeout(std::time::Duration::from_secs(30))
        .unwrap();
    assert!(
        inner.0.lock().unwrap().bytes.is_empty(),
        "first writer holds the I/O boundary"
    );
    // Reads bypass the writeback lock, including while a physical write waits.
    assert_eq!(backend.len().unwrap(), 0);
    release.send(()).unwrap();
    first.join().unwrap();
    second.join().unwrap();
    backend.sync_data().unwrap();
    let record = inner.0.lock().unwrap();
    assert_eq!(record.largest_unsynced, WRITEBACK_BYTES);
    assert_eq!(record.syncs, 2);
    assert!(record.bytes[..WRITEBACK_BYTES].iter().all(|b| *b == 0x1a));
    assert!(record.bytes[WRITEBACK_BYTES..].iter().all(|b| *b == 0x2b));
}
