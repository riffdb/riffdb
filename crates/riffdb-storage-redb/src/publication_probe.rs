//! Temporary bounded publication cost census. Durations are nested wall time,
//! not CPU time. No diagnostic changes cache decisions or publication ordering.
use std::fmt::Write as _;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

const PHASES: usize = 10;
static ENABLED: OnceLock<bool> = OnceLock::new();
static TIMES: [[AtomicU64; 3]; PHASES] = [const { [const { AtomicU64::new(0) }; 3] }; PHASES];
static PAYLOAD: [AtomicU64; 3] = [const { AtomicU64::new(0) }; 3];

#[derive(Clone, Copy)]
#[repr(usize)]
pub(crate) enum Phase {
    TransientLockAcquire,
    TransientLockHeld,
    SegmentMetadata,
    ManifestValidationInsertion,
    PayloadMetadataValidation,
    PayloadDecodeVerification,
    PayloadDecodedCharge,
    PayloadCacheLockAcquire,
    PayloadEvictionDrop,
    PayloadInsertion,
}

pub(crate) struct Timer {
    phase: Phase,
    started: Option<Instant>,
}

fn enabled() -> bool {
    *ENABLED.get_or_init(|| {
        std::env::var_os("RIFFDB_WRITER_BATCH_DIAGNOSTICS").is_some_and(|v| v == "1")
    })
}

pub(crate) fn start(phase: Phase) -> Timer {
    Timer {
        phase,
        started: enabled().then(Instant::now),
    }
}

fn add(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

impl Drop for Timer {
    fn drop(&mut self) {
        if let Some(started) = self.started {
            let elapsed = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let row = &TIMES[self.phase as usize];
            add(&row[0], 1);
            add(&row[1], elapsed);
            row[2].fetch_max(elapsed, Ordering::Relaxed);
        }
    }
}

pub(crate) fn eviction(canonical_bytes: usize, decoded_charge: usize) {
    if enabled() {
        add(&PAYLOAD[0], 1);
        add(&PAYLOAD[1], canonical_bytes as u64);
        add(&PAYLOAD[2], decoded_charge as u64);
    }
}

pub(crate) fn report() {
    if !enabled() {
        return;
    }
    let mut line = String::with_capacity(850);
    line.push_str("riffdb-publication-cost-probe-v1");
    for row in &TIMES {
        for counter in row {
            let _ = write!(line, "\t{}", counter.load(Ordering::Relaxed));
        }
    }
    for counter in &PAYLOAD {
        let _ = write!(line, "\t{}", counter.load(Ordering::Relaxed));
    }
    eprintln!("{line}");
}
