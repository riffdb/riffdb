//! Temporary exact-byte positive snapshot repeat census, private to one batch.
//! It never answers a read. At most 64 entries and 64 KiB of copied key/value
//! payload are retained; fixed slot/header storage is reported separately.
//! Rows: binding, root validation, cascade. Columns: successful reads,
//! existing positive, existing absence, cold positive, cold absence, exact
//! repeat, changed value, budget skip, copied key bytes, copied value bytes,
//! exact-repeat read wall ns, other read wall ns. Errors remain on the read path.

use std::fmt::Write as _;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

const ENTRY_LIMIT: usize = 64;
const PAYLOAD_LIMIT: usize = 64 * 1024;
const COLUMNS: usize = 12;
static ENABLED: OnceLock<bool> = OnceLock::new();
static COUNTERS: [[AtomicU64; COLUMNS]; 3] = [const { [const { AtomicU64::new(0) }; COLUMNS] }; 3];
static PEAK_PAYLOAD: AtomicU64 = AtomicU64::new(0);
static PEAK_ENTRIES: AtomicU64 = AtomicU64::new(0);
static BATCHES: AtomicU64 = AtomicU64::new(0);
static SKIPPED_RECORDS: AtomicU64 = AtomicU64::new(0);

struct Entry {
    key: Box<[u8]>,
    value: Box<[u8]>,
}

pub(crate) struct BatchProbe {
    entries: Box<[Option<Entry>; ENTRY_LIMIT]>,
    payload_bytes: usize,
    entry_count: usize,
    counters: [[u64; COLUMNS]; 3],
}

fn enabled() -> bool {
    *ENABLED.get_or_init(|| {
        std::env::var_os("RIFFDB_WRITER_BATCH_DIAGNOSTICS").is_some_and(|value| value == "1")
    })
}

impl BatchProbe {
    pub(crate) fn new() -> Option<Self> {
        enabled().then(Self::empty)
    }

    fn empty() -> Self {
        Self {
            entries: Box::new(std::array::from_fn(|_| None)),
            payload_bytes: 0,
            entry_count: 0,
            counters: [[0; COLUMNS]; 3],
        }
    }

    pub(crate) fn record(
        &mut self,
        role: usize,
        existing: u8,
        key: &[u8],
        value: Option<&[u8]>,
        read_wall_ns: u64,
    ) {
        let Some(row) = self.counters.get_mut(role) else {
            return;
        };
        row[0] = row[0].saturating_add(1);
        if existing != 0 {
            let column = if existing == 1 { 1 } else { 2 };
            row[column] = row[column].saturating_add(1);
            row[11] = row[11].saturating_add(read_wall_ns);
            return;
        }
        let Some(value) = value else {
            row[4] = row[4].saturating_add(1);
            row[11] = row[11].saturating_add(read_wall_ns);
            return;
        };
        row[3] = row[3].saturating_add(1);
        if let Some(slot) = self
            .entries
            .iter_mut()
            .find(|slot| slot.as_ref().is_some_and(|entry| entry.key.as_ref() == key))
        {
            if slot
                .as_ref()
                .is_some_and(|entry| entry.value.as_ref() == value)
            {
                row[5] = row[5].saturating_add(1);
                row[10] = row[10].saturating_add(read_wall_ns);
                return;
            }
            row[6] = row[6].saturating_add(1);
            if let Some(prior) = slot.take() {
                self.payload_bytes -= prior.key.len() + prior.value.len();
                self.entry_count -= 1;
            }
        }
        row[11] = row[11].saturating_add(read_wall_ns);
        let Some(next_payload) = self
            .payload_bytes
            .checked_add(key.len())
            .and_then(|bytes| bytes.checked_add(value.len()))
            .filter(|bytes| *bytes <= PAYLOAD_LIMIT)
        else {
            row[7] = row[7].saturating_add(1);
            return;
        };
        let Some(slot) = self.entries.iter_mut().find(|slot| slot.is_none()) else {
            row[7] = row[7].saturating_add(1);
            return;
        };
        *slot = Some(Entry {
            key: key.into(),
            value: value.into(),
        });
        self.payload_bytes = next_payload;
        self.entry_count += 1;
        row[8] = row[8].saturating_add(key.len() as u64);
        row[9] = row[9].saturating_add(value.len() as u64);
        PEAK_PAYLOAD.fetch_max(next_payload as u64, Ordering::Relaxed);
        PEAK_ENTRIES.fetch_max(self.entry_count as u64, Ordering::Relaxed);
    }
}

impl Drop for BatchProbe {
    fn drop(&mut self) {
        let _ = BATCHES.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_add(1))
        });
        for (global, local) in COUNTERS.iter().zip(self.counters) {
            for (counter, count) in global.iter().zip(local) {
                let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some(current.saturating_add(count))
                });
            }
        }
    }
}

pub(crate) fn skipped_record() {
    let _ = SKIPPED_RECORDS.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(1))
    });
}

pub(crate) fn report() {
    if !enabled() {
        return;
    }
    let mut line = String::with_capacity(1100);
    line.push_str("riffdb-snapshot-repeat-probe-v1");
    for row in &COUNTERS {
        for counter in row {
            let _ = write!(line, "\t{}", counter.load(Ordering::Relaxed));
        }
    }
    for value in [
        PEAK_PAYLOAD.load(Ordering::Relaxed),
        PEAK_ENTRIES.load(Ordering::Relaxed),
        PAYLOAD_LIMIT as u64,
        ENTRY_LIMIT as u64,
        std::mem::size_of::<[Option<Entry>; ENTRY_LIMIT]>() as u64,
        std::mem::size_of::<Option<std::cell::RefCell<BatchProbe>>>() as u64,
        BATCHES.load(Ordering::Relaxed),
        SKIPPED_RECORDS.load(Ordering::Relaxed),
    ] {
        let _ = write!(line, "\t{value}");
    }
    eprintln!("{line}");
}

#[cfg(test)]
mod tests {
    use super::*;

    // req: PERF-008, TXN-041
    #[test]
    fn exact_repeats_exclude_changed_bytes_absence_and_existing_observations() {
        let mut probe = BatchProbe::empty();
        probe.record(0, 0, b"key", Some(b"first"), 10);
        probe.record(0, 0, b"key", Some(b"first"), 20);
        probe.record(1, 0, b"key", Some(b"changed"), 30);
        probe.record(2, 0, b"missing", None, 40);
        probe.record(0, 1, b"key", Some(b"changed"), 50);
        probe.record(0, 2, b"key", Some(b"changed"), 60);
        assert_eq!(probe.counters[0][5], 1);
        assert_eq!(probe.counters[0][10], 20);
        assert_eq!(probe.counters[1][6], 1);
        assert_eq!(probe.counters[2][4], 1);
        assert_eq!(probe.entry_count, 1);
        assert_eq!(probe.payload_bytes, b"key".len() + b"changed".len());
        assert_eq!(probe.counters[0][1..3], [1, 1]);
    }

    // req: PERF-008
    #[test]
    fn entry_and_payload_limits_refuse_extra_diagnostic_retention() {
        let mut probe = BatchProbe::empty();
        for ordinal in 0..=ENTRY_LIMIT {
            probe.record(0, 0, &ordinal.to_be_bytes(), Some(b"value"), 0);
        }
        assert_eq!(probe.entry_count, ENTRY_LIMIT);
        assert_eq!(probe.counters[0][7], 1);
        let mut large = BatchProbe::empty();
        large.record(0, 0, b"key", Some(&vec![0; PAYLOAD_LIMIT]), 0);
        assert_eq!(large.payload_bytes, 0);
        assert_eq!(large.entry_count, 0);
        assert_eq!(large.counters[0][7], 1);
    }
}
