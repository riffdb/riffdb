//! Temporary bounded positive current-observation reuse census.
//! Three rows: binding, root validation, cascade. Eight columns: calls,
//! cached present, cached absent, uncached, cached-present encoded bytes,
//! inconsistent cached-present bytes, positive-read wall ns, other-read wall ns.
//! No reads are reused by this diagnostic build. Durations are wall time.

use std::fmt::Write as _;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static ENABLED: OnceLock<bool> = OnceLock::new();
static COUNTERS: [[AtomicU64; 8]; 3] = [const { [const { AtomicU64::new(0) }; 8] }; 3];

fn enabled() -> bool {
    *ENABLED.get_or_init(|| {
        std::env::var_os("RIFFDB_WRITER_BATCH_DIAGNOSTICS").is_some_and(|value| value == "1")
    })
}

pub(crate) fn start() -> Option<Instant> {
    enabled().then(Instant::now)
}

pub(crate) fn record(
    role: usize,
    cached_present: bool,
    cached_absent: bool,
    encoded_len: Option<usize>,
    started: Option<Instant>,
) {
    let Some(started) = started else {
        return;
    };
    let Some(row) = COUNTERS.get(role) else {
        return;
    };
    let counts = [
        1,
        u64::from(cached_present),
        u64::from(cached_absent),
        u64::from(!cached_present && !cached_absent),
        if cached_present {
            encoded_len.map_or(0, |len| u64::try_from(len).unwrap_or(u64::MAX))
        } else {
            0
        },
        u64::from(cached_present && encoded_len.is_none()),
        if cached_present {
            u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
        } else {
            0
        },
        if !cached_present {
            u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
        } else {
            0
        },
    ];
    for (counter, count) in row.iter().zip(counts) {
        let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_add(count))
        });
    }
}

pub(crate) fn report() {
    if !enabled() {
        return;
    }
    let mut line = String::with_capacity(550);
    line.push_str("riffdb-snapshot-cache-probe-v1");
    for row in &COUNTERS {
        for counter in row {
            let _ = write!(line, "\t{}", counter.load(Ordering::Relaxed));
        }
    }
    eprintln!("{line}");
}
