//! Bounded receipt construction counters, with no captured payload or key.
use std::cell::RefCell;
use std::time::Instant;

struct Totals {
    ordinal: u64,
    fields: [[u64; 9]; 3],
}
impl Default for Totals {
    fn default() -> Self {
        Self {
            ordinal: crate::writer_read_probe::ordinal(),
            fields: [[0; 9]; 3],
        }
    }
}
impl Drop for Totals {
    fn drop(&mut self) {
        for (phase, fields) in self.fields.iter().enumerate() {
            if fields[0] != 0 {
                eprintln!(
                    "riffdb-receipt-allocation-probe-v1\t{}\t{phase}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                    self.ordinal,
                    fields[0],
                    fields[1],
                    fields[2],
                    fields[3],
                    fields[4],
                    fields[5],
                    fields[6],
                    fields[7],
                    fields[8]
                );
            }
        }
    }
}
thread_local! { static TOTALS: RefCell<Totals> = RefCell::new(Totals::default()); }

/// Phase 0 builds/coalesces sources; 1 is the writer successor including 0 and
/// 2; phase 2 is history advancement. Intervals are nested and not additive.
pub(crate) struct Probe {
    phase: usize,
    started: Option<Instant>,
    fields: [u64; 9],
}
impl Probe {
    pub(crate) fn new(phase: usize) -> Self {
        Self::selected(phase, crate::writer_read_probe::enabled())
    }
    fn selected(phase: usize, enabled: bool) -> Self {
        Self {
            phase,
            started: enabled.then(Instant::now),
            fields: [1, 1, 0, 0, 0, 0, 0, 0, 0],
        }
    }
    pub(crate) fn owned(&mut self, key_bytes: usize, value_bytes: Option<usize>) {
        if self.started.is_some() {
            self.fields[2] = self.fields[2].saturating_add(1);
            self.fields[3] = self.fields[3]
                .saturating_add(1 + u64::from(value_bytes.is_some_and(|bytes| bytes > 0)));
            self.fields[4] = self.fields[4].saturating_add(key_bytes as u64);
            self.fields[5] = self.fields[5].saturating_add(value_bytes.unwrap_or(0) as u64);
        }
    }
    pub(crate) fn complete(&mut self, success: bool, output_rows: usize, encoded_bytes: usize) {
        self.fields[1] = u64::from(!success);
        self.fields[6] = output_rows as u64;
        self.fields[7] = encoded_bytes as u64;
    }
}
impl Drop for Probe {
    fn drop(&mut self) {
        if let Some(started) = self.started {
            self.fields[8] = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            TOTALS.with(|totals| {
                let mut totals = totals.borrow_mut();
                for (total, field) in totals.fields[self.phase].iter_mut().zip(self.fields) {
                    *total = total.saturating_add(field);
                }
            });
        }
    }
}

// req: PERF-008
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn receipt_probe_keeps_failed_attempts_and_zero_length_values_exact() {
        {
            let mut probe = Probe::selected(0, true);
            probe.owned(3, Some(0));
            probe.complete(true, 1, 167);
        }
        drop(Probe::selected(0, true));
        TOTALS.with(|totals| {
            let mut totals = totals.borrow_mut();
            assert_eq!(&totals.fields[0][..8], &[2, 1, 1, 1, 3, 0, 1, 167]);
            totals.fields = [[0; 9]; 3];
        });
    }
}
