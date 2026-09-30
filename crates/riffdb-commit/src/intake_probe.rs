//! Temporary, bounded coordinator wall-time and occupancy diagnostics.
//! Rows: idle sweep, fresh formation, completion-edge formation, unit selection,
//! feedback-to-dispatch, idle receive, busy arrival, feedback receipt.
//! Columns: calls, wall nanoseconds, pending sum/max, channel sum/max.
//! Formation and selection are nested in feedback-to-dispatch when applicable.
//! These counters report neither CPU nor reasons for off-CPU time.

use std::fmt::Write as _;
use std::time::Instant;

pub(crate) struct IntakeProbe {
    enabled: bool,
    rows: [[u64; 6]; 8],
}

impl IntakeProbe {
    pub(crate) fn new() -> Self {
        Self {
            enabled: crate::writer_census::writer_batch_diagnostics_enabled(),
            rows: [[0; 6]; 8],
        }
    }

    pub(crate) fn start(&self) -> Option<Instant> {
        self.enabled.then(Instant::now)
    }

    pub(crate) fn record(
        &mut self,
        index: usize,
        started: Option<Instant>,
        pending: usize,
        channel: usize,
    ) {
        if !self.enabled {
            return;
        }
        let elapsed = started.map_or(0, |start| {
            u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
        });
        self.add(index, elapsed, pending, channel);
    }

    fn add(&mut self, index: usize, elapsed: u64, pending: usize, channel: usize) {
        let Some(row) = self.rows.get_mut(index) else {
            return;
        };
        let pending = u64::try_from(pending).unwrap_or(u64::MAX);
        let channel = u64::try_from(channel).unwrap_or(u64::MAX);
        row[0] = row[0].saturating_add(1);
        row[1] = row[1].saturating_add(elapsed);
        row[2] = row[2].saturating_add(pending);
        row[3] = row[3].max(pending);
        row[4] = row[4].saturating_add(channel);
        row[5] = row[5].max(channel);
    }
}

impl Drop for IntakeProbe {
    fn drop(&mut self) {
        if self.enabled {
            // Fixed 48 numeric fields, each at most 20 digits plus a tab.
            let mut line = String::with_capacity(1_040);
            line.push_str("riffdb-intake-probe-v1");
            for row in self.rows {
                for value in row {
                    let _ = write!(line, "\t{value}");
                }
            }
            eprintln!("{line}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::IntakeProbe;

    // req: PERF-008
    #[test]
    fn disabled_probe_does_not_capture_or_record_request_activity() {
        let mut probe = IntakeProbe {
            enabled: false,
            rows: [[0; 6]; 8],
        };
        assert!(probe.start().is_none());
        probe.record(0, None, 256, 257);
        assert_eq!(probe.rows, [[0; 6]; 8]);
    }

    // req: PERF-008
    #[test]
    fn fixed_probe_saturates_and_separates_nested_windows() {
        let mut probe = IntakeProbe {
            enabled: false,
            rows: [[0; 6]; 8],
        };
        probe.add(4, 100, 2, 3);
        probe.add(1, 70, 4, 5);
        assert_eq!(probe.rows[4], [1, 100, 2, 2, 3, 3]);
        assert_eq!(probe.rows[1], [1, 70, 4, 4, 5, 5]);
        probe.rows[4][0] = u64::MAX;
        probe.rows[4][1] = u64::MAX;
        probe.add(4, 100, 1, 1);
        probe.add(8, 100, 1, 1);
        assert_eq!(probe.rows[4], [u64::MAX, u64::MAX, 3, 2, 4, 3]);
    }
}
