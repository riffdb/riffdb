//! Bounded read-work observations; no row values leave storage.
use super::*;

impl SharedRedb {
    /// ADR-0236 obligation three: the bounded history fallback is the only
    /// absence backstop, so both how often it runs and how much it reads are
    /// evidence. `rows` is the span it covered, bounded by the distance from
    /// the checkpoint application frontier to the captured frontier.
    pub(super) fn note_fresh_locator_history_fallback_scan(&self, rows: u64) {
        if let Some(controller) = &self.test_controller {
            controller.observe_fresh_locator_history_fallback_scan(rows);
        }
        let _ = self.fresh_locator_history_fallback_scans.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_add(1)),
        );
        let _ = self.fresh_locator_history_fallback_rows.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_add(rows)),
        );
    }

    pub(crate) fn observe_indexed_selection(&self, entries: usize) {
        if let Some(controller) = &self.test_controller {
            controller.observe_indexed_selection(entries);
        }
    }
}

impl RedbWriteAccess {
    pub(crate) fn observe_indexed_selection(&self, entries: usize) {
        self.shared.observe_indexed_selection(entries);
    }
}

// Temporary fixed-cardinality diagnostic; no values, keys, or identities.
struct Wp820Probe {
    values: std::cell::Cell<[u64; 112]>,
    role: u8,
}
impl Drop for Wp820Probe {
    fn drop(&mut self) {
        eprintln!(
            "riffdb-wp820-read-probe-v1\t{}\t{}\t{:?}",
            std::process::id(),
            self.role,
            self.values.get()
        );
    }
}
thread_local! {
    static WP820_PROBE: Option<Wp820Probe> = if std::env::var_os("RIFFDB_WP820_COPY_DIAGNOSTICS").is_some_and(|v| v == "1") {
        Some(Wp820Probe { values: std::cell::Cell::new([0; 112]), role: match std::thread::current().name() { Some("riffdb-command-writer") => 1, Some(n) if n.starts_with("riffdb-command-prepare-") => 2, Some("riffdb-command-coordinator") => 3, Some("riffdb-command-completion") => 4, _ => 0 } })
    } else { None };
}
pub(crate) fn wp820_count(index: usize, amount: usize) {
    WP820_PROBE.with(|probe| {
        if let Some(probe) = probe {
            let mut values = probe.values.get();
            values[index] = values[index].saturating_add(amount as u64);
            probe.values.set(values);
        }
    });
}
