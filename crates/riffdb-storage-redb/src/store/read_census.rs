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
