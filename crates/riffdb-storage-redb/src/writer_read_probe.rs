//! Fixed-size opt-in attribution of point reads; never retains keys or values.

use std::cell::RefCell;
use std::time::Instant;

use crate::journal::JournalTable;

const ROLES: usize = 5;
static NEXT_THREAD: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
const TABLES: usize = JournalTable::ALL.len();
const SOURCES: usize = 2;

#[derive(Clone, Copy, Default)]
struct Count {
    calls: u64,
    present: u64,
    absent: u64,
    errors: u64,
    copied_bytes: u64,
    elapsed_ns: u64,
}

struct Meter {
    role: usize,
    ordinal: u64,
    counts: [[[Count; SOURCES]; TABLES]; ROLES],
}

impl Default for Meter {
    fn default() -> Self {
        Self {
            role: 0,
            ordinal: NEXT_THREAD.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            counts: [[[Count::default(); SOURCES]; TABLES]; ROLES],
        }
    }
}

impl Drop for Meter {
    fn drop(&mut self) {
        for (role, tables) in self.counts.iter().enumerate() {
            for (table, sources) in tables.iter().enumerate() {
                for (source, count) in sources.iter().enumerate() {
                    if count.calls != 0 {
                        eprintln!(
                            "riffdb-writer-read-probe-v1\t{}\t{role}\t{}\t{source}\t{}\t{}\t{}\t{}\t{}\t{}",
                            self.ordinal,
                            table + 1,
                            count.calls,
                            count.present,
                            count.absent,
                            count.errors,
                            count.copied_bytes,
                            count.elapsed_ns
                        );
                    }
                }
            }
        }
    }
}

thread_local! {
    static METER: RefCell<Meter> = RefCell::new(Meter::default());
}

/// 0 unclassified; 1 preparation snapshot; 2 writer snapshot; 3 record staging;
/// 4 mutation before-image validation. Source 0 is a logical point read and
/// source 1 is its checkpoint fallback. Their intervals can be nested.
pub(crate) struct Scope(usize);

pub(crate) fn enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(crate::query_diagnostics::query_execute_diagnostics_enabled)
}

pub(crate) fn ordinal() -> u64 {
    METER.with(|meter| meter.borrow().ordinal)
}

pub(crate) fn scope(role: usize) -> Option<Scope> {
    if !enabled() {
        return None;
    }
    Some(enter(role))
}

fn enter(role: usize) -> Scope {
    METER.with(|meter| {
        let mut meter = meter.borrow_mut();
        let previous = meter.role;
        meter.role = role.min(ROLES - 1);
        Scope(previous)
    })
}

impl Drop for Scope {
    fn drop(&mut self) {
        METER.with(|meter| meter.borrow_mut().role = self.0);
    }
}

pub(crate) fn point<E>(
    table: JournalTable,
    source: usize,
    result: &Result<Option<Vec<u8>>, E>,
    started: Instant,
) {
    METER.with(|meter| {
        let mut meter = meter.borrow_mut();
        let role = meter.role;
        let count = &mut meter.counts[role][table as usize - 1][source];
        count.calls = count.calls.saturating_add(1);
        match result {
            Ok(Some(value)) => {
                count.present = count.present.saturating_add(1);
                count.copied_bytes = count.copied_bytes.saturating_add(value.len() as u64);
            }
            Ok(None) => count.absent = count.absent.saturating_add(1),
            Err(_) => count.errors = count.errors.saturating_add(1),
        }
        count.elapsed_ns = count
            .elapsed_ns
            .saturating_add(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
    });
}

// req: PERF-008
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_read_roles_restore_and_keep_absence_distinct_from_error() {
        let outer = enter(2);
        {
            let _inner = enter(4);
            point(
                JournalTable::Entities,
                1,
                &Ok::<_, ()>(None),
                Instant::now(),
            );
            point(
                JournalTable::Entities,
                1,
                &Err::<Option<Vec<u8>>, _>(()),
                Instant::now(),
            );
        }
        point(
            JournalTable::Entities,
            0,
            &Ok::<_, ()>(Some(vec![1; 7])),
            Instant::now(),
        );
        METER.with(|meter| {
            let meter = meter.borrow();
            assert_eq!(meter.role, 2);
            let inner = meter.counts[4][JournalTable::Entities as usize - 1][1];
            assert_eq!((inner.calls, inner.absent, inner.errors), (2, 1, 1));
            let outer = meter.counts[2][JournalTable::Entities as usize - 1][0];
            assert_eq!((outer.calls, outer.present, outer.copied_bytes), (1, 1, 7));
        });
        drop(outer);
        METER.with(|meter| assert_eq!(meter.borrow().role, 0));
    }
}
