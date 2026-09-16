//! Process-local, bounded custody of a selected derived provider's V3 tail.
//! These pins grant reads only. Restart falls back to the validated checkpoint
//! and available history; no process-local claim is persisted as replay proof.

use std::sync::{Arc, Mutex, MutexGuard, Weak};

use riffdb_storage_api::{
    ChangelogCursorErrorV3, ChangelogHistoryPointV3, ChangelogHistoryStateV3, ChangelogLineageV3,
    ChangelogReceiptCursorV3, StorageError, StorageErrorKind,
};

use crate::error::storage_error;
use crate::store::RedbReadAccess;

const MAX_DERIVED_SOURCE_PINS: usize = 2048;

struct Position {
    lineage: ChangelogLineageV3,
    point: ChangelogHistoryPointV3,
}

#[derive(Default)]
struct Pins {
    live: Vec<Weak<Position>>,
    // A reclamation reservation is conservative even when its engine outcome
    // is unknown. No later registration may retroactively veto that deletion.
    reserved_floor: Option<(ChangelogLineageV3, ChangelogHistoryPointV3)>,
}

#[derive(Default)]
pub(crate) struct DerivedSourcePins(Mutex<Pins>);

impl DerivedSourcePins {
    fn register(
        &self,
        lineage: ChangelogLineageV3,
        point: ChangelogHistoryPointV3,
    ) -> Result<Arc<Position>, StorageError> {
        let mut pins = self.0.lock().map_err(|_| integrity())?;
        if pins
            .reserved_floor
            .is_some_and(|(owner, floor)| owner == lineage && point.sequence() < floor.sequence())
        {
            return Err(storage_error(StorageErrorKind::HistoryPruned));
        }
        pins.live.retain(|pin| pin.strong_count() != 0);
        if pins.live.len() == MAX_DERIVED_SOURCE_PINS {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let position = Arc::new(Position { lineage, point });
        pins.live.push(Arc::downgrade(&position));
        Ok(position)
    }

    pub(crate) fn reclamation(&self) -> Result<Reclamation<'_>, StorageError> {
        self.0.lock().map(Reclamation).map_err(|_| integrity())
    }
}

/// Held from floor selection through the engine outcome. Registration never
/// holds this mutex while acquiring a writer gate or opening a new read root.
pub(crate) struct Reclamation<'a>(MutexGuard<'a, Pins>);

impl Reclamation<'_> {
    pub(crate) fn oldest(&mut self, lineage: ChangelogLineageV3) -> Option<u64> {
        self.0.live.retain(|pin| pin.strong_count() != 0);
        self.0
            .live
            .iter()
            .filter_map(Weak::upgrade)
            .filter(|pin| pin.lineage == lineage)
            .map(|pin| pin.point.sequence().get())
            .min()
    }

    pub(crate) fn reserve(&mut self, lineage: ChangelogLineageV3, floor: ChangelogHistoryPointV3) {
        self.0.reserved_floor = Some((lineage, floor));
    }
}

/// One immutable validated source view and an exact, retained resume position.
/// Clones share one bounded registration. Dropping the last clone releases it.
#[derive(Clone)]
pub struct RedbDerivedSourcePin {
    access: RedbReadAccess,
    history: ChangelogHistoryStateV3,
    position: Arc<Position>,
    pins: Arc<DerivedSourcePins>,
}

impl RedbDerivedSourcePin {
    pub(crate) fn capture(
        access: RedbReadAccess,
        pins: Arc<DerivedSourcePins>,
    ) -> Result<Self, ChangelogCursorErrorV3> {
        let history = crate::changelog_v3_cursor::history(&access)?;
        // Attached follower roots may legitimately retain only the exact applied
        // receipt hash and no SourceOnly history rows. `history` has already
        // validated that special root shape. It supports a full snapshot rebuild,
        // but cannot prove deltas; a missing row in nonempty history still fails.
        if let RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) = &access {
            use redb::ReadableTableMetadata;
            if root
                .open_table(crate::changelog_v3_activation::HISTORY)
                .map_err(crate::error::table_error)?
                .is_empty()
                .map_err(crate::error::precommit_storage_error)?
            {
                return Err(storage_error(StorageErrorKind::HistoryPruned).into());
            }
        }
        // Also prove the exact terminal receipt against its advertised hash and
        // frontier, rather than upgrading an advisory observation into proof.
        drop(crate::changelog_v3_cursor::open(
            &access,
            history.lineage(),
            history.tail(),
        )?);
        let position = pins.register(history.lineage(), history.tail())?;
        Ok(Self {
            access,
            history,
            position,
            pins,
        })
    }

    /// Exact immutable lineage and retained history observed by this view.
    #[must_use]
    pub const fn history(&self) -> ChangelogHistoryStateV3 {
        self.history
    }

    /// Resume position retained until the last clone is released.
    #[must_use]
    pub fn position(&self) -> ChangelogHistoryPointV3 {
        self.position.point
    }

    /// Reads complete checked successors from this immutable view. Source
    /// substitution, missing history, and invalid hash/frontier resumes refuse.
    pub fn receipts_after(
        &self,
        previous: &Self,
    ) -> Result<Box<dyn ChangelogReceiptCursorV3>, ChangelogCursorErrorV3> {
        if self.history.lineage() != previous.history.lineage()
            || !Arc::ptr_eq(&self.pins, &previous.pins)
        {
            return Err(ChangelogCursorErrorV3::ForeignLineage);
        }
        crate::changelog_v3_cursor::open(&self.access, self.history.lineage(), previous.position())
    }

    /// Retains a proved interior position after a bounded worker pass. The
    /// caller cannot invent an intermediate application frontier inside a receipt.
    pub fn at(&self, point: ChangelogHistoryPointV3) -> Result<Self, ChangelogCursorErrorV3> {
        drop(crate::changelog_v3_cursor::open(
            &self.access,
            self.history.lineage(),
            point,
        )?);
        let position = self.pins.register(self.history.lineage(), point)?;
        Ok(Self {
            access: self.access.clone(),
            history: self.history,
            position,
            pins: Arc::clone(&self.pins),
        })
    }
}

fn integrity() -> StorageError {
    storage_error(StorageErrorKind::InvariantViolation)
}

#[cfg(test)]
mod tests {
    use super::*;

    // req: PRJ-002, PRJ-004
    #[test]
    fn derived_source_pin_population_is_bounded_and_last_clone_releases_custody() {
        let (_scope, _ports, states) = crate::changelog_v3_control_tests::fixture();
        let source = states[0];
        let pins = DerivedSourcePins::default();
        let mut live = (0..MAX_DERIVED_SOURCE_PINS)
            .map(|_| pins.register(source.lineage(), source.tail()).unwrap())
            .collect::<Vec<_>>();
        assert!(
            matches!(pins.register(source.lineage(), source.tail()), Err(error) if error.kind() == StorageErrorKind::LimitExceeded)
        );
        let retained = Arc::clone(live.last().unwrap());
        live.pop();
        assert!(pins.register(source.lineage(), source.tail()).is_err());
        drop(retained);
        assert!(pins.register(source.lineage(), source.tail()).is_ok());
    }

    // req: PRJ-002, PRJ-004
    #[test]
    fn derived_source_registration_and_reclamation_have_two_safe_orders() {
        let (_scope, _ports, states) = crate::changelog_v3_control_tests::fixture();
        let pins = Arc::new(DerivedSourcePins::default());
        let lineage = states[0].lineage();
        let old = states[1].tail();
        let later = states[3].tail();
        let selected = pins.register(lineage, old).unwrap();
        assert_eq!(
            pins.reclamation().unwrap().oldest(lineage),
            Some(old.sequence().get())
        );
        drop(selected);
        // Hold the reclamation critical section while another thread enters
        // registration. The channel orders the attempts; no sleeps decide it.
        let mut reclaim = pins.reclamation().unwrap();
        let (entered, observed) = std::sync::mpsc::sync_channel(0);
        let worker_pins = Arc::clone(&pins);
        let worker = std::thread::spawn(move || {
            assert!(worker_pins.0.try_lock().is_err());
            entered.send(()).unwrap();
            worker_pins.register(lineage, old)
        });
        observed.recv().unwrap();
        assert_eq!(reclaim.oldest(lineage), None);
        reclaim.reserve(lineage, later);
        drop(reclaim);
        assert!(
            matches!(worker.join().unwrap(), Err(error) if error.kind() == StorageErrorKind::HistoryPruned)
        );
        assert!(pins.register(lineage, later).is_ok());
    }
}
