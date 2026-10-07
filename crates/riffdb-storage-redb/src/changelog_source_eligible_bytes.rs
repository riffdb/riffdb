//! Actual canonical source-receipt bytes below one validated native floor.
//! This read-only component never authorizes deletion or claims aggregate history.
use super::*;
use std::time::{Duration, Instant};

const MAX_ROWS: usize = 256;
const MAX_AGE: Duration = Duration::from_secs(30);
const CAPTURE_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Default)]
pub(super) struct Observer {
    probe: Option<Probe>,
    next_capture: Option<Instant>,
}

impl Observer {
    pub(super) fn pending(&self) -> bool {
        self.probe
            .as_ref()
            .is_some_and(|probe| probe.root.is_some() && probe.completed.is_none())
    }

    // Called only with the root and ceiling of the parent's validated barrier.
    // No I/O, scan or new reader capture occurs under that writer lease.
    pub(super) fn capture(
        &mut self,
        root: Arc<crate::checkpoint_root::CheckpointRoot>,
        history: History,
        ceiling: u64,
        now: Instant,
    ) -> Result<(), StorageError> {
        if self.next_capture.is_some_and(|next| now < next) {
            return Ok(());
        }
        self.probe = Some(Probe::new(root, history, ceiling, now)?);
        self.next_capture = Some(
            now.checked_add(CAPTURE_INTERVAL)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?,
        );
        Ok(())
    }

    pub(super) fn advance(&mut self, now: Instant) -> Result<(), StorageError> {
        let Some(probe) = self.probe.as_mut() else {
            return Ok(());
        };
        let was_complete = probe.completed.is_some();
        if let Err(error) = probe.advance(now, MAX_ROWS) {
            self.probe = None;
            return Err(error);
        }
        // The bounded page itself may have exceeded freshness; do not publish it.
        let observed = Instant::now();
        if observed.saturating_duration_since(probe.captured) >= MAX_AGE {
            self.probe = None;
        } else if !was_complete && probe.completed.is_some() {
            probe.completed = Some(observed);
            let sample = probe
                .sample(observed)
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
            let [
                root,
                incarnation,
                first,
                ceiling,
                rows,
                bytes,
                key_bytes,
                age_us,
                collection_us,
                pages,
            ] = sample;
            eprintln!(
                "riffdb-retention-source-bytes-v1\troot={root}\tincarnation={incarnation}\tresume={first}\tceiling={ceiling}\trows={rows}\tcanonical_receipt_bytes={bytes}\tkey_bytes={key_bytes}\tcapture_age_us={age_us}\tcollection_us={collection_us}\tpages={pages}"
            );
        }
        Ok(())
    }

    pub(super) fn retire(&mut self) {
        self.probe = None;
    }
}

struct Probe {
    root: Option<Arc<crate::checkpoint_root::CheckpointRoot>>,
    root_identity: u64,
    history: History,
    ceiling: u64,
    next: u64,
    bytes: u64,
    key_bytes: u64,
    rows: u64,
    pages: u64,
    captured: Instant,
    completed: Option<Instant>,
}

impl Probe {
    fn new(
        root: Arc<crate::checkpoint_root::CheckpointRoot>,
        history: History,
        ceiling: u64,
        now: Instant,
    ) -> Result<Self, StorageError> {
        if ceiling > history.tail().sequence().get() {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        Ok(Self {
            root_identity: root.identity(),
            root: Some(root),
            history,
            ceiling,
            next: history.minimum_resume().sequence().get(),
            bytes: 0,
            key_bytes: 0,
            rows: 0,
            pages: 0,
            captured: now,
            completed: None,
        })
    }

    fn advance(&mut self, now: Instant, limit: usize) -> Result<(), StorageError> {
        if limit == 0 || limit > MAX_ROWS {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        if now.saturating_duration_since(self.captured) >= MAX_AGE {
            self.root = None;
            self.completed = None;
            return Ok(());
        }
        let Some(root) = self.root.as_ref() else {
            return Ok(());
        };
        let table = root.open_table(HISTORY).map_err(table_error)?;
        let mut page_bytes = 0_usize;
        for _ in 0..limit {
            if self.next >= self.ceiling {
                break;
            }
            let key = self.next.to_be_bytes();
            let row = table
                .get(key.as_slice())
                .map_err(precommit_storage_error)?
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
            let encoded = crate::history_stub::resolve_history_value(row.value())?;
            if encoded.len() > riffdb_storage_api::MAX_CHANGELOG_FRAME_BYTES {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            let next_bytes = page_bytes
                .checked_add(encoded.len())
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            if next_bytes > riffdb_storage_api::MAX_CHANGELOG_FRAME_BYTES {
                break;
            }
            let receipt = AuthoritativeTransactionV3::decode(&encoded).map_err(value_error)?;
            let binding = receipt.binding();
            if binding.sequence.get() != self.next
                || binding.database_id != self.history.lineage().database_id()
                || binding.history_incarnation != self.history.lineage().history_incarnation()
            {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            self.bytes = self
                .bytes
                .checked_add(
                    u64::try_from(encoded.len())
                        .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?,
                )
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            self.key_bytes = self
                .key_bytes
                .checked_add(
                    u64::try_from(key.len())
                        .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?,
                )
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            self.rows = self
                .rows
                .checked_add(1)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            self.next = self
                .next
                .checked_add(1)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            page_bytes = next_bytes;
        }
        self.pages = self
            .pages
            .checked_add(1)
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        if self.next >= self.ceiling {
            self.completed = Some(now);
            self.root = None;
        }
        Ok(())
    }

    fn sample(&self, now: Instant) -> Option<[u64; 10]> {
        let completed = self.completed?;
        if now < completed || now.saturating_duration_since(self.captured) >= MAX_AGE {
            return None;
        }
        Some([
            self.root_identity,
            self.history.lineage().history_incarnation(),
            self.history.minimum_resume().sequence().get(),
            self.ceiling,
            self.rows,
            self.bytes,
            self.key_bytes,
            u64::try_from(now.saturating_duration_since(self.captured).as_micros()).ok()?,
            u64::try_from(
                completed
                    .saturating_duration_since(self.captured)
                    .as_micros(),
            )
            .ok()?,
            self.pages,
        ])
    }

    #[cfg(test)]
    fn bytes(&self, now: Instant) -> Option<u64> {
        self.sample(now).map(|sample| sample[5])
    }
}

#[cfg(test)]
#[path = "changelog_source_eligible_bytes_tests.rs"]
mod tests;
