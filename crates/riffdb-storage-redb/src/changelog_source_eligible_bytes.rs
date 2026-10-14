//! Actual canonical source-receipt bytes below one validated native floor.
//! This read-only component never authorizes deletion or claims aggregate history.
use super::*;
use crate::source_validation_pin::ValidationSource;
use crate::source_validation_read::ReceiptRead;
use std::time::{Duration, Instant};

const MAX_ROWS: usize = 256;
const MAX_AGE: Duration = Duration::from_secs(30);
const CAPTURE_INTERVAL: Duration = Duration::from_secs(60);

#[path = "changelog_source_eligible_continuation.rs"]
mod continuation;
use continuation::ContinuedProbe;

#[derive(Default)]
pub(super) struct Observer {
    probe: Option<Probe>,
    continued: Option<ContinuedProbe>,
    next_capture: Option<Instant>,
}

impl Observer {
    #[cfg(test)]
    pub(super) fn sample_available(&self, now: Instant) -> bool {
        self.continued
            .as_ref()
            .is_some_and(|probe| probe.sample_available(now))
            || self
                .probe
                .as_ref()
                .is_some_and(|probe| probe.sample(now).is_some())
    }

    pub(super) fn read_only_capture_due(&self, now: Instant) -> bool {
        self.next_capture.is_none_or(|next| now >= next) && !self.pending()
    }

    pub(super) fn qualified_prefix_capture_due(&self, qualification: u64, now: Instant) -> bool {
        self.next_capture.is_none_or(|next| now >= next)
            && !self.pending()
            && self
                .continued
                .as_ref()
                .is_none_or(|probe| probe.qualifies(qualification))
    }

    pub(super) fn can_capture_qualified_prefix(
        &self,
        history: History,
        ceiling: u64,
        qualification: u64,
    ) -> bool {
        self.continued.as_ref().is_none_or(|probe| {
            !probe.completed_prefix(qualification)
                || probe.covers_completed_prefix(history, ceiling, qualification)
        })
    }

    pub(super) fn pending(&self) -> bool {
        self.continued.as_ref().is_some_and(ContinuedProbe::pending)
            || self
                .probe
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
        self.capture_source(ValidationSource::Checkpoint(root), history, ceiling, now)
    }

    pub(super) fn capture_source(
        &mut self,
        root: ValidationSource,
        history: History,
        ceiling: u64,
        now: Instant,
    ) -> Result<(), StorageError> {
        if self.next_capture.is_some_and(|next| now < next) {
            return Ok(());
        }
        self.continued = None;
        self.probe = Some(Probe::new_source(root, history, ceiling, now)?);
        self.next_capture = Some(
            now.checked_add(CAPTURE_INTERVAL)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?,
        );
        Ok(())
    }

    pub(super) fn capture_qualified(
        &mut self,
        root: ValidationSource,
        history: History,
        ceiling: u64,
        qualification: u64,
        now: Instant,
    ) -> Result<(), StorageError> {
        self.check_qualification(qualification);
        if self.next_capture.is_some_and(|next| now < next) {
            return Ok(());
        }
        self.probe = None;
        self.continued = Some(ContinuedProbe::capture(
            self.continued.take(),
            root,
            history,
            ceiling,
            qualification,
            now,
        )?);
        self.next_capture = Some(
            now.checked_add(CAPTURE_INTERVAL)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?,
        );
        Ok(())
    }

    pub(super) fn share_qualified_pin(
        &mut self,
        root: ValidationSource,
        history: History,
        qualification: u64,
    ) -> Result<(), StorageError> {
        if let Some(probe) = self.continued.as_mut() {
            match probe.share_pin(root, history, qualification) {
                Ok(true) => (),
                Ok(false) => self.continued = None,
                Err(error) => {
                    self.continued = None;
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    pub(super) fn check_qualification(&mut self, qualification: u64) {
        if self
            .continued
            .as_ref()
            .is_some_and(|probe| !probe.qualifies(qualification))
        {
            self.continued = None;
        }
    }

    pub(super) fn rebase_own_cut(
        &mut self,
        root: ValidationSource,
        proof: &crate::journal::SourceReclamationProof,
        predecessor_qualification: u64,
        qualification: u64,
    ) -> Result<(), StorageError> {
        if let Some(probe) = self.continued.as_mut() {
            match probe.rebase(root, proof, predecessor_qualification, qualification) {
                Ok(true) => (),
                Ok(false) => self.continued = None,
                Err(error) => {
                    self.continued = None;
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    pub(super) fn advance(&mut self, now: Instant) -> Result<(), StorageError> {
        if let Some(probe) = self.continued.as_mut() {
            let sample = match probe.advance(now) {
                Ok(sample) => sample,
                Err(error) => {
                    self.continued = None;
                    return Err(error);
                }
            };
            if let Some(
                [
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
                    reused_rows,
                    checked_rows,
                    subtracted_rows,
                    publication_kind,
                    publication_hash_a,
                    publication_hash_b,
                    publication_hash_c,
                    publication_hash_d,
                ],
            ) = sample
            {
                eprintln!(
                    "riffdb-retention-source-bytes-v2\troot={root}\tincarnation={incarnation}\tresume={first}\tceiling={ceiling}\trows={rows}\tcanonical_receipt_bytes={bytes}\tkey_bytes={key_bytes}\tcapture_age_us={age_us}\tcollection_us={collection_us}\tpages={pages}\treused_rows={reused_rows}\tchecked_rows={checked_rows}\tsubtracted_rows={subtracted_rows}\tpublication_kind={publication_kind}\tpublication_hash_a={publication_hash_a}\tpublication_hash_b={publication_hash_b}\tpublication_hash_c={publication_hash_c}\tpublication_hash_d={publication_hash_d}"
                );
            }
            return Ok(());
        }

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

    pub(super) fn rebase_own_acknowledgement(
        &mut self,
        publication: &AcknowledgementPublication,
    ) -> Result<(), StorageError> {
        if let Some(probe) = self.continued.as_mut()
            && !probe.rebase_own_acknowledgement(publication)?
        {
            self.continued = None;
        }
        Ok(())
    }

    pub(super) fn retire(&mut self) {
        self.probe = None;
        self.continued = None;
    }
}

struct Probe {
    root: Option<ValidationSource>,
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
        Self::new_source(ValidationSource::Checkpoint(root), history, ceiling, now)
    }

    fn new_source(
        root: ValidationSource,
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
        let mut page_bytes = 0_usize;
        for _ in 0..limit {
            if self.next >= self.ceiling {
                break;
            }
            let key = self.next.to_be_bytes();
            let sequence = riffdb_storage_api::ChangelogTransactionSequence::new(self.next)
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
            let receipt = root
                .receipt(sequence, None)?
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
            let encoded = receipt.encode().map_err(value_error)?;
            if encoded.len() > riffdb_storage_api::MAX_CHANGELOG_FRAME_BYTES {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            let next_bytes = page_bytes
                .checked_add(encoded.len())
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            if next_bytes > riffdb_storage_api::MAX_CHANGELOG_FRAME_BYTES {
                break;
            }
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
