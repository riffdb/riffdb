//! Qualified exact interval bytes, without a per-row index or deletion authority.
use super::*;
use crate::journal::SourceReclamationProof;
use crate::source_validation_pin::SourceIdentity;

pub(super) struct ContinuedProbe {
    root: Option<ValidationSource>,
    root_identity: u64,
    publication: SourceIdentity,
    history: History,
    qualification: u64,
    ceiling: u64,
    next: u64,
    covered: riffdb_storage_api::ChangelogHistoryPointV3,
    bytes: u64,
    reused_rows: u64,
    checked_rows: u64,
    subtracted_rows: u64,
    pages: u64,
    captured: Instant,
    completed: Option<Instant>,
    emitted: bool,
}

impl ContinuedProbe {
    pub(super) fn capture(
        previous: Option<Self>,
        root: ValidationSource,
        history: History,
        ceiling: u64,
        qualification: u64,
        now: Instant,
    ) -> Result<Self, StorageError> {
        if qualification == u64::MAX
            || ceiling > history.tail().sequence().get()
            || root.history()? != Some(history)
        {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let first = history.minimum_resume().sequence().get();
        let ceiling = ceiling.max(first);
        let mut result = Self {
            root_identity: root.identity(),
            publication: root.publication(),
            root: Some(root),
            history,
            qualification,
            ceiling,
            next: first,
            covered: history.minimum_resume(),
            bytes: 0,
            reused_rows: 0,
            checked_rows: 0,
            subtracted_rows: 0,
            pages: 0,
            captured: now,
            completed: None,
            emitted: false,
        };
        if let Some(previous) = previous.filter(|prior| {
            prior.completed.is_some()
                && prior.qualification == qualification
                && prior.history.lineage() == history.lineage()
                && prior.history.anchor() == history.anchor()
                && prior.history.minimum_resume() == history.minimum_resume()
                && prior.history.tail().sequence() <= history.tail().sequence()
                && prior.ceiling <= ceiling
        }) {
            result.next = previous.ceiling;
            result.covered = previous.covered;
            result.bytes = previous.bytes;
            result.reused_rows = previous.ceiling.checked_sub(first).ok_or_else(limit)?;
        }
        Ok(result)
    }

    pub(super) fn qualifies(&self, qualification: u64) -> bool {
        qualification != u64::MAX && self.qualification == qualification
    }

    pub(super) fn pending(&self) -> bool {
        self.root.is_some() && self.completed.is_none()
    }

    pub(super) fn advance(&mut self, now: Instant) -> Result<Option<[u64; 18]>, StorageError> {
        if self.emitted {
            return Ok(None);
        }
        if now.saturating_duration_since(self.captured) >= MAX_AGE {
            self.root = None;
            self.completed = None;
            return Ok(None);
        }
        let Some(root) = self.root.as_ref() else {
            return Ok(None);
        };
        let mut page_bytes = 0_usize;
        let mut rows = 0;
        for _ in 0..MAX_ROWS {
            if self.next >= self.ceiling {
                break;
            }
            let sequence = riffdb_storage_api::ChangelogTransactionSequence::new(self.next)
                .ok_or_else(corrupt)?;
            let receipt = root.receipt(sequence, None)?.ok_or_else(corrupt)?;
            let encoded = receipt.encode().map_err(value_error)?;
            let bytes = page_bytes.checked_add(encoded.len()).ok_or_else(limit)?;
            if bytes > riffdb_storage_api::MAX_CHANGELOG_FRAME_BYTES {
                break;
            }
            let covered = History::new(
                self.history.lineage(),
                self.history.anchor(),
                self.covered,
                self.history.minimum_resume(),
            )
            .map_err(value_error)?;
            if self.next == self.history.minimum_resume().sequence().get() {
                covered
                    .validate_terminal_receipt(&receipt)
                    .map_err(value_error)?;
            } else {
                self.covered = covered.advance(&receipt).map_err(value_error)?.tail();
            }
            if receipt.binding().sequence != sequence {
                return Err(corrupt());
            }
            self.bytes = self
                .bytes
                .checked_add(u64::try_from(encoded.len()).map_err(|_| limit())?)
                .ok_or_else(limit)?;
            self.next = self.next.checked_add(1).ok_or_else(limit)?;
            self.checked_rows = self.checked_rows.checked_add(1).ok_or_else(limit)?;
            page_bytes = bytes;
            rows += 1;
        }
        if rows != 0 {
            self.pages = self.pages.checked_add(1).ok_or_else(limit)?;
        }
        if self.next != self.ceiling {
            return Ok(None);
        }
        let observed = Instant::now().max(now);
        // A page that finishes too late is not a completed observation.
        if observed.saturating_duration_since(self.captured) >= MAX_AGE {
            self.root = None;
            self.completed = None;
            return Ok(None);
        }
        self.completed = Some(observed);
        self.root = None;
        self.emitted = true;
        self.sample(observed).map(Some)
    }

    /// Called only after the owner has acknowledged its checked V5 cut and
    /// completely validated its successor. Subtract the canonical preimages
    /// actually included in our scanned prefix; unscanned deletions add no bytes.
    pub(super) fn rebase(
        &mut self,
        root: ValidationSource,
        proof: &SourceReclamationProof,
        predecessor_qualification: u64,
        qualification: u64,
    ) -> Result<bool, StorageError> {
        let predecessor = proof.predecessor();
        let history = root.history()?.ok_or_else(corrupt)?;
        if !self.qualifies(predecessor_qualification)
            || qualification == u64::MAX
            || predecessor.lineage() != self.history.lineage()
            || predecessor.anchor() != self.history.anchor()
            || predecessor.minimum_resume() != self.history.minimum_resume()
            || predecessor.tail().sequence() < self.history.tail().sequence()
            || history.lineage() != predecessor.lineage()
            || history.anchor() != predecessor.anchor()
            || history.minimum_resume() != proof.retained().minimum_resume()
            || predecessor.tail().sequence().get().checked_add(1)
                != Some(history.tail().sequence().get())
            || history.minimum_resume().sequence().get() > self.ceiling
        {
            return Ok(false);
        }
        let first = self.history.minimum_resume().sequence().get();
        for (offset, row) in proof.removed().iter().enumerate() {
            let sequence = first
                .checked_add(u64::try_from(offset).map_err(|_| limit())?)
                .ok_or_else(limit)?;
            if sequence >= self.next {
                break;
            }
            self.bytes = self
                .bytes
                .checked_sub(u64::from(row.canonical_bytes()))
                .ok_or_else(corrupt)?;
            self.subtracted_rows = self.subtracted_rows.checked_add(1).ok_or_else(limit)?;
        }
        self.next = self.next.max(history.minimum_resume().sequence().get());
        if self.next == history.minimum_resume().sequence().get() {
            self.covered = history.minimum_resume();
        }
        self.history = history;
        self.qualification = qualification;
        self.root_identity = root.identity();
        self.publication = root.publication();
        // Completed certificates retain no reader. Candidates share the new
        // immutable successor with source validation, releasing the obsolete pin.
        self.root = self.completed.is_none().then_some(root);
        Ok(true)
    }

    fn sample(&self, now: Instant) -> Result<[u64; 18], StorageError> {
        let completed = self.completed.ok_or_else(corrupt)?;
        let first = self.history.minimum_resume().sequence().get();
        let rows = self.ceiling.checked_sub(first).ok_or_else(corrupt)?;
        if self
            .reused_rows
            .checked_add(self.checked_rows)
            .and_then(|n| n.checked_sub(self.subtracted_rows))
            != Some(rows)
        {
            return Err(corrupt());
        }
        let key_bytes = rows.checked_mul(8).ok_or_else(limit)?;
        self.bytes.checked_add(key_bytes).ok_or_else(limit)?;
        let (kind, hash) = match self.publication {
            SourceIdentity::Checkpoint(_) => (0, [0; 32]),
            SourceIdentity::Composite { frame_hash, .. } => (1, frame_hash),
        };
        let mut words = [0_u64; 4];
        for (word, chunk) in words.iter_mut().zip(hash.chunks_exact(8)) {
            *word = u64::from_be_bytes(chunk.try_into().map_err(|_| corrupt())?);
        }
        Ok([
            self.root_identity,
            self.history.lineage().history_incarnation(),
            first,
            self.ceiling,
            rows,
            self.bytes,
            key_bytes,
            u64::try_from(now.saturating_duration_since(self.captured).as_micros())
                .map_err(|_| limit())?,
            u64::try_from(
                completed
                    .saturating_duration_since(self.captured)
                    .as_micros(),
            )
            .map_err(|_| limit())?,
            self.pages,
            self.reused_rows,
            self.checked_rows,
            self.subtracted_rows,
            kind,
            words[0],
            words[1],
            words[2],
            words[3],
        ])
    }
}
fn limit() -> StorageError {
    storage_error(StorageErrorKind::LimitExceeded)
}
fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

#[cfg(test)]
#[path = "changelog_source_eligible_continuation_tests.rs"]
mod tests;
