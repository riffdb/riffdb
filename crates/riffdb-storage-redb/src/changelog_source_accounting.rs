//! Independent read-only checkpoint coverage and exact current hold evidence.
//! None of these fields can create or renew the mutation owner's license.
use super::*;
use crate::source_validation_pin::ValidationSource;
use crate::source_validation_read::ReceiptRead;
use riffdb_storage_api::ChangelogHistoryPointV3 as Point;
use std::time::{Duration, Instant};

const MAX_CAPTURE_AGE: Duration = Duration::from_secs(30);

#[derive(Default)]
pub(super) struct ReadOnlyAccounting {
    qualification: Option<u64>,
    observed: Option<Observation>,
    coverage: Option<Coverage>,
    independent_sequence: u64,
    holds: Option<accounting_holds::AccountingHolds>,
    captured: Option<(Instant, Observation)>,
    inactive_publication: Option<u64>,
}

/// Scalar evidence about an independently covered finite interval. This is
/// deliberately a different type from the source deletion owner's license.
#[derive(Clone, Copy)]
struct Coverage {
    checkpoint: Observation,
    ceiling: Point,
    resume: Point,
    qualification: u64,
}

impl Coverage {
    fn covers(
        self,
        source: &ValidationSource,
        history: History,
        checkpoint: Observation,
        qualification: u64,
    ) -> Result<bool, StorageError> {
        if qualification == u64::MAX
            || self.qualification != qualification
            || history.lineage() != self.checkpoint.history.lineage()
            || history.anchor() != self.checkpoint.history.anchor()
            || history.minimum_resume() != self.resume
            || self.resume.sequence() >= self.ceiling.sequence()
            || self.ceiling.sequence() > history.tail().sequence()
            || checkpoint.history.lineage() != history.lineage()
            || checkpoint.history.anchor() != history.anchor()
            || checkpoint.history.tail().sequence() < self.checkpoint.history.tail().sequence()
            || checkpoint.history.minimum_resume().sequence() > self.resume.sequence()
            || checkpoint.durable_epoch < self.checkpoint.durable_epoch
        {
            return Ok(false);
        }
        let receipt = source
            .receipt(self.ceiling.sequence(), None)?
            .ok_or_else(corrupt)?;
        Ok(Point::from_receipt(&receipt).map_err(value_error)? == self.ceiling)
    }
}

impl ReadOnlyAccounting {
    pub(super) fn check_qualification(&mut self, qualification: u64) {
        if qualification == u64::MAX || self.qualification != Some(qualification) {
            *self = Self::default();
            self.qualification = (qualification != u64::MAX).then_some(qualification);
        }
    }

    pub(super) fn has_pin(&self) -> bool {
        self.holds
            .as_ref()
            .is_some_and(|walk| walk.source().is_some())
    }

    pub(super) fn ready(
        &self,
        shared: &SharedRedb,
        observer: &eligible_bytes::Observer,
        qualification: u64,
        now: Instant,
    ) -> bool {
        if self.has_pin() {
            return true;
        }
        if !observer.read_only_capture_due(now) {
            return false;
        }
        let publication = shared.durable_root_publication.load(Ordering::Acquire);
        let already_inspected = self.captured.is_some_and(|(_, checkpoint)| {
            checkpoint.publication == publication
                && checkpoint.durable_epoch == shared.durable_commit_epoch()
        });
        publication.is_multiple_of(2)
            && (self.qualification != Some(qualification)
                || self.inactive_publication != Some(publication))
            && (self.qualification != Some(qualification)
                || self.coverage.is_some()
                || (!already_inspected
                    && self
                        .observed
                        .is_none_or(|prior| prior.publication != publication)))
    }

    pub(super) fn capture(
        &mut self,
        source: ValidationSource,
        shared: &SharedRedb,
        qualification: u64,
        now: Instant,
    ) -> Result<(), StorageError> {
        self.check_qualification(qualification);
        if qualification == u64::MAX || self.has_pin() {
            return Err(unavailable());
        }
        let Some(history) = source.history()? else {
            // The root reader admits None only for exact pre-V3 inactivity.
            // Current-registry root loss remains an error in that same reader.
            self.inactive_publication = Some(source.identity());
            return Ok(());
        };
        self.inactive_publication = None;
        let native = ValidationSource::Checkpoint(Arc::clone(source.checkpoint_pin()));
        let checkpoint = Observation {
            publication: native.identity(),
            durable_epoch: shared.durable_commit_epoch(),
            history: native.history()?.ok_or_else(corrupt)?,
        };
        if let Some(coverage) = self.coverage
            && !coverage.covers(&source, history, checkpoint, qualification)?
        {
            self.coverage = None;
        }
        match self.observed {
            None => self.observed = Some(checkpoint),
            Some(previous)
                if previous.history.lineage() != history.lineage()
                    || previous.history.anchor() != history.anchor() =>
            {
                self.observed = Some(checkpoint);
                self.coverage = None;
                self.independent_sequence = 0;
            }
            Some(previous) if self.coverage.is_none() => {
                if previous.publication != checkpoint.publication
                    && previous.durable_epoch < checkpoint.durable_epoch
                    && checkpoint.history.lineage() == history.lineage()
                    && checkpoint.history.anchor() == history.anchor()
                    && checkpoint.history.tail().sequence() >= previous.history.tail().sequence()
                    && previous.history.tail().sequence() > history.minimum_resume().sequence()
                    && let Some(independent) = journaled::independently_checkpointed(&native)?
                    && independent > self.independent_sequence
                {
                    let candidate = Coverage {
                        checkpoint,
                        ceiling: previous.history.tail(),
                        resume: history.minimum_resume(),
                        qualification,
                    };
                    if candidate.covers(&source, history, checkpoint, qualification)? {
                        self.coverage = Some(candidate);
                        self.observed = Some(checkpoint);
                        self.independent_sequence = independent;
                    }
                }
            }
            Some(_) => {}
        }
        let continued = match self.holds.as_mut() {
            Some(walk) => walk.continue_on(source.clone(), qualification)?,
            None => false,
        };
        if !continued {
            self.holds = Some(accounting_holds::AccountingHolds::capture(
                source,
                qualification,
            )?);
        }
        self.captured = Some((now, checkpoint));
        Ok(())
    }

    /// Continue only the existing immutable pin. Freshness starts at the real
    /// capture above, never at the later completion of hold or audit work.
    pub(super) fn advance_captured(
        &mut self,
        shared: &SharedRedb,
        observer: &mut eligible_bytes::Observer,
        qualification: u64,
        now: Instant,
    ) -> Result<(), StorageError> {
        self.check_qualification(qualification);
        let Some(walk) = self.holds.as_mut().filter(|walk| walk.source().is_some()) else {
            return Ok(());
        };
        if !walk.advance()? {
            return Ok(());
        }
        let Some((captured, checkpoint)) = self.captured else {
            return Err(unavailable());
        };
        if now.saturating_duration_since(captured) >= MAX_CAPTURE_AGE {
            walk.release_completed_pin();
            return Ok(());
        }
        let Some(coverage) = self.coverage else {
            walk.release_completed_pin();
            return Ok(());
        };
        let source = walk.source().ok_or_else(unavailable)?;
        let history = source.history()?.ok_or_else(corrupt)?;
        if shared.source_continuation_epoch.load(Ordering::Acquire) != qualification
            || !coverage.covers(source, history, checkpoint, qualification)?
        {
            walk.release_completed_pin();
            return Ok(());
        }
        let mut derived = shared.derived_source_pins.reclamation()?;
        let floor = walk
            .validated(qualification)?
            .floor(coverage.ceiling, derived.oldest(history.lineage()))?;
        drop(derived);
        if shared.source_continuation_epoch.load(Ordering::Acquire) == qualification {
            observer.capture_qualified(
                source.clone(),
                history,
                floor[5],
                qualification,
                captured,
            )?;
        }
        walk.release_completed_pin();
        Ok(())
    }

    pub(super) fn rebase_own_cut(
        &mut self,
        proof: &crate::journal::SourceReclamationProof,
        validated: &crate::changelog_v3_retained_validation::ValidatedRetainedHistory,
        predecessor: u64,
        qualification: u64,
    ) -> Result<(), StorageError> {
        if self.qualification != Some(predecessor)
            || predecessor.checked_add(1) != Some(qualification)
            || qualification == u64::MAX
        {
            self.check_qualification(qualification);
            return Ok(());
        }
        let before = proof.predecessor();
        let after = validated.history();
        if before.lineage() != after.lineage()
            || before.anchor() != after.anchor()
            || after.minimum_resume() != proof.retained().minimum_resume()
            || before.tail().sequence().get().checked_add(1) != Some(after.tail().sequence().get())
        {
            return Err(corrupt());
        }
        if let Some(coverage) = self.coverage.as_mut() {
            if coverage.qualification == predecessor
                && coverage.resume == before.minimum_resume()
                && coverage.checkpoint.history.lineage() == before.lineage()
                && coverage.checkpoint.history.anchor() == before.anchor()
                && after.minimum_resume().sequence() < coverage.ceiling.sequence()
            {
                coverage.resume = after.minimum_resume();
                coverage.qualification = qualification;
            } else {
                self.coverage = None;
            }
        }
        if self.observed.is_some_and(|observed| {
            observed.history.tail().sequence() <= after.minimum_resume().sequence()
        }) {
            self.observed = None;
        }
        if let Some(walk) = self.holds.as_mut()
            && !walk.rebase_own_cut(proof, validated, predecessor, qualification)?
        {
            self.holds = None;
        }
        self.qualification = Some(qualification);
        self.captured = None;
        Ok(())
    }

    pub(super) fn rebase_own_acknowledgement(
        &mut self,
        publication: &AcknowledgementPublication,
    ) -> Result<(), StorageError> {
        let Some((before, after, _, qualification)) = publication.successor() else {
            return Ok(());
        };
        let predecessor = qualification.checked_sub(1).ok_or_else(corrupt)?;
        if self.qualification != Some(predecessor) {
            self.check_qualification(qualification);
            return Ok(());
        }
        if let Some(coverage) = self.coverage.as_mut() {
            if coverage.qualification == predecessor
                && coverage.resume == before.minimum_resume()
                && after.minimum_resume() == before.minimum_resume()
                && coverage.checkpoint.history.lineage() == before.lineage()
                && coverage.checkpoint.history.anchor() == before.anchor()
            {
                coverage.qualification = qualification;
            } else {
                self.coverage = None;
            }
        }
        if let Some(walk) = self.holds.as_mut()
            && !walk.rebase_own_acknowledgement(publication)?
        {
            self.holds = None;
        }
        self.qualification = Some(qualification);
        self.captured = None;
        Ok(())
    }
}

fn unavailable() -> StorageError {
    storage_error(StorageErrorKind::Unavailable)
}
fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}
