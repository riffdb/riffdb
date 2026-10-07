//! Online source cuts over acknowledged composite state (ADR-0278).

use super::*;
use crate::{
    changelog_v3_retained_validation::RetainedHistoryValidation,
    journal::{SourceReceiptPreimage, SourceReclamationProof},
    source_validation_pin::ValidationSource,
    source_validation_read::ReceiptRead,
};
use transaction::{AdmissionAttempt, AdmissionYield};

struct PreparedCut {
    checkpoint: Observation,
    proof: SourceReclamationProof,
    license: checkpoint_license::CheckpointLicense,
}

impl ReplicationSourceControl {
    pub(super) fn reclaim_history_journaled_online_step(&mut self) -> Result<u64, StorageError> {
        let epoch = self
            .shared
            .source_continuation_epoch
            .load(Ordering::Acquire);
        if self
            .online_validation
            .as_ref()
            .is_some_and(|walk| !walk.qualifies(epoch))
        {
            self.online_validation = None;
            self.online_license = None;
            self.online_validation_phases[2] = 1;
        }
        if self.online_validation.is_none() {
            let Some(lease) = self.shared.mutation_gate.acquire_source()? else {
                return self.journaled_yield(AdmissionYield::BusyWriter);
            };
            let source = match capture(&self.shared)? {
                AdmissionAttempt::Admitted(source) => source,
                AdmissionAttempt::Yielded(reason) => return self.journaled_yield(reason),
            };
            self.online_validation = RetainedHistoryValidation::new_source(source)?;
            if let Some(walk) = self.online_validation.as_mut() {
                walk.bind_qualification(
                    self.shared
                        .source_continuation_epoch
                        .load(Ordering::Acquire),
                );
            }
            drop(lease);
            return Ok(0);
        }
        // Continue a completed certificate on the current acknowledged pin
        // outside the writer. Incomplete candidates retain their original pin.
        let walk = self.online_validation.as_mut().ok_or_else(invariant)?;
        if walk.is_complete() {
            walk.release_completed_pin();
            let source = self.shared.capture_source_validation_pin()?;
            if !walk.continue_source(source, epoch)? {
                self.online_validation = None;
                self.online_license = None;
                self.online_validation_phases[2] = 1;
                return Ok(0);
            }
        }
        if !walk.is_complete()
            && !walk
                .advance(riffdb_storage_api::EvidencePageLimit::new(256).ok_or_else(invariant)?)?
        {
            return Ok(0);
        }
        crash_edge("continuation-qualified");
        let proof = walk.proof()?;
        let source = proof.source().clone();
        let history = proof.history();
        drop(proof);
        // A candidate may finish on an older, qualified pin while ordinary
        // writers install later checkpoints. License preparation with the
        // actual installed checkpoint, then close only the bounded current
        // composite suffix at admission. The candidate never substitutes its
        // own acknowledgement or an obsolete pin for checkpoint progress.
        // Share the validated source's installed checkpoint. A completed older
        // candidate continues on the next paced turn instead of opening a third
        // validation snapshot alongside byte accounting.
        if !self
            .shared
            .checkpoint_publication_matches(source.checkpoint_pin().identity())
        {
            return self.journaled_yield(AdmissionYield::CheckpointPublication);
        }
        let checkpoint_root = Arc::clone(source.checkpoint_pin());
        let checkpoint = Observation {
            publication: checkpoint_root.identity(),
            durable_epoch: self.shared.durable_commit_epoch(),
            history: crate::changelog_v3_roots::read_checkpoint_roots(&checkpoint_root)?
                .ok_or_else(corrupt)?,
        };
        let Some(previous) = self.observed else {
            self.observed = Some(checkpoint);
            self.reclamation_yields[0] = 1;
            return Ok(0);
        };
        if previous.history.lineage() != checkpoint.history.lineage()
            || previous.history.anchor() != checkpoint.history.anchor()
        {
            self.observed = Some(checkpoint);
            self.online_license = None;
            self.online_independent_sequence = 0;
            self.reclamation_yields[0] = 1;
            return Ok(0);
        }
        let accounting_ceiling = self
            .online_license
            .map_or(previous.history.tail(), |license| license.ceiling());
        let mut derived = self.shared.derived_source_pins.reclamation()?;
        let (_, observation) = retention::floor(
            &source,
            history,
            accounting_ceiling,
            derived.oldest(history.lineage()),
        )?;
        drop(derived);
        self.eligible_bytes.capture_qualified(
            source.clone(),
            history,
            observation[5],
            epoch,
            std::time::Instant::now(),
        )?;
        if let Some(license) = self.online_license
            && !license.covers(&source, history, checkpoint, epoch)?
        {
            self.online_license = None;
        }
        if self.online_license.is_none() {
            let Some(independent) =
                independently_checkpointed(&ValidationSource::Checkpoint(checkpoint_root))?
            else {
                self.reclamation_yields[1] = 1;
                return Ok(0);
            };
            if independent <= self.online_independent_sequence {
                self.reclamation_yields[1] = 1;
                return Ok(0);
            }
            let Some(license) =
                checkpoint_license::CheckpointLicense::new(previous, checkpoint, history, epoch)
            else {
                self.reclamation_yields[1] = 1;
                return Ok(0);
            };
            if !license.covers(&source, history, checkpoint, epoch)? {
                self.reclamation_yields[1] = 1;
                return Ok(0);
            }
            // Issue once. Our own cuts and their materialization cannot renew
            // this independently evidenced, immutable ceiling.
            self.online_license = Some(license);
            self.observed = Some(checkpoint);
            self.online_independent_sequence = independent;
        }
        let license = self.online_license.ok_or_else(invariant)?;
        let mut derived = self.shared.derived_source_pins.reclamation()?;
        let (floor, _) = retention::floor(
            &source,
            history,
            license.ceiling(),
            derived.oldest(history.lineage()),
        )?;
        drop(derived);
        let Some(floor) = floor else {
            self.reclamation_yields[2] = 1;
            return Ok(0);
        };
        let mut rows = Vec::with_capacity(256);
        let mut bytes = 0_usize;
        let mut retained_floor = floor;
        for sequence in history.minimum_resume().sequence().get()..floor.sequence().get() {
            let sequence = riffdb_storage_api::ChangelogTransactionSequence::new(sequence)
                .ok_or_else(corrupt)?;
            let receipt = source.receipt(sequence, None)?.ok_or_else(corrupt)?;
            let canonical = receipt.encode().map_err(value_error)?;
            let next = bytes.checked_add(canonical.len()).ok_or_else(corrupt)?;
            if next > riffdb_storage_api::MAX_CHANGELOG_FRAME_BYTES {
                retained_floor =
                    riffdb_storage_api::ChangelogHistoryPointV3::from_receipt(&receipt)
                        .map_err(value_error)?;
                break;
            }
            rows.push(SourceReceiptPreimage::capture(&canonical).map_err(journal_storage_error)?);
            bytes = next;
        }
        if rows.is_empty() {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let retained = History::new(
            history.lineage(),
            history.anchor(),
            history.tail(),
            retained_floor,
        )
        .map_err(value_error)?;
        let prepared = PreparedCut {
            checkpoint,
            license,
            proof: SourceReclamationProof::new(history, retained, rows)
                .map_err(journal_storage_error)?,
        };
        drop(source);
        self.online_validation
            .as_mut()
            .ok_or_else(invariant)?
            .release_completed_pin();
        self.submit_journaled_cut(prepared)
    }

    fn submit_journaled_cut(&mut self, mut prepared: PreparedCut) -> Result<u64, StorageError> {
        let Some(lease) = self.shared.mutation_gate.acquire_source()? else {
            return self.journaled_yield(AdmissionYield::BusyWriter);
        };
        let source = match capture(&self.shared)? {
            AdmissionAttempt::Admitted(source) => source,
            AdmissionAttempt::Yielded(reason) => return self.journaled_yield(reason),
        };
        // A changed candidate is discarded. Preparation already ran off-gate;
        // no current tail, history or checkpoint is materialized during admission.
        let epoch = self
            .shared
            .source_continuation_epoch
            .load(Ordering::Acquire);
        let walk = self.online_validation.as_mut().ok_or_else(invariant)?;
        if !walk.qualifies(epoch)
            || source.checkpoint_pin().identity() != prepared.checkpoint.publication
        {
            return self.journaled_yield(AdmissionYield::ChangedQualification);
        }
        if !walk.continue_source(source.clone(), epoch)? {
            return self.journaled_yield(AdmissionYield::ChangedQualification);
        }
        if !walk.close_suffix_page()? {
            return self.journaled_yield(AdmissionYield::CatchupPageBound);
        }
        let proof = walk.proof()?;
        let history = proof.history();
        if !prepared
            .license
            .covers(&source, history, prepared.checkpoint, epoch)?
        {
            self.online_license = None;
            return self.journaled_yield(AdmissionYield::ChangedQualification);
        }
        prepared.proof = prepared
            .proof
            .rebind_checked_prefix(history)
            .map_err(journal_storage_error)?;
        let mut derived = self.shared.derived_source_pins.reclamation()?;
        let (floor, observation) = retention::floor(
            &source,
            history,
            prepared.license.ceiling(),
            derived.oldest(history.lineage()),
        )?;
        self.reclamation_observation = Some(observation);
        self.eligible_bytes.capture_qualified(
            source.clone(),
            history,
            observation[5],
            epoch,
            std::time::Instant::now(),
        )?;
        if floor.is_none_or(|floor| {
            floor.sequence() < prepared.proof.retained().minimum_resume().sequence()
        }) {
            self.reclamation_yields[2] = 1;
            return Ok(0);
        }
        let view = self.shared.source_composite_for_write(source)?;
        let frame =
            crate::journal::JournalFrame::source_reclamation(prepared.proof.clone(), [0; 32])
                .map_err(journal_storage_error)?
                .encode()
                .map_err(journal_storage_error)?;
        let physical =
            crate::journal::extent_frame_bytes(frame.as_bytes().len()).ok_or_else(invariant)?;
        if !self
            .shared
            .has_journal_frame_headroom(1, frame.as_bytes().len(), physical)?
        {
            drop(derived);
            return self.journaled_yield(AdmissionYield::PendingJournal);
        }
        crash_edge("reclamation-planned");
        let removed = u64::try_from(prepared.proof.removed().len()).map_err(|_| invariant())?;
        let floor = prepared.proof.retained().minimum_resume();
        let write = self.shared.begin_source_journal_write(lease, view)?;
        derived.reserve(history.lineage(), floor);
        let accounting_proof = prepared.proof.clone();
        let fence = write.submit_source_reclamation(prepared.proof)?;
        let (successor, qualification) = fence.source_successor().ok_or_else(invariant)?;
        drop(derived);
        riffdb_storage_api::DeferredServiceAuditFence::wait(Box::new(fence))?;
        if qualification
            == self
                .shared
                .source_continuation_epoch
                .load(Ordering::Acquire)
        {
            let walk = self.online_validation.as_mut().ok_or_else(invariant)?;
            let successor = ValidationSource::Composite(successor);
            walk.rebase_own_source_deletion(successor.clone(), qualification)?;
            self.eligible_bytes.rebase_own_cut(
                successor,
                &accounting_proof,
                epoch,
                qualification,
            )?;
        } else {
            self.online_validation = None;
            self.online_license = None;
            self.online_validation_phases[2] = 1;
        }
        if let Some(license) = self.online_license.as_mut()
            && !license.consume(floor, qualification)
        {
            self.online_license = None;
        }
        crash_edge("continuation-certificate");
        crash_edge("reclamation-committed");
        Ok(removed)
    }

    fn journaled_yield(&mut self, reason: AdmissionYield) -> Result<u64, StorageError> {
        self.online_validation_phases[3] = 1;
        self.online_admission_yields[reason.index()] = 1;
        Ok(0)
    }
}

fn capture(shared: &Arc<SharedRedb>) -> Result<AdmissionAttempt<ValidationSource>, StorageError> {
    if shared.write_fenced.load(Ordering::Acquire) || shared.is_follower_mode() {
        return Err(storage_error(StorageErrorKind::Unavailable));
    }
    if shared.journal_seal_publication_pending()? {
        return Ok(AdmissionAttempt::Yielded(
            AdmissionYield::CheckpointPublication,
        ));
    }
    let runtime = shared.journal_runtime.lock().map_err(|_| invariant())?;
    if runtime.as_ref().is_some_and(|runtime| {
        runtime.unpublished_transitions != 0 || runtime.unpublished_bytes != 0
    }) {
        return Ok(AdmissionAttempt::Yielded(AdmissionYield::PendingJournal));
    }
    drop(runtime);
    let source = shared.capture_source_validation_pin()?;
    let meta = source.open_table(META).map_err(table_error)?;
    if let Some(row) = meta
        .get(META_CLEAN_CLOSE_LIFECYCLE)
        .map_err(precommit_storage_error)?
    {
        let lifecycle =
            crate::clean_close::CleanCloseLifecycle::decode(row.value()).map_err(|_| corrupt())?;
        if matches!(
            lifecycle.state(),
            crate::clean_close::CleanCloseState::Clean(_)
        ) {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
    }
    drop(meta);
    Ok(AdmissionAttempt::Admitted(source))
}

/// A bounded actual checkpoint read, never the composite's newer tail. A long
/// control-only suffix yields; it cannot invent independent progress.
fn independently_checkpointed(source: &ValidationSource) -> Result<Option<u64>, StorageError> {
    let root = ValidationSource::Checkpoint(Arc::clone(source.checkpoint_pin()));
    let history = root.history()?.ok_or_else(corrupt)?;
    let mut sequence = history.tail().sequence();
    let mut bytes = 0_usize;
    for _ in 0..256 {
        let receipt = root.receipt(sequence, None)?.ok_or_else(corrupt)?;
        bytes = bytes
            .checked_add(receipt.encode().map_err(value_error)?.len())
            .ok_or_else(corrupt)?;
        if bytes > riffdb_storage_api::MAX_CHANGELOG_FRAME_BYTES {
            return Ok(None);
        }
        if !matches!(
            receipt.attribution(),
            Source::HistoryReclamation | Source::ValidatedPrefixCheckpoint
        ) {
            return Ok(Some(sequence.get()));
        }
        let Some(previous) = sequence
            .get()
            .checked_sub(1)
            .and_then(riffdb_storage_api::ChangelogTransactionSequence::new)
            .filter(|sequence| *sequence >= history.minimum_resume().sequence())
        else {
            return Ok(None);
        };
        sequence = previous;
    }
    Ok(None)
}

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}
fn invariant() -> StorageError {
    storage_error(StorageErrorKind::InvariantViolation)
}
