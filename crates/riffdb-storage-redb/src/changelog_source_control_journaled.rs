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
        // Finish already captured accounting before offering this proof's pin.
        // The final proof page can belong to an older snapshot than unfinished
        // accounting. Sharing that older pin correctly refuses reuse but would
        // discard the sample until the next capture cadence. Keep the two
        // existing pins and every sharing check; this bounded turn grants no
        // writer or additional snapshot. Captured pages continue off-gate.
        if self.eligible_bytes.pending() || self.accounting.has_pin() {
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
        // The completed source proof may precede an installed checkpoint.
        // Share its qualified immutable pin with any accounting candidate
        // before reading actual checkpoint evidence, keeping two pins total.
        self.eligible_bytes
            .share_qualified_pin(source.clone(), history, epoch)?;
        let checkpoint_root = self.shared.capture_checkpoint_root()?;
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
        let (_, observation) = retention::floor_on_source(
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
            let Some(independent) = independently_checkpointed(&ValidationSource::Checkpoint(
                Arc::clone(&checkpoint_root),
            ))?
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
        drop(checkpoint_root);
        let license = self.online_license.ok_or_else(invariant)?;
        let mut derived = self.shared.derived_source_pins.reclamation()?;
        let (floor, _) = retention::floor_on_source(
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

    // Accounting below an existing finite license does not depend on validating
    // newly appended receipts above it. Capture on an ordinary paced turn, never
    // from the no-new-pin continuation used during proof/writer rest. Native
    // qualification preserves a checked prefix. New eligibility or missing
    // accounting requires bounded canonical validation on the actual current pin.
    pub(super) fn refresh_qualified_source_byte_prefix(
        &mut self,
        epoch: u64,
    ) -> Result<(), StorageError> {
        let now = std::time::Instant::now();
        if !self.source_background_work_ready()
            || !self.eligible_bytes.qualified_prefix_capture_due(epoch, now)
            || !self
                .online_validation
                .as_ref()
                .is_some_and(|walk| walk.qualifies(epoch))
        {
            return Ok(());
        }
        let Some(license) = self.online_license else {
            return Ok(());
        };
        // Completed or absent accounting retains no pin. This capture and the existing
        // mutation-proof candidate therefore remain within the two-pin bound.
        let source = self.shared.capture_source_validation_pin()?;
        let history = source.history()?.ok_or_else(corrupt)?;
        if self
            .shared
            .source_continuation_epoch
            .load(Ordering::Acquire)
            != epoch
        {
            return Ok(());
        }
        let checkpoint = Observation {
            publication: source.checkpoint_pin().identity(),
            durable_epoch: self.shared.durable_commit_epoch(),
            history: crate::changelog_v3_roots::read_checkpoint_roots(&source)?
                .ok_or_else(corrupt)?,
        };
        if !license.covers(&source, history, checkpoint, epoch)? {
            return Ok(());
        }
        let mut derived = self.shared.derived_source_pins.reclamation()?;
        let (_, observation) = retention::floor_on_source(
            &source,
            history,
            license.ceiling(),
            derived.oldest(history.lineage()),
        )?;
        drop(derived);
        if !self
            .eligible_bytes
            .can_capture_qualified_prefix(history, observation[5], epoch)
            || self
                .shared
                .source_continuation_epoch
                .load(Ordering::Acquire)
                != epoch
        {
            return Ok(());
        }
        self.eligible_bytes
            .capture_qualified(source, history, observation[5], epoch, now)?;
        // Unchanged intervals finish with zero new rows. Added eligibility or
        // absent accounting stays unknown until bounded pages complete. The
        // mutation-proof path shares its pin before capturing an independent
        // checkpoint, preserving the two-pin bound for pending observations.
        self.advance_source_byte_observation_off_gate()
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
        if !walk.qualifies(epoch) {
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
        drop(proof);
        self.eligible_bytes
            .share_qualified_pin(source.clone(), history, epoch)?;
        // Classified replacement does not revoke a fixed covered prefix. Read
        // the checkpoint actually bound to this admitted immutable source pin;
        // no new snapshot, checkpoint materialization or independent progress
        // is introduced under the writer. The license still checks its exact
        // covered point, original checkpoint evidence and current qualification.
        let checkpoint = Observation {
            publication: source.checkpoint_pin().identity(),
            durable_epoch: self.shared.durable_commit_epoch(),
            history: crate::changelog_v3_roots::read_checkpoint_roots(&source)?
                .ok_or_else(corrupt)?,
        };
        if !prepared
            .license
            .covers(&source, history, checkpoint, epoch)?
        {
            self.online_license = None;
            return self.journaled_yield(AdmissionYield::ChangedQualification);
        }
        prepared.proof = prepared
            .proof
            .rebind_checked_prefix(history)
            .map_err(journal_storage_error)?;
        let mut derived = self.shared.derived_source_pins.reclamation()?;
        let (floor, observation) = retention::floor_on_source(
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
        self.online_validation
            .as_mut()
            .ok_or_else(invariant)?
            .release_completed_pin();
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
            self.accounting.rebase_own_cut(
                &accounting_proof,
                &walk.proof()?,
                epoch,
                qualification,
            )?;
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
pub(super) fn independently_checkpointed(
    source: &ValidationSource,
) -> Result<Option<u64>, StorageError> {
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

#[cfg(test)]
mod checkpoint_replacement_tests {
    use super::*;

    fn append(ports: &RedbOperationalPorts, ordinal: u64) {
        let write = crate::changelog_v3_write::CapturedImmediateWrite::begin(
            &ports.shared.database,
            crate::RedbCommitProfile::Hardened,
            Source::OutboxTransition,
        )
        .unwrap();
        write
            .open_table(crate::layout::OUTBOX_STATUS)
            .unwrap()
            .insert(
                b"checkpoint-replacement-fixture".as_slice(),
                ordinal.to_be_bytes().as_slice(),
            )
            .unwrap();
        write.finish().unwrap().commit(&ports.shared).unwrap();
    }

    fn observed(ports: &RedbOperationalPorts) -> Observation {
        let root = ports.shared.capture_checkpoint_root().unwrap();
        Observation {
            publication: root.identity(),
            durable_epoch: ports.shared.durable_commit_epoch(),
            history: crate::changelog_v3_roots::read_checkpoint_roots(&root)
                .unwrap()
                .unwrap(),
        }
    }

    // req: STO-030, STO-031, STO-032, REC-001, END-008
    #[test]
    fn classified_checkpoint_replacement_preserves_admission_of_a_finite_checked_prefix() {
        let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
        for ordinal in 1..=300 {
            append(&ports, ordinal);
        }
        let previous = observed(&ports);
        append(&ports, 301);
        let checkpoint = observed(&ports);
        let source = ports.shared.capture_source_validation_pin().unwrap();
        let history = source.history().unwrap().unwrap();
        let epoch = ports
            .shared
            .source_continuation_epoch
            .load(Ordering::Acquire);
        let license =
            checkpoint_license::CheckpointLicense::new(previous, checkpoint, history, epoch)
                .unwrap();
        assert!(license.covers(&source, history, checkpoint, epoch).unwrap());
        let mut walk = RetainedHistoryValidation::new_source(source.clone())
            .unwrap()
            .unwrap();
        walk.bind_qualification(epoch);
        for _ in 0..8 {
            if walk
                .advance(riffdb_storage_api::EvidencePageLimit::new(256).unwrap())
                .unwrap()
            {
                break;
            }
        }
        assert!(walk.is_complete());
        let first = history.minimum_resume().sequence().get();
        let floor_sequence =
            riffdb_storage_api::ChangelogTransactionSequence::new(first + 256).unwrap();
        let floor = riffdb_storage_api::ChangelogHistoryPointV3::from_receipt(
            &source.receipt(floor_sequence, None).unwrap().unwrap(),
        )
        .unwrap();
        let rows = (first..first + 256)
            .map(|sequence| {
                let sequence =
                    riffdb_storage_api::ChangelogTransactionSequence::new(sequence).unwrap();
                SourceReceiptPreimage::capture(
                    &source
                        .receipt(sequence, None)
                        .unwrap()
                        .unwrap()
                        .encode()
                        .unwrap(),
                )
                .unwrap()
            })
            .collect();
        let retained =
            History::new(history.lineage(), history.anchor(), history.tail(), floor).unwrap();
        let proof = SourceReclamationProof::new(history, retained, rows).unwrap();
        walk.release_completed_pin();
        drop(source);
        let mut control = ports.replication_source_control();
        control.online_validation = Some(walk);
        control.online_license = Some(license);
        control.observed = Some(checkpoint);
        // A real checked native append installs a different durable checkpoint
        // between preparation and admission. The fixed licensed prefix is intact.
        append(&ports, 302);
        let installed = observed(&ports);
        assert_ne!(installed.publication, checkpoint.publication);
        assert_eq!(
            ports
                .shared
                .source_continuation_epoch
                .load(Ordering::Acquire),
            epoch
        );
        assert_eq!(
            control
                .submit_journaled_cut(PreparedCut { proof, license })
                .unwrap(),
            256,
            "classified checkpoint replacement must not discard an otherwise qualified finite-prefix cut"
        );
        let current = ports.shared.capture_source_validation_pin().unwrap();
        let after = current.history().unwrap().unwrap();
        assert_eq!(after.minimum_resume(), floor);
        assert_eq!(
            after.tail().sequence().get(),
            installed.history.tail().sequence().get() + 1
        );
        assert_eq!(control.online_license.unwrap().ceiling(), license.ceiling());
        let mut verified = RetainedHistoryValidation::new_source(current)
            .unwrap()
            .unwrap();
        for _ in 0..8 {
            if verified
                .advance(riffdb_storage_api::EvidencePageLimit::new(256).unwrap())
                .unwrap()
            {
                break;
            }
        }
        assert!(
            verified.is_complete(),
            "the actual acknowledged successor retains exact source/audit evidence"
        );
    }
}
