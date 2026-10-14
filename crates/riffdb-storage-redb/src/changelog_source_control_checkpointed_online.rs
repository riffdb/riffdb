//! Finite licensing for the existing immediate checkpointed online writer.
//! Offline reclamation retains its independent full-validation barrier.
use super::*;
use crate::journal::{SourceReceiptPreimage, SourceReclamationProof};
use crate::source_validation_pin::ValidationSource;
use crate::source_validation_read::ReceiptRead;

impl ReplicationSourceControl {
    pub(super) fn reclaim_checkpointed_online(
        &mut self,
        barrier: Barrier,
    ) -> Result<u64, StorageError> {
        let current = barrier.observation();
        let source = ValidationSource::Checkpoint(Arc::clone(&barrier.root));
        let epoch = self
            .shared
            .source_continuation_epoch
            .load(Ordering::Acquire);
        self.eligible_bytes
            .share_qualified_pin(source.clone(), current.history, epoch)?;
        let Some(previous) = self.observed else {
            self.observed = Some(current);
            self.reclamation_yields[0] = 1;
            return Ok(0);
        };
        if previous.history.lineage() != current.history.lineage()
            || previous.history.anchor() != current.history.anchor()
        {
            self.observed = Some(current);
            self.online_license = None;
            self.online_independent_sequence = 0;
            self.reclamation_yields[0] = 1;
            return Ok(0);
        }
        if let Some(license) = self.online_license
            && !license.covers(&source, current.history, current, epoch)?
        {
            self.online_license = None;
        }
        if self.online_license.is_none() {
            let independent = self
                .online_validation
                .as_ref()
                .ok_or_else(invariant)?
                .independent_sequence();
            if independent > self.online_independent_sequence
                && let Some(license) = checkpoint_license::CheckpointLicense::new(
                    previous,
                    current,
                    current.history,
                    epoch,
                )
                && license.covers(&source, current.history, current, epoch)?
            {
                self.online_license = Some(license);
                self.online_independent_sequence = independent;
                self.observed = Some(current);
            }
        }
        let ceiling = self
            .online_license
            .map_or(previous.history.tail(), |license| license.ceiling());
        let mut derived = self.shared.derived_source_pins.reclamation()?;
        let (floor, observation) = retention::floor(
            &barrier.root,
            current.history,
            ceiling,
            derived.oldest(current.history.lineage()),
        )?;
        self.reclamation_observation = Some(observation);
        self.eligible_bytes.capture_qualified(
            source.clone(),
            current.history,
            observation[5],
            epoch,
            std::time::Instant::now(),
        )?;
        if self.online_license.is_none() {
            self.reclamation_yields[1] = 1;
            return Ok(0);
        }
        let Some(mut floor) = floor else {
            self.reclamation_yields[2] = 1;
            return Ok(0);
        };
        let mut rows = Vec::with_capacity(256);
        let mut bytes = 0_usize;
        for sequence in current.history.minimum_resume().sequence().get()..floor.sequence().get() {
            let sequence = riffdb_storage_api::ChangelogTransactionSequence::new(sequence)
                .ok_or_else(invariant)?;
            let receipt = source
                .receipt(sequence, None)?
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
            let canonical = receipt.encode().map_err(value_error)?;
            let next = bytes.checked_add(canonical.len()).ok_or_else(invariant)?;
            if next > riffdb_storage_api::MAX_CHANGELOG_FRAME_BYTES {
                floor = riffdb_storage_api::ChangelogHistoryPointV3::from_receipt(&receipt)
                    .map_err(value_error)?;
                break;
            }
            rows.push(SourceReceiptPreimage::capture(&canonical).map_err(journal_storage_error)?);
            bytes = next;
        }
        if rows.is_empty() {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let proof = SourceReclamationProof::new(
            current.history,
            History::new(
                current.history.lineage(),
                current.history.anchor(),
                current.history.tail(),
                floor,
            )
            .map_err(value_error)?,
            rows,
        )
        .map_err(journal_storage_error)?;
        let removed = u64::try_from(proof.removed().len()).map_err(|_| invariant())?;
        let write = barrier.begin()?;
        let (receipt, successor) = prepare_control_receipt(
            write.transaction()?,
            write.history(),
            Source::HistoryReclamation,
        )?;
        crash_edge("reclamation-planned");
        retention::stage(
            write.transaction()?,
            &source,
            current.history,
            successor,
            floor,
            receipt,
        )?;
        write.validate()?;
        crash_edge("reclamation-staged");
        derived.reserve(current.history.lineage(), floor);
        drop(source);
        // Release the validation pin before the existing atomic commit captures
        // its successor. Byte accounting is the only other validation pin.
        self.online_validation
            .as_mut()
            .ok_or_else(invariant)?
            .release_completed_pin();
        let (_, root, qualification) = write.commit_reclamation()?;
        let walk = self.online_validation.as_mut().ok_or_else(invariant)?;
        walk.rebase_own_deletion(Arc::clone(&root), qualification)?;
        self.accounting
            .rebase_own_cut(&proof, &walk.proof()?, epoch, qualification)?;
        self.eligible_bytes.rebase_own_cut(
            ValidationSource::Checkpoint(root),
            &proof,
            epoch,
            qualification,
        )?;
        if let Some(license) = self.online_license.as_mut()
            && !license.consume(floor, qualification)
        {
            self.online_license = None;
        }
        crash_edge("continuation-certificate");
        crash_edge("reclamation-committed");
        Ok(removed)
    }
}
fn invariant() -> StorageError {
    storage_error(StorageErrorKind::InvariantViolation)
}
