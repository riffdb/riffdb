//! Read-only custody uses the single native source certificate, never a writer.
use super::*;
use crate::changelog_v3_retained_validation::RetainedHistoryValidation;
use crate::source_validation_pin::ValidationSource;
use std::sync::Weak;

pub(super) fn validate_hold(
    source: &ValidationSource,
    history: History,
    bootstrap: Hold,
) -> Result<(), StorageError> {
    if bootstrap.kind() != Kind::Bootstrap || bootstrap.lineage() != history.lineage() {
        return Err(unavailable());
    }
    let table = source.open_table(SOURCE_HOLDS).map_err(table_error)?;
    let row = table
        .get(bootstrap.storage_key().as_slice())
        .map_err(precommit_storage_error)?
        .ok_or_else(unavailable)?;
    let held = *decode_replication_source_hold_v1(row.value())
        .map_err(crate::error::codec_error)?
        .value();
    if held != bootstrap {
        return Err(unavailable());
    }
    let key = Hold::storage_key_for(bootstrap.id(), Kind::FollowerAcknowledgement);
    if let Some(row) = table.get(key.as_slice()).map_err(precommit_storage_error)? {
        let state = *decode_replication_source_hold(row.value())
            .map_err(crate::error::codec_error)?
            .value();
        if let State::Registered(policy) = state
            && !registered_ack::permits_bootstrap(policy, bootstrap)
        {
            return Err(unavailable());
        }
    }
    Ok(())
}

impl ReplicationSourceControl {
    pub(crate) fn observe_bootstrap_custody_work(
        &self,
        owner: &Arc<()>,
        started: std::time::Instant,
        result: &Result<bool, StorageError>,
    ) {
        let ours = self
            .bootstrap_reader
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some_and(|current| Arc::ptr_eq(&current, owner));
        // Waiting for another owner performs no source-proof work. Do not
        // emit a per-retry diagnostic or attribute its rows to this reader.
        if !ours {
            return;
        }
        let [initial_rows, continued_rows] = self
            .online_validation
            .as_ref()
            .map_or([0; 2], |walk| walk.step_rows());
        let elapsed_us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
        let result_class = match result {
            Ok(true) => 0,
            Ok(false) => 1,
            Err(_) => 2,
        };
        eprintln!(
            "riffdb-bootstrap-custody-work-v1\tresult={result_class}\telapsed_us={elapsed_us}\tinitial_rows={initial_rows}\tcontinued_rows={continued_rows}"
        );
    }

    pub(crate) fn bootstrap_read_active(&self) -> bool {
        self.bootstrap_reader
            .as_ref()
            .is_some_and(|owner| owner.strong_count() != 0)
    }

    /// Only this request may advance the candidate it claimed. Another ready
    /// acknowledgement is neither displaced nor cancelled. Tokens identify
    /// nondurable ownership only; they are never source-validation evidence.
    pub(crate) fn prepare_bootstrap_custody(
        &mut self,
        owner: &Arc<()>,
        bootstrap: Hold,
    ) -> Result<bool, StorageError> {
        self.verify_bootstrap_read_admission()?;
        let ours = self
            .bootstrap_reader
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some_and(|current| Arc::ptr_eq(&current, owner));
        if !ours {
            if self.bootstrap_read_active()
                || self.acknowledgement_active.is_some()
                || self.prepared_acknowledgement.is_some()
            {
                return Ok(false);
            }
            self.bootstrap_reader = Some(Arc::downgrade(owner));
        }
        if let Some(walk) = self.online_validation.as_mut() {
            walk.clear_step_rows();
        }
        let epoch = self
            .shared
            .source_continuation_epoch
            .load(Ordering::Acquire);
        if epoch == u64::MAX {
            self.retire_online_validation();
            return Err(unavailable());
        }
        if self
            .online_validation
            .as_ref()
            .is_some_and(|walk| !walk.qualifies(epoch))
        {
            self.retire_online_validation();
        }
        if self.online_validation.is_none() {
            let source = self.shared.capture_source_validation_pin()?;
            if self
                .shared
                .source_continuation_epoch
                .load(Ordering::Acquire)
                != epoch
            {
                return Ok(false);
            }
            let mut walk =
                RetainedHistoryValidation::new_source(source)?.ok_or_else(unavailable)?;
            walk.bind_qualification(epoch);
            self.online_validation = Some(walk);
            return Ok(false);
        }
        let walk = self.online_validation.as_mut().ok_or_else(unavailable)?;
        walk.clear_step_rows();
        if walk.is_complete() {
            walk.release_completed_pin();
            let source = self.shared.capture_source_validation_pin()?;
            if self
                .shared
                .source_continuation_epoch
                .load(Ordering::Acquire)
                != epoch
                || !walk.continue_source(source, epoch)?
            {
                self.retire_online_validation();
                return Ok(false);
            }
        }
        if !walk.is_complete()
            && !walk
                .advance(riffdb_storage_api::EvidencePageLimit::new(256).ok_or_else(unavailable)?)?
        {
            return Ok(false);
        }
        let proof = walk.proof()?;
        validate_hold(proof.source(), proof.history(), bootstrap)?;
        drop(proof);
        walk.release_completed_pin();
        if self
            .shared
            .source_continuation_epoch
            .load(Ordering::Acquire)
            != epoch
        {
            self.retire_online_validation();
            return Ok(false);
        }
        self.verify_bootstrap_read_admission()?;
        Ok(true)
    }

    pub(crate) fn verify_bootstrap_read_admission(&self) -> Result<(), StorageError> {
        if self.shared.mutation_gate.source_admission_retired()?
            || self.shared.write_fenced.load(Ordering::Acquire)
            || self.shared.is_follower_mode()
        {
            return Err(unavailable());
        }
        Ok(())
    }

    pub(crate) fn cancel_bootstrap_custody(&mut self, owner: &Arc<()>) {
        if !self
            .bootstrap_reader
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some_and(|current| Arc::ptr_eq(&current, owner))
        {
            return;
        }
        self.bootstrap_reader = None;
        if self
            .online_validation
            .as_ref()
            .is_some_and(|walk| !walk.is_complete())
        {
            self.retire_online_validation();
        } else if let Some(walk) = self.online_validation.as_mut() {
            walk.release_completed_pin();
        }
    }
}

fn unavailable() -> StorageError {
    storage_error(StorageErrorKind::Unavailable)
}

#[cfg(test)]
#[path = "changelog_bootstrap_custody_tests.rs"]
mod tests;
