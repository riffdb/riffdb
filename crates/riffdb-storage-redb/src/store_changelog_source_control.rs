//! Crate-private source maintenance at the existing drained writer barrier.
//! No public exports, application mutation, callback or bootstrap-release permit.
#![cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "WP-772 proves the internal substrate; WP-746 owns activation through authorized replication composition"
    )
)]

use super::*;
use crate::changelog_v3_activation::{HISTORY, SOURCE_HOLDS};
use crate::changelog_v3_write::{PreparedHistoryAdvance, value_error};
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, AuthoritativeTransactionBindingV3, AuthoritativeTransactionV3,
    ChangelogAttributionV3 as Source, ChangelogCursorErrorV3 as Refusal,
    ChangelogHistoryStateV3 as History, FollowerRegistrationPhaseV1 as Phase,
    MAX_REPLICATION_SOURCE_HOLDS_V1, ReplicationSourceHoldKindV1 as Kind,
    ReplicationSourceHoldStateV1 as State, ReplicationSourceHoldV1 as Hold,
    proto_codec::{
        decode_replication_source_hold, decode_replication_source_hold_v1,
        encode_replication_source_hold_v1, encode_replication_source_hold_v2,
    },
};

#[path = "changelog_source_control_transaction.rs"]
mod transaction;
use transaction::Barrier;
pub(crate) use transaction::{AcknowledgementPublication, ControlWrite};
#[path = "changelog_source_prefix_refresh.rs"]
mod prefix_refresh;
pub(crate) use prefix_refresh::{PendingPrefixRefresh, PrefixRefreshPublication};
#[path = "replication_administration_write.rs"]
mod administration;
#[path = "changelog_bootstrap_attachment.rs"]
mod bootstrap_attachment;
#[path = "changelog_bootstrap_custody.rs"]
mod bootstrap_custody;
#[path = "changelog_source_checkpoint_license.rs"]
mod checkpoint_license;
#[path = "changelog_source_control_checkpointed_online.rs"]
mod checkpointed_online;
#[path = "changelog_source_eligible_bytes.rs"]
mod eligible_bytes;
#[path = "changelog_follower_acknowledgement.rs"]
mod follower_acknowledgement;
#[path = "changelog_source_control_journaled.rs"]
mod journaled;
#[path = "changelog_source_pacing.rs"]
mod pacing;
#[path = "primary_fence_control.rs"]
mod primary_fence;
#[path = "changelog_registered_ack.rs"]
mod registered_ack;
#[path = "replication_registration_maintenance.rs"]
mod registration_maintenance;
#[path = "changelog_source_control_retention.rs"]
mod retention;

#[derive(Clone, Copy)]
struct Observation {
    publication: u64,
    durable_epoch: u64,
    history: History,
}

/// Internal owner; inaccessible to application/library consumers. WP-746 owns
/// authorization and remote durability evidence before activating any caller.
/// There is no generic hold removal, bootstrap release or raw row editor.
pub(crate) struct ReplicationSourceControl {
    shared: Arc<SharedRedb>,
    observed: Option<Observation>,
    prepared_acknowledgement: Option<(Hold, Vec<u8>)>,
    pub(crate) acknowledgement_active: Option<Hold>,
    bootstrap_reader: Option<std::sync::Weak<()>>,
    // A storage primitive has no background scheduler. The graph and closed
    // acknowledgement custodian bind this shared scheduling policy once.
    source_pacing: Option<pacing::SourcePacing>,
    online_validation: Option<crate::changelog_v3_retained_validation::RetainedHistoryValidation>,
    online_independent_sequence: u64,
    online_license: Option<checkpoint_license::CheckpointLicense>,
    online_validation_phases: [u64; 4],
    online_admission_yields: [u64; 5],
    reclamation_observation: Option<[u64; 8]>,
    reclamation_yields: [u64; 3],
    source_admission_census: [u64; 8],
    eligible_bytes: eligible_bytes::Observer,
}

impl RedbOperationalPorts {
    /// A retained artifact handle is not permission to continue after retirement
    /// or custody loss. Outbound bytes use one current, completely validated pin.
    pub(crate) fn validate_bootstrap_custody(&self, bootstrap: Hold) -> Result<(), StorageError> {
        if self.shared.mutation_gate.source_admission_retired()?
            || self.shared.write_fenced.load(Ordering::Acquire)
            || self.shared.is_follower_mode()
        {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let root = self.shared.capture_checkpoint_root()?;
        let history = crate::changelog_v3_roots::validate_retained_history(&root)?
            .ok_or_else(|| storage_error(StorageErrorKind::IncompatibleFormat))?;
        bootstrap_custody::validate_hold(
            &crate::source_validation_pin::ValidationSource::Checkpoint(root),
            history,
            bootstrap,
        )
    }

    /// Bounded source-only custody probe. Cleanup already owns the artifact's
    /// actual engine exclusion, so its registration cannot race this read.
    pub(crate) fn bootstrap_id_is_held(
        &self,
        id: riffdb_storage_api::ReplicationSourceHoldIdV1,
    ) -> Result<bool, StorageError> {
        self.bootstrap_id_has_custody(id, true)
    }

    /// A pending registration permits its first artifact, but cannot authorize
    /// replacement of any existing bootstrap, attached follower or archive hold.
    pub(crate) fn bootstrap_id_blocks_new_artifact(
        &self,
        id: riffdb_storage_api::ReplicationSourceHoldIdV1,
    ) -> Result<bool, StorageError> {
        self.bootstrap_id_has_custody(id, false)
    }

    fn bootstrap_id_has_custody(
        &self,
        id: riffdb_storage_api::ReplicationSourceHoldIdV1,
        include_pending_registration: bool,
    ) -> Result<bool, StorageError> {
        if self.shared.write_fenced.load(Ordering::Acquire) || self.shared.is_follower_mode() {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let root = self.shared.capture_checkpoint_root()?;
        // Excluding a retired policy can permit artifact cleanup. Prove the
        // original registration and exact release from this same pin first.
        crate::changelog_v3_roots::validate_retained_history(&root)?
            .ok_or_else(|| storage_error(StorageErrorKind::IncompatibleFormat))?;
        let table = root.open_table(SOURCE_HOLDS).map_err(table_error)?;
        for kind in [
            Kind::Bootstrap,
            Kind::FollowerAcknowledgement,
            Kind::ArchiveAcknowledgement,
        ] {
            let key = Hold::storage_key_for(id, kind);
            if let Some(row) = table.get(key.as_slice()).map_err(precommit_storage_error)? {
                let state = *decode_replication_source_hold(row.value())
                    .map_err(crate::error::codec_error)?
                    .value();
                let (hold, retired) = match state {
                    State::Legacy(hold) => (hold, false),
                    State::Registered(policy) => (policy.hold(), policy.phase() == Phase::Retired),
                };
                if hold.id() != id || hold.kind() != kind {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                if retired
                    || (!include_pending_registration
                        && matches!(state, State::Registered(policy) if policy.phase() == Phase::AwaitingBootstrap))
                {
                    continue;
                }
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(crate) fn shared_source_control(
        &self,
    ) -> Result<Arc<Mutex<ReplicationSourceControl>>, StorageError> {
        let mut owner = self
            .shared
            .source_control_owner
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if let Some(existing) = owner.upgrade() {
            return Ok(existing);
        }
        let control = Arc::new(Mutex::new(self.replication_source_control()));
        *owner = Arc::downgrade(&control);
        Ok(control)
    }

    /// A new handle loses only conservative observations, never durable holds.
    pub(crate) fn replication_source_control(&self) -> ReplicationSourceControl {
        ReplicationSourceControl {
            shared: Arc::clone(&self.shared),
            observed: None,
            prepared_acknowledgement: None,
            acknowledgement_active: None,
            bootstrap_reader: None,
            source_pacing: None,
            online_validation: None,
            online_independent_sequence: 0,
            online_license: None,
            online_validation_phases: [0; 4],
            online_admission_yields: [0; 5],
            reclamation_observation: None,
            reclamation_yields: [0; 3],
            source_admission_census: [0; 8],
            eligible_bytes: eligible_bytes::Observer::default(),
        }
    }
}

impl ReplicationSourceControl {
    pub(crate) fn bind_source_scheduler(&mut self) {
        // Rebinding cannot reset charged rest or recover clock overflow.
        self.source_pacing.get_or_insert_with(Default::default);
    }

    #[cfg(feature = "test-fixtures")]
    pub(crate) fn source_scheduler_bound(&self) -> bool {
        self.source_pacing.is_some()
    }

    pub(crate) fn source_work_wait(&self) -> std::time::Duration {
        self.source_pacing
            .as_ref()
            .map_or(std::time::Duration::ZERO, |pacing| {
                pacing.wait(std::time::Instant::now())
            })
    }

    /// A ready candidate owns the single proof slot until admission, yield or
    /// cancellation. Background work cannot overwrite it or renew its rest
    /// between preparation and the existing ordered writer turn.
    pub(crate) fn source_background_work_ready(&self) -> bool {
        self.prepared_acknowledgement.is_none()
            && !self.bootstrap_read_active()
            && self.source_work_wait().is_zero()
    }

    pub(crate) fn charge_source_work(&mut self, started: std::time::Instant) {
        if let Some(pacing) = self.source_pacing.as_mut() {
            let now = std::time::Instant::now();
            pacing.charge(now, now.saturating_duration_since(started));
        }
    }

    pub(crate) fn acknowledgement_gate_census(&self) -> Result<[u64; 2], StorageError> {
        let sample = self.shared.mutation_gate.source_census()?;
        Ok([sample[5], sample[6]])
    }

    pub(crate) fn charge_acknowledgement_work(
        &mut self,
        started: std::time::Instant,
        phase: u8,
        writer_wait_us: u64,
        gate_held_us: u64,
        result: &Result<bool, Refusal>,
    ) {
        let elapsed = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
        self.charge_source_work(started);
        let [initial_rows, continued_rows] = self
            .online_validation
            .as_ref()
            .map_or([0; 2], |walk| walk.step_rows());
        let result_class = match result {
            Ok(true) => 0,
            Ok(false) => 1,
            Err(_) => 2,
        };
        let off_gate_us =
            if elapsed == u64::MAX || writer_wait_us == u64::MAX || gate_held_us == u64::MAX {
                u64::MAX
            } else {
                elapsed
                    .saturating_sub(writer_wait_us)
                    .saturating_sub(gate_held_us)
            };
        eprintln!(
            "riffdb-follower-ack-work-v1\tphase={phase}\tresult={result_class}\toff_gate_us={off_gate_us}\twriter_wait_us={writer_wait_us}\tgate_held_us={gate_held_us}\tinitial_rows={initial_rows}\tcontinued_rows={continued_rows}"
        );
    }

    /// Exact registration retries allocate no receipt; replacement is refused.
    pub(crate) fn register(&mut self, hold: Hold) -> Result<bool, Refusal> {
        self.change_hold(hold, false)
    }

    /// The internal caller owns remote durable-ack evidence. Bootstrap is fixed.
    pub(crate) fn advance_acknowledgement(&mut self, hold: Hold) -> Result<bool, Refusal> {
        if hold.kind() == Kind::Bootstrap {
            return Err(Refusal::InvalidPosition);
        }
        self.change_hold(hold, true)
    }

    fn change_hold(&mut self, hold: Hold, advancing: bool) -> Result<bool, Refusal> {
        let barrier = Barrier::acquire(Arc::clone(&self.shared))?;
        // Reuse the sole exact-position checker on the already drained pin.
        drop(crate::changelog_v3_cursor::open(
            &RedbReadAccess::Durable(Arc::clone(&barrier.root)),
            hold.lineage(),
            hold.fence(),
        )?);
        let table = barrier.root.open_table(SOURCE_HOLDS).map_err(table_error)?;
        let prior = table
            .get(hold.storage_key().as_slice())
            .map_err(precommit_storage_error)?
            .map(|row| {
                decode_replication_source_hold(row.value())
                    .map(|decoded| *decoded.value())
                    .map_err(crate::error::codec_error)
            })
            .transpose()?;
        let (prior, policy) = match prior {
            None => (None, None),
            Some(State::Legacy(hold)) => (Some(hold), None),
            Some(State::Registered(policy)) => {
                // Neither a legacy registration retry nor an equal ack may
                // bypass the registered lifecycle or downgrade its policy.
                if !advancing || policy.phase() != Phase::Attached {
                    return Err(Refusal::InvalidPosition);
                }
                (Some(policy.hold()), Some(policy))
            }
        };
        if hold.kind() == Kind::Bootstrap {
            let key = Hold::storage_key_for(hold.id(), Kind::FollowerAcknowledgement);
            let follower = table
                .get(key.as_slice())
                .map_err(precommit_storage_error)?
                .map(|row| {
                    decode_replication_source_hold(row.value())
                        .map(|decoded| *decoded.value())
                        .map_err(crate::error::codec_error)
                })
                .transpose()?;
            match follower {
                Some(State::Registered(policy)) => {
                    // Check even an equal job retry: retirement must never
                    // renew custody or expose an old artifact under this ID.
                    if !registered_ack::permits_bootstrap(policy, hold) {
                        return Err(Refusal::InvalidPosition);
                    }
                }
                Some(State::Legacy(_)) if prior.is_none() => {
                    return Err(Refusal::InvalidPosition);
                }
                _ => {}
            }
        }
        if prior == Some(hold) {
            return Ok(false);
        }
        match (advancing, prior) {
            (false, None) => {
                if table.len().map_err(precommit_storage_error)? >= MAX_REPLICATION_SOURCE_HOLDS_V1
                {
                    return Err(storage_error(StorageErrorKind::LimitExceeded).into());
                }
            }
            (true, Some(prior)) if prior.fence().sequence() < hold.fence().sequence() => {}
            _ => return Err(Refusal::InvalidPosition),
        }
        drop(table);
        let encoded = if let Some(policy) = policy {
            let successor = registered_ack::advance(policy, hold, barrier.observation().history)
                .map_err(value_error)?;
            encode_replication_source_hold_v2(successor).map_err(crate::error::codec_error)?
        } else {
            encode_replication_source_hold_v1(hold).map_err(crate::error::codec_error)?
        };
        let write = barrier.begin()?;
        // Exhaustion and complete preflight precede the first mutation.
        let (receipt, _) = prepare_control_receipt(
            write.transaction()?,
            write.history(),
            Source::ReplicationSourceHold,
        )?;
        write
            .transaction()?
            .open_table(SOURCE_HOLDS)
            .map_err(table_error)?
            .insert(hold.storage_key().as_slice(), encoded.as_bytes())
            .map_err(precommit_storage_error)?;
        crash_edge("hold-staged");
        receipt.stage(write.transaction()?)?;
        write.validate()?;
        crash_edge("hold-receipted");
        write.commit()?;
        crash_edge("hold-committed");
        Ok(true)
    }

    /// Deletes at most 256 receipts, strictly below the prior observed durable
    /// checkpoint AND all consumer fences, only after a later durable checkpoint.
    /// A first/same-root observation or a no-op creates no control transaction.
    pub(crate) fn reclaim_history(&mut self) -> Result<bool, Refusal> {
        self.reclamation_observation = None;
        self.reclamation_yields = [0; 3];
        // The offline owner deliberately reconstructs the complete current
        // proof. A retained online candidate cannot authorize its publication.
        self.retire_online_validation();
        let barrier = Barrier::acquire(Arc::clone(&self.shared))?;
        self.reclaim_at_barrier(barrier, false)
    }

    /// One bounded validation page, followed only at exact end by a
    /// bounded FIFO, drained-root admission. Returns actual removed receipts
    /// and gate-held time; the caller applies the existing maintenance pacing.
    pub(crate) fn reclaim_history_online(&mut self) -> Result<(u64, u64), StorageError> {
        self.eligible_bytes.check_qualification(
            self.shared
                .source_continuation_epoch
                .load(Ordering::Acquire),
        );
        if let Err(error) = self.eligible_bytes.advance(std::time::Instant::now()) {
            self.retire_online_validation();
            return Err(error);
        }
        self.reclamation_observation = None;
        self.reclamation_yields = [0; 3];
        self.online_validation_phases = [0; 4];
        self.online_admission_yields = [0; 5];
        if let Some(walk) = self.online_validation.as_mut() {
            walk.clear_step_rows();
        }
        self.source_admission_census = [0; 8];
        let before = match self.shared.mutation_gate.source_census() {
            Ok(sample) => sample,
            Err(error) => {
                self.online_validation = None;
                self.online_license = None;
                return Err(error);
            }
        };
        let result = self.reclaim_history_online_step();
        let after = match self.shared.mutation_gate.source_census() {
            Ok(sample) => sample,
            Err(error) => {
                self.online_validation = None;
                self.online_license = None;
                return Err(error);
            }
        };
        self.source_admission_census = std::array::from_fn(|index| {
            if after[index] == u64::MAX {
                u64::MAX
            } else {
                after[index].saturating_sub(before[index])
            }
        });
        if let Some(walk) = self.online_validation.as_ref() {
            let [initial, continued] = walk.step_rows();
            self.online_validation_phases[0] = initial;
            self.online_validation_phases[1] = continued;
        }
        if result.is_err() {
            self.retire_online_validation();
        } else if let Some(walk) = self.online_validation.as_mut() {
            walk.release_completed_pin();
        }
        result.map(|removed| (removed, self.source_admission_census[6]))
    }

    fn reclaim_history_online_step(&mut self) -> Result<u64, StorageError> {
        self.refresh_qualified_source_byte_prefix(
            self.shared
                .source_continuation_epoch
                .load(Ordering::Acquire),
        )?;
        if self.shared.application_commit_profile == RedbCommitProfile::Standard
            && riffdb_storage_api::DURABILITY_JOURNAL_FRAME_VERSION >= 5
        {
            return self.reclaim_history_journaled_online_step();
        }
        use crate::changelog_v3_retained_validation::RetainedHistoryValidation;
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
        // A completed ancestor may lag an already installed checkpoint even
        // when the newer active suffix cannot admit a drained source turn.
        // Continue its qualified proof outside the writer before admission;
        // leave incomplete candidates on their original pin until exact end.
        if let Some(walk) = self.online_validation.as_mut() {
            let publication = self.shared.durable_root_publication.load(Ordering::Acquire);
            if walk.is_complete()
                && publication.is_multiple_of(2)
                && publication != walk.root_identity()
            {
                walk.release_completed_pin();
                let root = self.shared.capture_checkpoint_root()?;
                let epoch = self
                    .shared
                    .source_continuation_epoch
                    .load(Ordering::Acquire);
                if !walk.continue_on(root, epoch)? {
                    self.online_validation = None;
                    self.online_license = None;
                    self.online_validation_phases[2] = 1;
                    return Ok(0);
                }
            }
        }
        if self.online_validation.is_none() {
            let (root, qualification) =
                match Barrier::try_validation_capture_observed(&self.shared)? {
                    transaction::AdmissionAttempt::Admitted(captured) => captured,
                    transaction::AdmissionAttempt::Yielded(reason) => {
                        self.online_validation_phases[3] = 1;
                        self.online_admission_yields[reason.index()] = 1;
                        return Ok(0);
                    }
                };
            self.online_validation = RetainedHistoryValidation::new(root)?;
            if let Some(walk) = self.online_validation.as_mut() {
                walk.bind_qualification(qualification);
            }
            // A capture is one source turn. Return its actual residence to the
            // owner for rest before requesting any subsequent admission.
            return Ok(0);
        }
        let Some(walk) = self.online_validation.as_mut() else {
            return Ok(0);
        };
        if !walk.is_complete()
            && !walk.advance(
                riffdb_storage_api::EvidencePageLimit::new(256)
                    .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
            )?
        {
            return Ok(0);
        }
        crash_edge("continuation-qualified");
        let admission = Barrier::try_from_continuation_observed(Arc::clone(&self.shared), walk);
        let barrier = match admission {
            Ok(transaction::AdmissionAttempt::Admitted(barrier)) => barrier,
            Ok(transaction::AdmissionAttempt::Yielded(reason)) => {
                self.online_validation_phases[3] = 1;
                self.online_admission_yields[reason.index()] = 1;
                return Ok(0);
            }
            Err(error) => {
                self.online_validation = None;
                self.online_license = None;
                return Err(error);
            }
        };
        self.reclaim_checkpointed_online(barrier)
    }

    pub(crate) fn source_admission_census(&self) -> [u64; 8] {
        self.source_admission_census
    }

    /// Advances only the already captured candidate. This cannot capture a
    /// root, request a writer turn, drain a journal or delete a receipt. Exact
    /// end leaves a certificate for the next ordinarily paced admission.
    #[cfg(test)]
    pub(crate) fn advance_validation_off_gate(&mut self) -> Result<[u64; 3], StorageError> {
        self.advance_source_byte_observation_off_gate()?;
        self.advance_source_proof_off_gate()
    }

    /// Already captured exact byte accounting is read-only observation work.
    /// It cannot advance source proof, capture a pin, request a writer turn or
    /// change the source owner's preparation/rest deadline.
    pub(crate) fn advance_source_byte_observation_off_gate(&mut self) -> Result<(), StorageError> {
        self.eligible_bytes.check_qualification(
            self.shared
                .source_continuation_epoch
                .load(Ordering::Acquire),
        );
        if let Err(error) = self.eligible_bytes.advance(std::time::Instant::now()) {
            self.retire_online_validation();
            return Err(error);
        }
        Ok(())
    }

    /// Mutation-proof preparation stays on the graph's shared source pacing.
    /// Observations have their own captured pages and grant no such authority.
    pub(crate) fn advance_source_proof_off_gate(&mut self) -> Result<[u64; 3], StorageError> {
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
            return Ok([0, 0, 1]);
        }
        let Some(walk) = self.online_validation.as_mut() else {
            return Ok([0; 3]);
        };
        walk.clear_step_rows();
        if !walk.is_complete()
            && let Err(error) = walk.advance(
                riffdb_storage_api::EvidencePageLimit::new(256)
                    .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
            )
        {
            self.online_validation = None;
            self.online_license = None;
            return Err(error);
        }
        let [initial, continued] = walk.step_rows();
        walk.release_completed_pin();
        Ok([initial, continued, 0])
    }

    pub(crate) fn retire_online_validation(&mut self) {
        self.eligible_bytes.retire();
        self.online_validation = None;
        self.online_license = None;
    }

    pub(crate) fn validation_phases(&self) -> [u64; 4] {
        self.online_validation_phases
    }

    pub(crate) fn admission_yields(&self) -> [u64; 5] {
        self.online_admission_yields
    }

    pub(crate) fn receipt_validation_pending(&self) -> bool {
        self.online_validation
            .as_ref()
            .is_some_and(|walk| !walk.is_complete())
    }

    pub(crate) fn captured_observations_pending(&self) -> bool {
        self.receipt_validation_pending() || self.eligible_bytes.pending()
    }

    pub(crate) fn captured_read_only_observation_pending(&self) -> bool {
        self.eligible_bytes.pending()
    }

    pub(crate) fn reclamation_observation(&self) -> Option<[u64; 8]> {
        self.reclamation_observation
    }

    pub(crate) fn reclamation_yields(&self) -> [u64; 3] {
        self.reclamation_yields
    }

    fn reclaim_at_barrier(&mut self, barrier: Barrier, online: bool) -> Result<bool, Refusal> {
        let current = barrier.observation();
        let Some(previous) = self.observed else {
            self.reclamation_yields[0] = 1;
            self.observed = Some(current);
            return Ok(false);
        };
        if previous.publication == current.publication
            || previous.durable_epoch >= current.durable_epoch
            || previous.history.lineage() != current.history.lineage()
            || previous.history.tail().sequence() < current.history.minimum_resume().sequence()
        {
            self.reclamation_yields[1] = 1;
            self.observed = Some(current);
            return Ok(false);
        }
        drop(crate::changelog_v3_cursor::open(
            &RedbReadAccess::Durable(Arc::clone(&barrier.root)),
            previous.history.lineage(),
            previous.history.tail(),
        )?);
        let mut derived = self.shared.derived_source_pins.reclamation()?;
        let derived_floor = derived.oldest(current.history.lineage());
        let (floor, observation) = retention::floor(
            &barrier.root,
            current.history,
            previous.history.tail(),
            derived_floor,
        )?;
        self.reclamation_observation = Some(observation);
        if online {
            self.eligible_bytes.capture(
                Arc::clone(&barrier.root),
                current.history,
                observation[5],
                std::time::Instant::now(),
            )?;
        }
        let Some(floor) = floor else {
            self.reclamation_yields[2] = 1;
            self.observed = Some(current);
            return Ok(false);
        };
        let validated = Arc::clone(&barrier.root);
        let write = barrier.begin()?;
        let (receipt, successor) = prepare_control_receipt(
            write.transaction()?,
            write.history(),
            Source::HistoryReclamation,
        )?;
        crash_edge("reclamation-planned");
        retention::stage(
            write.transaction()?,
            &validated,
            current.history,
            successor,
            floor,
            receipt,
        )?;
        write.validate()?;
        crash_edge("reclamation-staged");
        derived.reserve(current.history.lineage(), floor);
        // Observe the post-commit root so this receipt cannot stimulate itself.
        let (observation, root, qualification) = write.commit_reclamation()?;
        self.observed = Some(observation);
        if let Some(walk) = self.online_validation.as_mut()
            && let Err(error) = walk.rebase_own_deletion(root, qualification)
        {
            self.online_validation = None;
            self.online_license = None;
            return Err(Refusal::Storage(error));
        }
        crash_edge("continuation-certificate");
        crash_edge("reclamation-committed");
        Ok(true)
    }
}

fn prepare_control_receipt(
    transaction: &WriteTransaction,
    history: History,
    source: Source,
) -> Result<(PreparedHistoryAdvance, History), StorageError> {
    if !matches!(
        source,
        Source::ReplicationSourceHold | Source::HistoryReclamation
    ) {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    let (sequence, _) = history
        .expected_allocator()
        .allocate_one()
        .map_err(value_error)?;
    let receipt = AuthoritativeTransactionV3::new_for_catalog(
        AuthoritativeTransactionBindingV3 {
            database_id: history.lineage().database_id(),
            history_incarnation: history.lineage().history_incarnation(),
            predecessor: Some(history.tail().sequence()),
            sequence,
            predecessor_frontier: history.tail().frontier(),
            covered_frontier: history.tail().frontier(),
            prior_history_hash: history.tail().history_hash(),
        },
        source,
        Vec::new(),
        history.lineage().catalog_digest(),
    )
    .map_err(value_error)?;
    Ok((
        PreparedHistoryAdvance::prepare(transaction, &receipt)?,
        history.advance(&receipt).map_err(value_error)?,
    ))
}

pub(super) fn crash_edge(_edge: &str) {
    #[cfg(test)]
    if std::env::var("RIFFDB_V3_SOURCE_CONTROL_EDGE")
        .ok()
        .as_deref()
        == Some(_edge)
    {
        std::process::exit(93);
    }
}
