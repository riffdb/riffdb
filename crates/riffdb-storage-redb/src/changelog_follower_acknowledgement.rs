//! Closed preparation and admission for an existing follower acknowledgement.
//! Preparation pages never occupy a coordinator turn or retain a writer lease.
use super::*;
use crate::changelog_v3_retained_validation::RetainedHistoryValidation;
use transaction::{AdmissionAttempt, AdmissionYield};

// Policies consist entirely of existing fixed-size fields. Refuse before
// copying an unknown envelope; one candidate stays below the shared 64-KiB cap.
const MAX_ACKNOWLEDGEMENT_BEFORE_BYTES: usize = 4096;

impl ReplicationSourceControl {
    pub(crate) fn verify_follower_acknowledgement_admission(&self) -> Result<(), Refusal> {
        if self.shared.mutation_gate.source_admission_retired()? {
            return Err(storage_error(StorageErrorKind::Unavailable).into());
        }
        Ok(())
    }

    pub(crate) fn cancel_follower_acknowledgement_preparation(&mut self) {
        self.prepared_acknowledgement = None;
        if self.bootstrap_read_active() {
            return;
        }
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

    pub(crate) fn prepare_follower_acknowledgement(&mut self, hold: Hold) -> Result<bool, Refusal> {
        self.verify_follower_acknowledgement_admission()?;
        if self.bootstrap_read_active() {
            return Ok(false);
        }
        if hold.kind() != Kind::FollowerAcknowledgement {
            return Err(Refusal::InvalidPosition);
        }
        self.prepared_acknowledgement = None;
        if self.shared.write_fenced.load(Ordering::Acquire) || self.shared.is_follower_mode() {
            return Err(storage_error(StorageErrorKind::Unavailable).into());
        }
        let epoch = self
            .shared
            .source_continuation_epoch
            .load(Ordering::Acquire);
        if epoch == u64::MAX {
            self.retire_online_validation();
            return Err(storage_error(StorageErrorKind::Unavailable).into());
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
            self.online_validation = RetainedHistoryValidation::new_source(source)?;
            self.online_validation
                .as_mut()
                .ok_or_else(invariant)?
                .bind_qualification(epoch);
            // Capturing and scanning are separate bounded steps. Capture does
            // not drain a suffix or perform retained-history validation.
            return Ok(false);
        }
        let walk = self.online_validation.as_mut().ok_or_else(invariant)?;
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
                .advance(riffdb_storage_api::EvidencePageLimit::new(256).ok_or_else(invariant)?)?
        {
            return Ok(false);
        }
        let proof = walk.proof()?;
        if proof.history().lineage() != hold.lineage() {
            return Err(
                if proof.history().lineage().database_id() == hold.lineage().database_id()
                    && proof.history().lineage().history_incarnation()
                        == hold.lineage().history_incarnation()
                {
                    Refusal::StaleEpoch
                } else {
                    Refusal::ForeignLineage
                },
            );
        }
        let holds = proof
            .source()
            .open_table(SOURCE_HOLDS)
            .map_err(table_error)?;
        let row = holds
            .get(hold.storage_key().as_slice())
            .map_err(precommit_storage_error)?
            .ok_or(Refusal::InvalidPosition)?;
        if row.value().len() > MAX_ACKNOWLEDGEMENT_BEFORE_BYTES {
            return Err(storage_error(StorageErrorKind::LimitExceeded).into());
        }
        self.prepared_acknowledgement = Some((hold, row.value().to_vec()));
        // A completed certificate retains no unnecessary validation pin.
        drop(row);
        drop(holds);
        drop(proof);
        walk.release_completed_pin();
        crash_edge("acknowledgement-prepared");
        Ok(true)
    }

    /// Called only for the already prepared closed operation at its ordered
    /// writer turn. False returns the turn without mutating durable state.
    pub(crate) fn advance_prepared_follower_acknowledgement(
        &mut self,
        hold: Hold,
    ) -> Result<bool, Refusal> {
        self.verify_follower_acknowledgement_admission()?;
        if self.accounting.has_pin() {
            return Ok(false);
        }
        let Some((prepared, before)) = self.prepared_acknowledgement.take() else {
            return Ok(false);
        };
        if prepared != hold {
            return Ok(false);
        }
        let Some(walk) = self.online_validation.as_mut() else {
            return Ok(false);
        };
        walk.clear_step_rows();
        let barrier = match Barrier::try_from_continuation_observed(Arc::clone(&self.shared), walk)?
        {
            AdmissionAttempt::Admitted(barrier) => barrier,
            AdmissionAttempt::Yielded(reason) => {
                // Retirement may race the preflight or cancel an existing
                // ticket. It can never become a successful later retry.
                self.verify_follower_acknowledgement_admission()?;
                if reason == AdmissionYield::ChangedQualification {
                    self.retire_online_validation();
                }
                return Ok(false);
            }
        };
        self.eligible_bytes.share_qualified_pin(
            crate::source_validation_pin::ValidationSource::Checkpoint(Arc::clone(&barrier.root)),
            barrier.observation().history,
            self.shared
                .source_continuation_epoch
                .load(Ordering::Acquire),
        )?;
        self.online_validation
            .as_mut()
            .ok_or_else(invariant)?
            .release_completed_pin();
        let Some(publication) = barrier.acknowledge_follower(hold, &before)? else {
            return Ok(false);
        };
        let result = self
            .online_validation
            .as_mut()
            .ok_or_else(invariant)?
            .rebase_own_acknowledgement(&publication);
        if let Err(error) = result {
            self.retire_online_validation();
            return Err(error.into());
        }
        if let Some(license) = self.online_license.as_mut()
            && !license.rebase_own_acknowledgement(&publication)?
        {
            self.online_license = None;
        }
        self.eligible_bytes
            .rebase_own_acknowledgement(&publication)?;
        self.accounting.rebase_own_acknowledgement(&publication)?;
        self.online_validation
            .as_mut()
            .ok_or_else(invariant)?
            .release_completed_pin();
        Ok(true)
    }
}

fn invariant() -> StorageError {
    storage_error(StorageErrorKind::InvariantViolation)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prepare(control: &mut ReplicationSourceControl, hold: Hold) {
        for _ in 0..64 {
            if control.prepare_follower_acknowledgement(hold).unwrap() {
                return;
            }
        }
        panic!("bounded preparation did not reach the exact end");
    }

    fn append(ports: &RedbOperationalPorts, ordinal: u64) -> History {
        let _lease = ports.shared.mutation_gate.acquire().unwrap();
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
                b"ack-proof-fixture".as_slice(),
                ordinal.to_be_bytes().as_slice(),
            )
            .unwrap();
        write.finish().unwrap().commit(&ports.shared).unwrap();
        crate::changelog_v3_roots::read_checkpoint_roots(
            &ports.shared.capture_checkpoint_root().unwrap(),
        )
        .unwrap()
        .unwrap()
    }

    // req: REP-003, REP-006, REC-001, END-008, LOG-001
    #[test]
    fn follower_acknowledgement_reads_initial_population_then_only_checked_successors() {
        for population in [7_u64, 300, 600] {
            let (_scope, ports, states) = crate::changelog_v3_control_tests::fixture();
            for ordinal in 8..=population {
                append(&ports, ordinal);
            }
            let mut control = ports.replication_source_control();
            let hold = Hold::new(
                riffdb_storage_api::ReplicationSourceHoldIdV1::new([1; 16]).unwrap(),
                Kind::FollowerAcknowledgement,
                states[1].lineage(),
                states[1].tail(),
            );
            control.register(hold).unwrap();
            let initial = crate::changelog_v3_roots::read_checkpoint_roots(
                &ports.shared.capture_checkpoint_root().unwrap(),
            )
            .unwrap()
            .unwrap();
            let mut requested = Hold::new(hold.id(), hold.kind(), hold.lineage(), initial.tail());
            prepare(&mut control, requested);
            assert_eq!(
                control.online_validation.as_ref().unwrap().rows_read(),
                [population + 1, 0]
            );
            assert!(
                control
                    .advance_prepared_follower_acknowledgement(requested)
                    .unwrap()
            );
            for ordinal in 0..4 {
                let current = append(&ports, population + ordinal + 1);
                requested = Hold::new(hold.id(), hold.kind(), hold.lineage(), current.tail());
                prepare(&mut control, requested);
                assert!(
                    control
                        .advance_prepared_follower_acknowledgement(requested)
                        .unwrap()
                );
            }
            let root = ports.shared.capture_checkpoint_root().unwrap();
            let oracle = crate::changelog_v3_roots::validate_retained_history(&root)
                .unwrap()
                .unwrap();
            let walk = control.online_validation.as_mut().unwrap();
            let epoch = ports
                .shared
                .source_continuation_epoch
                .load(Ordering::Acquire);
            assert!(walk.continue_on(root, epoch).unwrap());
            assert_eq!(walk.proof().unwrap().history(), oracle);
            assert_eq!(walk.rows_read(), [population + 10, 0]);
            assert!(!ports.shared.mutation_gate.is_held());
        }
    }

    // req: REP-003, REP-006, REC-001, END-008, LOG-001
    #[test]
    fn follower_acknowledgement_refuses_incomplete_stale_and_unclassified_preparation() {
        let (_scope, ports, states) = crate::changelog_v3_control_tests::fixture();
        let mut control = ports.replication_source_control();
        let hold = Hold::new(
            riffdb_storage_api::ReplicationSourceHoldIdV1::new([1; 16]).unwrap(),
            Kind::FollowerAcknowledgement,
            states[1].lineage(),
            states[1].tail(),
        );
        control.register(hold).unwrap();
        let requested = Hold::new(hold.id(), hold.kind(), hold.lineage(), states[5].tail());
        let before = ports.shared.durable_commit_epoch();
        assert!(
            !control
                .advance_prepared_follower_acknowledgement(requested)
                .unwrap()
        );
        assert!(!control.prepare_follower_acknowledgement(requested).unwrap());
        assert!(
            !control
                .advance_prepared_follower_acknowledgement(requested)
                .unwrap()
        );
        assert_eq!(ports.shared.durable_commit_epoch(), before);
        prepare(&mut control, requested);
        {
            let _lease = ports.shared.mutation_gate.acquire().unwrap();
            let write = ports.shared.database.begin_write().unwrap();
            ports.shared.commit_durable(write).unwrap();
        }
        let before = ports.shared.durable_commit_epoch();
        assert!(
            !control
                .advance_prepared_follower_acknowledgement(requested)
                .unwrap()
        );
        assert_eq!(ports.shared.durable_commit_epoch(), before);
        assert!(control.online_validation.is_none());
        prepare(&mut control, requested);
        assert!(
            control
                .advance_prepared_follower_acknowledgement(requested)
                .unwrap()
        );
        let before = ports.shared.durable_commit_epoch();
        prepare(&mut control, requested);
        assert!(
            control
                .advance_prepared_follower_acknowledgement(requested)
                .unwrap()
        );
        assert_eq!(
            ports.shared.durable_commit_epoch(),
            before,
            "equal acknowledgement allocates no receipt"
        );
        prepare(&mut control, hold);
        assert!(matches!(
            control.advance_prepared_follower_acknowledgement(hold),
            Err(Refusal::InvalidPosition)
        ));
        assert_eq!(ports.shared.durable_commit_epoch(), before);
    }
    // req: REP-003, REP-006, REC-001, END-008, LOG-001
    #[test]
    fn follower_acknowledgement_cancellation_releases_candidate_and_single_request_capacity() {
        let (_scope, ports, states) = crate::changelog_v3_control_tests::fixture();
        let mut initial = ports.replication_source_control();
        let hold = Hold::new(
            riffdb_storage_api::ReplicationSourceHoldIdV1::new([1; 16]).unwrap(),
            Kind::FollowerAcknowledgement,
            states[1].lineage(),
            states[1].tail(),
        );
        initial.register(hold).unwrap();
        let request = riffdb_storage_api::ReplicationFollowerAcknowledgementV3::new(
            hold.id(),
            hold.lineage(),
            states[5].tail(),
        );
        let owner = ports.shared_source_control().unwrap();
        let mut job = ports
            .begin_replication_follower_acknowledgement_v3(request)
            .unwrap();
        assert!(
            ports
                .begin_replication_follower_acknowledgement_v3(request)
                .is_err()
        );
        assert!(!job.advance().unwrap());
        let before = ports.shared.durable_commit_epoch();
        let candidate = {
            let owner = owner.lock().unwrap();
            assert!(!owner.online_validation.as_ref().unwrap().is_complete());
            assert!(owner.acknowledgement_active.is_some());
            owner
                .online_validation
                .as_ref()
                .unwrap()
                .validation_pin()
                .unwrap()
        };
        drop(job);
        {
            let owner = owner.lock().unwrap();
            assert!(owner.acknowledgement_active.is_none());
            assert!(owner.online_validation.is_none());
            assert!(owner.prepared_acknowledgement.is_none());
        }
        assert_eq!(ports.shared.durable_commit_epoch(), before);
        assert!(
            candidate.upgrade().is_none(),
            "cancelled preparation releases its actual validation pin"
        );
        let replacement = ports
            .begin_replication_follower_acknowledgement_v3(request)
            .unwrap();
        drop(replacement);
        assert!(owner.lock().unwrap().acknowledgement_active.is_none());
        let bookkeeping = std::mem::size_of::<ReplicationSourceControl>()
            + std::mem::size_of::<RetainedHistoryValidation>()
            + MAX_ACKNOWLEDGEMENT_BEFORE_BYTES
            + 4 * usize::try_from(MAX_REPLICATION_SOURCE_HOLDS_V1).unwrap()
            + riffdb_types::MAX_KEY_BYTES;
        assert!(bookkeeping <= 64 * 1024);
    }

    // req: REP-003, REP-006, REC-001, END-008
    #[test]
    fn follower_acknowledgement_prepared_custody_prevents_background_rest_and_proof_churn() {
        let (_scope, ports, states) = crate::changelog_v3_control_tests::fixture();
        let hold = Hold::new(
            riffdb_storage_api::ReplicationSourceHoldIdV1::new([21; 16]).unwrap(),
            Kind::FollowerAcknowledgement,
            states[1].lineage(),
            states[1].tail(),
        );
        ports.replication_source_control().register(hold).unwrap();
        let current = crate::changelog_v3_roots::read_checkpoint_roots(
            &ports.shared.capture_checkpoint_root().unwrap(),
        )
        .unwrap()
        .unwrap();
        let request = riffdb_storage_api::ReplicationFollowerAcknowledgementV3::new(
            hold.id(),
            hold.lineage(),
            current.tail(),
        );
        let custody = ports
            .begin_replication_follower_acknowledgement_v3(request)
            .unwrap();
        let owner = ports.shared_source_control().unwrap();
        let lane = ports.online_retention_lane();
        {
            let mut control = owner.lock().unwrap();
            prepare(&mut control, request.hold());
            assert!(control.source_work_wait().is_zero());
        }
        // Ordinary writers still advance, without extending an accepted request's
        // rest or giving it a writer turn ahead of an already queued command.
        append(&ports, 8);
        let before = ports.shared.mutation_gate.source_census().unwrap();
        lane.advance_source_validation_off_gate().unwrap();
        {
            let control = owner.lock().unwrap();
            assert!(
                control.source_work_wait().is_zero(),
                "background work must leave the ready candidate's rest untouched"
            );
            assert_eq!(
                control.prepared_acknowledgement.as_ref().unwrap().0,
                request.hold()
            );
        }
        lane.step_timed(
            std::num::NonZeroU64::new(u64::MAX).unwrap(),
            std::num::NonZeroU64::new(1).unwrap(),
            false,
        )
        .unwrap();
        assert_eq!(
            ports.shared.mutation_gate.source_census().unwrap(),
            before,
            "background source work must not consume admission while a prepared candidate owns custody"
        );
        assert!(
            owner
                .lock()
                .unwrap()
                .advance_prepared_follower_acknowledgement(request.hold())
                .unwrap()
        );
        drop(custody);
        // After real completion/drop the same owner can resume normal background work.
        assert!(owner.lock().unwrap().prepared_acknowledgement.is_none());
        lane.advance_source_validation_off_gate().unwrap();
        assert!(!owner.lock().unwrap().source_work_wait().is_zero());
    }

    // req: REP-003, REC-001, LOG-001, END-008
    #[test]
    fn follower_acknowledgement_concurrent_factory_handles_share_one_proof_owner() {
        let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
        let exclusion = ports.shared.source_control_owner.lock().unwrap();
        let (entered, arrivals) = std::sync::mpsc::sync_channel(2);
        let (first, second) = std::thread::scope(|scope| {
            let left = scope.spawn(|| {
                entered.send(()).unwrap();
                ports.shared_source_control().unwrap()
            });
            let right = scope.spawn(|| {
                entered.send(()).unwrap();
                ports.shared_source_control().unwrap()
            });
            arrivals.recv().unwrap();
            arrivals.recv().unwrap();
            drop(exclusion);
            (left.join().unwrap(), right.join().unwrap())
        });
        assert!(Arc::ptr_eq(&first, &second));
        let weak = Arc::downgrade(&first);
        drop(first);
        drop(second);
        assert!(
            weak.upgrade().is_none(),
            "the database's weak link cannot retain its proof owner"
        );
    }

    // req: REP-003, REP-006, REC-001, LOG-001, END-008
    #[test]
    fn follower_acknowledgement_retired_admission_is_terminal_and_preserves_durable_hold() {
        let (_scope, ports, states) = crate::changelog_v3_control_tests::fixture();
        let hold = Hold::new(
            riffdb_storage_api::ReplicationSourceHoldIdV1::new([41; 16]).unwrap(),
            Kind::FollowerAcknowledgement,
            states[1].lineage(),
            states[1].tail(),
        );
        ports.replication_source_control().register(hold).unwrap();
        let request = riffdb_storage_api::ReplicationFollowerAcknowledgementV3::new(
            hold.id(),
            hold.lineage(),
            states[5].tail(),
        );
        let mut custody = ports
            .begin_replication_follower_acknowledgement_v3(request)
            .unwrap();
        let owner = ports.shared_source_control().unwrap();
        {
            let mut control = owner.lock().unwrap();
            prepare(&mut control, request.hold());
            control.charge_source_work(std::time::Instant::now());
        }
        let before = ports.shared.durable_commit_epoch();
        let lane = ports.online_retention_lane();
        lane.retire_source_admission();
        // Permanent retirement is a certain refusal, even if a prepared
        // candidate has scheduling rest left. It must never become a retry loop.
        assert!(matches!(custody.wait_duration(),
            Err(Refusal::Storage(error)) if error.kind() == StorageErrorKind::Unavailable));
        assert!(matches!(custody.advance(),
            Err(Refusal::Storage(error)) if error.kind() == StorageErrorKind::Unavailable));
        assert!(
            matches!(riffdb_storage_api::ReplicationRegistrationMaintenancePort::advance_prepared_follower_acknowledgement_v3(&ports, request),
            Err(Refusal::Storage(error)) if error.kind() == StorageErrorKind::Unavailable)
        );
        assert!(
            matches!(owner.lock().unwrap().advance_prepared_follower_acknowledgement(request.hold()),
            Err(Refusal::Storage(error)) if error.kind() == StorageErrorKind::Unavailable)
        );
        drop(custody);
        assert!(owner.lock().unwrap().acknowledgement_active.is_none());
        assert!(owner.lock().unwrap().prepared_acknowledgement.is_none());
        assert!(
            matches!(ports.begin_replication_follower_acknowledgement_v3(request),
            Err(Refusal::Storage(error)) if error.kind() == StorageErrorKind::Unavailable)
        );
        assert_eq!(ports.shared.durable_commit_epoch(), before);
        let root = ports.shared.capture_checkpoint_root().unwrap();
        let holds = root.open_table(SOURCE_HOLDS).unwrap();
        let row = holds.get(hold.storage_key().as_slice()).unwrap().unwrap();
        assert_eq!(
            *decode_replication_source_hold_v1(row.value())
                .unwrap()
                .value(),
            hold
        );
        crate::changelog_v3_roots::validate_retained_history(&root)
            .unwrap()
            .unwrap();
    }

    // req: REP-003, REC-001, STO-030, STO-031, END-008
    #[test]
    fn follower_acknowledgement_shared_scheduler_binding_never_resets_charged_rest() {
        let (_scope, ports, states) = crate::changelog_v3_control_tests::fixture();
        let owner = ports.shared_source_control().unwrap();
        let now = std::time::Instant::now();
        {
            let mut control = owner.lock().unwrap();
            // Explicit storage steps have no graph scheduling clock. They
            // retain all native validation and admission limits regardless.
            control.charge_source_work(now);
            assert!(control.source_pacing.is_none());
            assert!(control.source_work_wait().is_zero());
            control.bind_source_scheduler();
            control
                .source_pacing
                .as_mut()
                .unwrap()
                .charge(now, std::time::Duration::from_secs(1));
            let rest = control.source_pacing.as_ref().unwrap().wait(now);
            assert_eq!(rest, std::time::Duration::from_secs(19));
            control.bind_source_scheduler();
            assert_eq!(control.source_pacing.as_ref().unwrap().wait(now), rest);
        }
        let request = riffdb_storage_api::ReplicationFollowerAcknowledgementV3::new(
            riffdb_storage_api::ReplicationSourceHoldIdV1::new([43; 16]).unwrap(),
            states[1].lineage(),
            states[1].tail(),
        );
        let custody = ports
            .begin_replication_follower_acknowledgement_v3(request)
            .unwrap();
        assert_eq!(
            owner
                .lock()
                .unwrap()
                .source_pacing
                .as_ref()
                .unwrap()
                .wait(now),
            std::time::Duration::from_secs(19)
        );
        drop(custody);
        assert_eq!(
            owner
                .lock()
                .unwrap()
                .source_pacing
                .as_ref()
                .unwrap()
                .wait(now),
            std::time::Duration::from_secs(19)
        );
    }

    // req: REP-003, REP-006, REC-001, LOG-001, END-008
    #[test]
    fn follower_acknowledgement_qualification_overflow_refuses_before_durability() {
        let (_scope, ports, states) = crate::changelog_v3_control_tests::fixture();
        let mut control = ports.replication_source_control();
        let hold = Hold::new(
            riffdb_storage_api::ReplicationSourceHoldIdV1::new([1; 16]).unwrap(),
            Kind::FollowerAcknowledgement,
            states[1].lineage(),
            states[1].tail(),
        );
        control.register(hold).unwrap();
        ports
            .shared
            .source_continuation_epoch
            .store(u64::MAX - 1, Ordering::Release);
        let requested = Hold::new(hold.id(), hold.kind(), hold.lineage(), states[5].tail());
        prepare(&mut control, requested);
        let before = ports.shared.durable_commit_epoch();
        assert!(
            matches!(control.advance_prepared_follower_acknowledgement(requested),
            Err(Refusal::Storage(error)) if error.kind() == StorageErrorKind::SequenceExhausted)
        );
        assert_eq!(ports.shared.durable_commit_epoch(), before);
    }

    // req: REP-003, REP-006, REC-001, LOG-001, END-008
    #[test]
    fn follower_acknowledgement_crash_child() {
        let Some(path) = std::env::var_os("RIFFDB_ACK_CRASH_PATH") else {
            return;
        };
        let store = crate::RedbStore::open(path).unwrap();
        let ports = RedbOperationalPorts {
            shared: store.shared,
        };
        let before = crate::changelog_v3_roots::validate_retained_history(
            &ports.shared.capture_checkpoint_root().unwrap(),
        )
        .unwrap()
        .unwrap();
        let request = Hold::new(
            riffdb_storage_api::ReplicationSourceHoldIdV1::new([1; 16]).unwrap(),
            Kind::FollowerAcknowledgement,
            before.lineage(),
            before.tail(),
        );
        let mut control = ports.replication_source_control();
        prepare(&mut control, request);
        assert!(
            control
                .advance_prepared_follower_acknowledgement(request)
                .unwrap()
        );
        assert_eq!(
            std::env::var("RIFFDB_ACK_CRASH_NORMAL").as_deref(),
            Ok("true")
        );
    }

    // req: REP-003, REP-006, REC-001, LOG-001, END-008
    #[test]
    fn follower_acknowledgement_process_crashes_preserve_atomic_hold_receipt_and_exact_retry() {
        use std::process::{Command, Stdio};
        for edge in [
            "acknowledgement-prepared",
            "hold-staged",
            "hold-receipted",
            "hold-committed",
            "normal",
        ] {
            let (scope, ports, states) = crate::changelog_v3_control_tests::fixture();
            let path = scope.join("db.redb");
            let mut control = ports.replication_source_control();
            let prior = Hold::new(
                riffdb_storage_api::ReplicationSourceHoldIdV1::new([1; 16]).unwrap(),
                Kind::FollowerAcknowledgement,
                states[1].lineage(),
                states[1].tail(),
            );
            control.register(prior).unwrap();
            let root = ports.shared.capture_checkpoint_root().unwrap();
            let before = crate::changelog_v3_roots::validate_retained_history(&root)
                .unwrap()
                .unwrap();
            let request = Hold::new(prior.id(), prior.kind(), prior.lineage(), before.tail());
            let original: Vec<_> = root
                .open_table(HISTORY)
                .unwrap()
                .iter()
                .unwrap()
                .map(|entry| {
                    let (key, value) = entry.unwrap();
                    (key.value().to_vec(), value.value().to_vec())
                })
                .collect();
            drop(root);
            drop(control);
            drop(ports);
            let mut child = Command::new(std::env::current_exe().unwrap());
            child.args(["--exact", "store::changelog_source_control::follower_acknowledgement::tests::follower_acknowledgement_crash_child", "--nocapture"])
                .env("RIFFDB_ACK_CRASH_PATH", &path).stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::inherit());
            if edge == "normal" {
                child.env("RIFFDB_ACK_CRASH_NORMAL", "true");
            } else {
                child.env("RIFFDB_V3_SOURCE_CONTROL_EDGE", edge);
            }
            assert_eq!(
                child.status().unwrap().code(),
                Some(if edge == "normal" { 0 } else { 93 }),
                "{edge}"
            );
            let committed = matches!(edge, "hold-committed" | "normal");
            for reopen in 0..2 {
                let store = crate::RedbStore::open(&path).unwrap();
                let ports = RedbOperationalPorts {
                    shared: store.shared,
                };
                let root = ports.shared.capture_checkpoint_root().unwrap();
                let recovered = crate::changelog_v3_roots::validate_retained_history(&root)
                    .unwrap()
                    .unwrap();
                let has_ack = committed || reopen > 0;
                assert_eq!(
                    recovered.tail().sequence().get(),
                    before.tail().sequence().get() + u64::from(has_ack),
                    "{edge}"
                );
                assert_eq!(recovered.tail().frontier(), before.tail().frontier());
                let holds = root.open_table(SOURCE_HOLDS).unwrap();
                let row = holds.get(prior.storage_key().as_slice()).unwrap().unwrap();
                assert_eq!(
                    *decode_replication_source_hold_v1(row.value())
                        .unwrap()
                        .value(),
                    if has_ack { request } else { prior }
                );
                let history = root.open_table(HISTORY).unwrap();
                for (key, value) in &original {
                    assert_eq!(
                        history.get(key.as_slice()).unwrap().unwrap().value(),
                        value.as_slice()
                    );
                }
                drop(history);
                drop(row);
                drop(holds);
                drop(root);
                let mut control = ports.replication_source_control();
                prepare(&mut control, request);
                assert!(
                    control
                        .advance_prepared_follower_acknowledgement(request)
                        .unwrap()
                );
                let after = crate::changelog_v3_roots::validate_retained_history(
                    &ports.shared.capture_checkpoint_root().unwrap(),
                )
                .unwrap()
                .unwrap();
                assert_eq!(
                    after.tail().sequence(),
                    before.tail().sequence().checked_next().unwrap()
                );
                assert_eq!(after.minimum_resume(), before.minimum_resume());
            }
        }
    }
}
