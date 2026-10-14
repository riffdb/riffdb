// req: STO-030, STO-031, STO-032, REC-001, END-007, END-008
use super::*;
use crate::changelog_v3_retained_validation::RetainedHistoryValidation;
use crate::journal::SourceReceiptPreimage;

fn append(ports: &RedbOperationalPorts, size: usize) {
    let write = crate::changelog_v3_write::CapturedImmediateWrite::begin(
        &ports.shared.database,
        crate::RedbCommitProfile::Hardened,
        riffdb_storage_api::ChangelogAttributionV3::OutboxTransition,
    )
    .unwrap();
    write
        .open_table(crate::layout::OUTBOX_STATUS)
        .unwrap()
        .insert(b"source-bytes-fixture".as_slice(), vec![1; size].as_slice())
        .unwrap();
    write.finish().unwrap().commit(&ports.shared).unwrap();
}

fn validated(ports: &RedbOperationalPorts) -> (ValidationSource, History, u64) {
    let source = ports.shared.capture_source_validation_pin().unwrap();
    let mut walk = RetainedHistoryValidation::new_source(source.clone())
        .unwrap()
        .unwrap();
    for _ in 0..32 {
        if walk
            .advance(riffdb_storage_api::EvidencePageLimit::new(256).unwrap())
            .unwrap()
        {
            break;
        }
    }
    let history = walk.complete().unwrap().history();
    (
        source,
        history,
        ports
            .shared
            .source_continuation_epoch
            .load(Ordering::Acquire),
    )
}

fn oracle(source: &ValidationSource, history: History, ceiling: u64) -> u64 {
    // Independent exact walk on the actual immutable pin. No continued counters.
    (history.minimum_resume().sequence().get()..ceiling)
        .map(|seq| {
            let sequence = riffdb_storage_api::ChangelogTransactionSequence::new(seq).unwrap();
            source
                .receipt(sequence, None)
                .unwrap()
                .unwrap()
                .encode()
                .unwrap()
                .len() as u64
        })
        .sum()
}

fn finish(probe: &mut ContinuedProbe, now: Instant) -> [u64; 18] {
    for _ in 0..32 {
        if let Some(sample) = probe.advance(now).unwrap() {
            return sample;
        }
    }
    panic!("bounded fixture must finish its exact source interval");
}

#[test]
fn source_byte_continuation_matches_exact_pinned_full_walk() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    for row in 0..300 {
        append(&ports, if row % 2 == 0 { 17 } else { 4096 });
    }
    let mut control = ports.replication_source_control();
    for _ in 0..16 {
        assert_eq!(control.reclaim_history_online().unwrap().0, 0);
        for _ in 0..8 {
            control.advance_validation_off_gate().unwrap();
        }
        if control.reclamation_yields()[0] == 1 {
            break;
        }
    }
    assert_eq!(control.reclamation_yields()[0], 1);
    let (source, original, epoch) = validated(&ports);
    let now = Instant::now();
    let ceiling = original.tail().sequence().get();
    let expected = oracle(&source, original, ceiling);
    let mut probe =
        ContinuedProbe::capture(None, source.clone(), original, ceiling, epoch, now).unwrap();
    let first = finish(&mut probe, now);
    assert_eq!(first[5], expected);
    assert_eq!(first[10], 0);
    assert_eq!(first[11], first[4]);
    assert_eq!(first[9], 2);
    assert!(probe.root.is_none());
    drop(source);

    append(&ports, 512);
    let (source, before_cut, epoch) = validated(&ports);
    assert_eq!(before_cut.tail().sequence().get(), ceiling + 1);
    let mut probe = ContinuedProbe::capture(
        Some(probe),
        source.clone(),
        before_cut,
        before_cut.tail().sequence().get(),
        epoch,
        now + CAPTURE_INTERVAL,
    )
    .unwrap();
    let append_sample = finish(&mut probe, now + CAPTURE_INTERVAL);
    assert_eq!(
        append_sample[5],
        oracle(&source, before_cut, before_cut.tail().sequence().get())
    );
    assert_eq!(append_sample[10], first[4]);
    assert_eq!((append_sample[11], append_sample[9]), (1, 1));
    let successor_floor_seq = riffdb_storage_api::ChangelogTransactionSequence::new(
        before_cut.minimum_resume().sequence().get() + 256,
    )
    .unwrap();
    let floor = riffdb_storage_api::ChangelogHistoryPointV3::from_receipt(
        &source.receipt(successor_floor_seq, None).unwrap().unwrap(),
    )
    .unwrap();
    let rows = (before_cut.minimum_resume().sequence().get()..floor.sequence().get())
        .map(|seq| {
            let sequence = riffdb_storage_api::ChangelogTransactionSequence::new(seq).unwrap();
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
    let proof = SourceReclamationProof::new(
        before_cut,
        History::new(
            before_cut.lineage(),
            before_cut.anchor(),
            before_cut.tail(),
            floor,
        )
        .unwrap(),
        rows,
    )
    .unwrap();
    let mut partial =
        ContinuedProbe::capture(None, source, before_cut, ceiling, epoch, now).unwrap();
    assert!(partial.advance(now).unwrap().is_none());
    assert_eq!(partial.next, floor.sequence().get());
    let mut removed = 0;
    for _ in 0..16 {
        removed += control.reclaim_history_online().unwrap().0;
        if removed > 0 {
            break;
        }
        for _ in 0..8 {
            control.advance_validation_off_gate().unwrap();
        }
    }
    assert_eq!(removed, 256);
    control.retire_online_validation();
    let (source, after_cut, qualification) = validated(&ports);
    assert_eq!(after_cut.minimum_resume(), floor);
    assert!(
        probe
            .rebase(source.clone(), &proof, epoch, qualification)
            .unwrap()
    );
    assert!(
        partial
            .rebase(source.clone(), &proof, epoch, qualification)
            .unwrap()
    );
    let partial_sample = finish(&mut partial, now);
    assert_eq!(partial_sample[5], oracle(&source, after_cut, ceiling));
    assert_eq!(
        partial_sample[12], 256,
        "subtract only rows that were actually scanned"
    );
    let mut probe = ContinuedProbe::capture(
        Some(probe),
        source.clone(),
        after_cut,
        before_cut.tail().sequence().get(),
        qualification,
        now + CAPTURE_INTERVAL * 2,
    )
    .unwrap();
    let cut_sample = finish(&mut probe, now + CAPTURE_INTERVAL * 2);
    assert_eq!(
        cut_sample[5],
        oracle(&source, after_cut, before_cut.tail().sequence().get())
    );
    assert_eq!(cut_sample[10], cut_sample[4]);
    assert_eq!((cut_sample[11], cut_sample[9]), (0, 0));
    assert!(probe.root.is_none());
}

#[test]
fn source_byte_continuation_reconstructs_changed_ceiling_and_revoked_qualification() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let (source, history, epoch) = validated(&ports);
    let now = Instant::now();
    let mut probe = ContinuedProbe::capture(
        None,
        source.clone(),
        history,
        history.tail().sequence().get(),
        epoch,
        now,
    )
    .unwrap();
    finish(&mut probe, now);
    let ceiling = history.tail().sequence().get() - 2;
    let mut changed = ContinuedProbe::capture(
        Some(probe),
        source.clone(),
        history,
        ceiling,
        epoch,
        now + CAPTURE_INTERVAL,
    )
    .unwrap();
    let sample = finish(&mut changed, now + CAPTURE_INTERVAL);
    assert_eq!(sample[10], 0);
    assert_eq!(sample[5], oracle(&source, history, ceiling));
    ports.shared.invalidate_source_continuation();
    let (source, history, epoch) = validated(&ports);
    let mut changed = ContinuedProbe::capture(
        Some(changed),
        source.clone(),
        history,
        ceiling,
        epoch,
        now + CAPTURE_INTERVAL * 2,
    )
    .unwrap();
    let sample = finish(&mut changed, now + CAPTURE_INTERVAL * 2);
    assert_eq!(sample[10], 0);
    assert_eq!(sample[5], oracle(&source, history, ceiling));
    assert!(
        ContinuedProbe::capture(Some(changed), source, history, ceiling, u64::MAX, now).is_err()
    );
}

#[test]
fn source_byte_continuation_expiry_overflow_and_retirement_keep_unknown_and_drop_pins() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let (source, history, epoch) = validated(&ports);
    let weak = Arc::downgrade(source.checkpoint_pin());
    let now = Instant::now();
    let mut observer = Observer::default();
    observer
        .capture_qualified(source, history, history.tail().sequence().get(), epoch, now)
        .unwrap();
    assert!(observer.pending());
    observer.advance(now + MAX_AGE).unwrap();
    assert!(!observer.pending());
    assert!(weak.upgrade().is_none());
    let (source, history, epoch) = validated(&ports);
    observer
        .capture_qualified(
            source,
            history,
            history.tail().sequence().get(),
            epoch,
            now + CAPTURE_INTERVAL,
        )
        .unwrap();
    observer.continued.as_mut().unwrap().bytes = u64::MAX;
    assert_eq!(
        observer.advance(now + CAPTURE_INTERVAL).unwrap_err().kind(),
        StorageErrorKind::LimitExceeded
    );
    assert!(observer.continued.is_none());
    let (source, history, epoch) = validated(&ports);
    observer
        .capture_qualified(
            source,
            history,
            history.tail().sequence().get(),
            epoch,
            now + CAPTURE_INTERVAL * 2,
        )
        .unwrap();
    observer.check_qualification(epoch + 1);
    assert!(observer.continued.is_none());
    observer.retire();
    assert!(observer.probe.is_none());
    assert!(!observer.pending());
}

#[test]
fn actual_v2_source_producer_matches_its_pinned_canonical_bytes_and_qualified_reuse() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    for row in 0..300 {
        append(&ports, if row % 2 == 0 { 17 } else { 4096 });
    }
    let (source, history, epoch) = validated(&ports);
    let now = Instant::now();
    let expected = oracle(&source, history, history.tail().sequence().get());
    let mut observer = Observer::default();
    observer
        .capture_qualified(source, history, history.tail().sequence().get(), epoch, now)
        .unwrap();
    observer.advance(now).unwrap();
    assert!(observer.pending());
    observer.advance(now).unwrap();
    assert!(!observer.pending());
    let sample = observer.continued.as_ref().unwrap().sample(now).unwrap();
    assert_eq!(sample[5], expected);
    assert_eq!(sample[10], 0);
    assert_eq!(sample[9], 2);
    append(&ports, 512);
    let (source, after, epoch) = validated(&ports);
    let expected = oracle(&source, after, after.tail().sequence().get());
    observer
        .capture_qualified(
            source,
            after,
            after.tail().sequence().get(),
            epoch,
            now + CAPTURE_INTERVAL,
        )
        .unwrap();
    observer.advance(now + CAPTURE_INTERVAL).unwrap();
    let new = observer
        .continued
        .as_ref()
        .unwrap()
        .sample(now + CAPTURE_INTERVAL)
        .unwrap();
    assert_eq!(new[5], expected);
    assert_eq!(new[10], sample[4]);
    assert_eq!((new[11], new[9]), (1, 1));
}

#[test]
fn qualified_accounting_candidate_shares_new_validated_pin_without_restarting_its_capture() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    for row in 0..300 {
        append(&ports, if row % 2 == 0 { 17 } else { 4096 });
    }
    let (source, history, epoch) = validated(&ports);
    let weak = Arc::downgrade(source.checkpoint_pin());
    let now = Instant::now();
    let ceiling = history.tail().sequence().get();
    let mut probe = ContinuedProbe::capture(None, source, history, ceiling, epoch, now).unwrap();
    assert!(probe.advance(now).unwrap().is_none());
    let counters = (
        probe.next,
        probe.checked_rows,
        probe.pages,
        probe.bytes,
        probe.captured,
    );
    append(&ports, 512);
    let (source, current, epoch) = validated(&ports);
    let expected = oracle(&source, current, ceiling);
    assert!(probe.share_pin(source.clone(), current, epoch).unwrap());
    assert!(
        weak.upgrade().is_none(),
        "the obsolete accounting pin is released before checkpoint capture"
    );
    assert_eq!(
        probe.root.as_ref().unwrap().publication(),
        source.publication()
    );
    assert_eq!(
        (
            probe.next,
            probe.checked_rows,
            probe.pages,
            probe.bytes,
            probe.captured
        ),
        counters
    );
    let sample = finish(&mut probe, now);
    assert_eq!(sample[5], expected);
    assert_eq!(sample[10], 0);
    assert_eq!(sample[11], sample[4]);
    assert_eq!(sample[9], 2);
}

#[test]
fn captured_source_byte_pages_progress_during_source_proof_rest_without_advancing_authority() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    for row in 0..300 {
        append(&ports, if row % 2 == 0 { 17 } else { 4096 });
    }
    let (source, history, epoch) = validated(&ports);
    let ceiling = history.tail().sequence().get();
    let expected = oracle(&source, history, ceiling);
    let lane = ports.online_retention_lane();
    let owner = ports.shared_source_control().unwrap();
    let gate_before = ports.shared.mutation_gate.source_census().unwrap();
    let rest_clock = Instant::now();
    let rest_before;
    {
        let mut control = owner.lock().unwrap();
        control.bind_source_scheduler();
        control.online_validation = RetainedHistoryValidation::new_source(source.clone()).unwrap();
        control
            .online_validation
            .as_mut()
            .unwrap()
            .bind_qualification(epoch);
        control
            .eligible_bytes
            .capture_qualified(source, history, ceiling, epoch, Instant::now())
            .unwrap();
        // Charge actual proof work through the production policy. No sleep or
        // writer priority is used to arrange the pending-observation schedule.
        control.charge_source_work(Instant::now().checked_sub(Duration::from_secs(1)).unwrap());
        assert!(!control.source_background_work_ready());
        rest_before = control.source_pacing.as_ref().unwrap().wait(rest_clock);
        assert!(control.eligible_bytes.pending());
    }
    assert!(lane.captured_read_only_observation_pending().unwrap());
    for _ in 0..2 {
        let phases = lane.advance_captured_observations_off_gate().unwrap();
        assert!(
            !phases.source_work_wait.is_zero(),
            "the worker must observe the actual charged source rest"
        );
        assert_eq!(phases.source_initial_rows, 0);
        assert_eq!(phases.source_continuation_rows, 0);
        assert_eq!(phases.reclaimed_receipts, 0);
        assert_eq!(phases.gate_held_us(), 0);
    }
    assert!(!lane.captured_read_only_observation_pending().unwrap());
    assert!(lane.captured_observations_pending().unwrap());
    let (step, phases) = lane
        .step_timed(
            std::num::NonZeroU64::new(1).unwrap(),
            std::num::NonZeroU64::new(256).unwrap(),
            false,
        )
        .unwrap();
    assert_eq!(
        step,
        crate::OnlineRetentionStep::Held(crate::OnlineRetentionHold::TargetReached)
    );
    assert!(
        !phases.source_work_wait.is_zero(),
        "an idle native turn must report its outstanding proof rest"
    );
    assert!(phases.receipt_validation_pending);
    assert_eq!(phases.reclaimed_receipts, 0);
    assert_eq!(phases.gate_held_us(), 0);
    let control = owner.lock().unwrap();
    assert!(
        !control.source_background_work_ready(),
        "observation work cannot spend or reset proof rest"
    );
    assert_eq!(
        control.source_pacing.as_ref().unwrap().wait(rest_clock),
        rest_before
    );
    assert!(!control.online_validation.as_ref().unwrap().is_complete());
    assert_eq!(
        control.online_validation.as_ref().unwrap().step_rows(),
        [0; 2]
    );
    assert!(
        !control.eligible_bytes.pending(),
        "the two bounded observation pages must reach their exact ceiling during proof rest"
    );
    let sample = control
        .eligible_bytes
        .continued
        .as_ref()
        .unwrap()
        .sample(Instant::now())
        .unwrap();
    assert_eq!(sample[5], expected);
    assert_eq!(
        sample[11],
        ceiling - history.minimum_resume().sequence().get()
    );
    assert_eq!(sample[9], 2);
    assert_eq!(
        ports.shared.mutation_gate.source_census().unwrap(),
        gate_before
    );
    assert_eq!(
        ports
            .shared
            .capture_source_validation_pin()
            .unwrap()
            .history()
            .unwrap(),
        Some(history)
    );
}

#[test]
fn captured_source_bytes_revoke_lost_qualification_during_proof_rest_without_new_capture() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let (source, history, epoch) = validated(&ports);
    let lane = ports.online_retention_lane();
    let owner = ports.shared_source_control().unwrap();
    let rest_clock = Instant::now();
    let rest_before;
    let gate_before = ports.shared.mutation_gate.source_census().unwrap();
    {
        let mut control = owner.lock().unwrap();
        control.bind_source_scheduler();
        control
            .eligible_bytes
            .capture_qualified(
                source,
                history,
                history.tail().sequence().get(),
                epoch,
                Instant::now(),
            )
            .unwrap();
        control.charge_source_work(Instant::now().checked_sub(Duration::from_secs(1)).unwrap());
        rest_before = control.source_pacing.as_ref().unwrap().wait(rest_clock);
        assert!(control.eligible_bytes.pending());
    }
    ports.shared.invalidate_source_continuation();
    let phases = lane.advance_captured_observations_off_gate().unwrap();
    assert!(!lane.captured_read_only_observation_pending().unwrap());
    assert_eq!(phases.source_initial_rows, 0);
    assert_eq!(phases.gate_held_us(), 0);
    let control = owner.lock().unwrap();
    assert!(control.eligible_bytes.continued.is_none());
    assert!(control.online_validation.is_none());
    assert!(!control.source_background_work_ready());
    assert_eq!(
        control.source_pacing.as_ref().unwrap().wait(rest_clock),
        rest_before
    );
    assert_eq!(
        ports.shared.mutation_gate.source_census().unwrap(),
        gate_before
    );
    assert_eq!(
        ports
            .shared
            .capture_source_validation_pin()
            .unwrap()
            .history()
            .unwrap(),
        Some(history)
    );
}

#[test]
fn completed_source_byte_prefix_refreshes_while_unrelated_tail_proof_is_incomplete() {
    fixed_prefix_refresh(crate::RedbCommitProfile::Standard);
}

#[test]
fn hardened_completed_source_byte_prefix_refreshes_while_unrelated_tail_proof_is_incomplete() {
    fixed_prefix_refresh(crate::RedbCommitProfile::Hardened);
}

#[test]
fn growing_source_byte_prefix_refreshes_while_unrelated_tail_proof_is_incomplete() {
    prefix_refresh(crate::RedbCommitProfile::Standard, true, false);
}

#[test]
fn hardened_growing_source_byte_prefix_refreshes_while_unrelated_tail_proof_is_incomplete() {
    prefix_refresh(crate::RedbCommitProfile::Hardened, true, false);
}

fn fixed_prefix_refresh(profile: crate::RedbCommitProfile) {
    prefix_refresh(profile, false, false);
}

#[test]
fn absent_source_byte_accounting_captures_a_licensed_prefix_without_waiting_for_tail_proof() {
    prefix_refresh(crate::RedbCommitProfile::Standard, false, true);
}

#[test]
fn hardened_absent_source_byte_accounting_captures_a_licensed_prefix_without_waiting_for_tail_proof()
 {
    prefix_refresh(crate::RedbCommitProfile::Hardened, false, true);
}

fn prefix_refresh(profile: crate::RedbCommitProfile, growing: bool, without_accounting: bool) {
    let (_scope, mut ports, _) = crate::changelog_v3_control_tests::fixture();
    Arc::get_mut(&mut ports.shared)
        .unwrap()
        .application_commit_profile = profile;
    for ordinal in 0..300 {
        append(&ports, if ordinal % 2 == 0 { 17 } else { 31 });
    }
    let previous = {
        let root = ports.shared.capture_checkpoint_root().unwrap();
        Observation {
            publication: root.identity(),
            durable_epoch: ports.shared.durable_commit_epoch(),
            history: crate::changelog_v3_roots::read_checkpoint_roots(&root)
                .unwrap()
                .unwrap(),
        }
    };
    append(&ports, 47);
    let checkpoint = {
        let root = ports.shared.capture_checkpoint_root().unwrap();
        Observation {
            publication: root.identity(),
            durable_epoch: ports.shared.durable_commit_epoch(),
            history: crate::changelog_v3_roots::read_checkpoint_roots(&root)
                .unwrap()
                .unwrap(),
        }
    };
    let (source, history, epoch) = validated(&ports);
    let license =
        checkpoint_license::CheckpointLicense::new(previous, checkpoint, history, epoch).unwrap();
    assert!(license.covers(&source, history, checkpoint, epoch).unwrap());
    let ceiling = license.ceiling().sequence().get();
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
    walk.release_completed_pin();
    let mut control = ports.replication_source_control();
    control.online_validation = Some(walk);
    control.online_license = Some(license);
    control.observed = Some(checkpoint);
    let expected = oracle(&source, history, ceiling);
    let initial_ceiling = if growing { ceiling - 257 } else { ceiling };
    let held_prefix = growing.then(|| {
        let point = riffdb_storage_api::ChangelogHistoryPointV3::from_receipt(
            &source
                .receipt(
                    riffdb_storage_api::ChangelogTransactionSequence::new(initial_ceiling).unwrap(),
                    None,
                )
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        crate::derived_source_pin::RedbDerivedSourcePin::capture(
            ports.begin_composite_read().unwrap(),
            Arc::clone(&ports.shared.derived_source_pins),
        )
        .unwrap()
        .at(point)
        .unwrap()
    });
    {
        let mut derived = ports.shared.derived_source_pins.reclamation().unwrap();
        let (_, observation) = retention::floor_on_source(
            &source,
            history,
            license.ceiling(),
            derived.oldest(history.lineage()),
        )
        .unwrap();
        assert_eq!(observation[5], initial_ceiling);
    }
    let initial_expected = oracle(&source, history, initial_ceiling);
    let captured = Instant::now();
    // Establish a genuine exact interval, then make the next ordinary cadence
    // due deterministically. No capture is extended and no pending page expires.
    if !without_accounting {
        control.eligible_bytes.next_capture = None;
        control
            .eligible_bytes
            .capture_qualified(source, history, initial_ceiling, epoch, captured)
            .unwrap();
        for _ in 0..4 {
            control.advance_source_byte_observation_off_gate().unwrap();
        }
        let prior = control.eligible_bytes.continued.as_ref().unwrap();
        assert!(prior.completed.is_some());
        assert_eq!(prior.bytes, initial_expected);
        assert!(prior.root.is_none());
    } else {
        assert!(control.eligible_bytes.continued.is_none());
        drop(source);
    }
    control.eligible_bytes.next_capture = Some(captured);
    // Releasing a real derived hold expands eligibility inside the same finite
    // checkpoint license; no new mutation authority or tail proof is supplied.
    drop(held_prefix);

    for ordinal in 0..600 {
        append(&ports, if ordinal % 2 == 0 { 41 } else { 43 });
    }
    let gate_before = ports.shared.mutation_gate.source_census().unwrap();
    assert_eq!(control.reclaim_history_online().unwrap().0, 0);
    let walk = control.online_validation.as_ref().unwrap();
    assert!(
        !walk.is_complete(),
        "the unrelated suffix still needs exact validation"
    );
    assert_eq!(
        control.online_license.unwrap().ceiling().sequence().get(),
        ceiling
    );
    assert_eq!(
        ports.shared.mutation_gate.source_census().unwrap(),
        gate_before
    );
    let refreshed = control.eligible_bytes.continued.as_ref().unwrap();
    assert!(
        refreshed.captured > captured,
        "licensed accounting must capture without waiting for the unrelated source tail"
    );
    if growing || without_accounting {
        assert!(refreshed.pending());
        assert!(refreshed.sample(Instant::now()).is_err());
        assert_eq!(refreshed.checked_rows, 256);
        assert_eq!(refreshed.pages, 1);
    }
    control.advance_source_byte_observation_off_gate().unwrap();
    let refreshed = control.eligible_bytes.continued.as_ref().unwrap();
    let sample = refreshed.sample(Instant::now()).unwrap();
    assert_eq!(sample[5], expected);
    if without_accounting {
        assert_eq!(sample[10], 0);
        assert_eq!(sample[11], sample[4]);
        assert_eq!(sample[9], 2);
    } else if growing {
        assert_eq!(
            sample[10],
            initial_ceiling - history.minimum_resume().sequence().get()
        );
        assert_eq!((sample[11], sample[9]), (257, 2));
        assert_eq!(sample[10] + sample[11], sample[4]);
    } else {
        assert_eq!(sample[10], sample[4]);
        assert_eq!((sample[11], sample[9]), (0, 0));
    }
    assert!(refreshed.root.is_none());
    assert_eq!(
        control.eligible_bytes.next_capture,
        Some(refreshed.captured + CAPTURE_INTERVAL),
        "a new observation preserves the original cadence"
    );
    let refreshed_at = refreshed.captured;
    assert!(!control.online_validation.as_ref().unwrap().is_complete());
    assert_eq!(
        ports
            .shared
            .capture_source_validation_pin()
            .unwrap()
            .history()
            .unwrap()
            .unwrap()
            .minimum_resume(),
        history.minimum_resume()
    );

    // A newly protected subinterval has no independently exact prefix total.
    // Even with unchanged native qualification it cannot refresh the larger
    // previously eligible count as though that were the current floor.
    let current = ports.shared.capture_source_validation_pin().unwrap();
    let sequence = riffdb_storage_api::ChangelogTransactionSequence::new(
        history.minimum_resume().sequence().get() + 10,
    )
    .unwrap();
    let point = riffdb_storage_api::ChangelogHistoryPointV3::from_receipt(
        &current.receipt(sequence, None).unwrap().unwrap(),
    )
    .unwrap();
    drop(current);
    let pin = crate::derived_source_pin::RedbDerivedSourcePin::capture(
        ports.begin_composite_read().unwrap(),
        Arc::clone(&ports.shared.derived_source_pins),
    )
    .unwrap()
    .at(point)
    .unwrap();
    control.eligible_bytes.next_capture = Some(captured);
    assert_eq!(control.reclaim_history_online().unwrap().0, 0);
    assert_eq!(
        control.eligible_bytes.continued.as_ref().unwrap().captured,
        refreshed_at
    );
    assert_eq!(
        ports.shared.mutation_gate.source_census().unwrap(),
        gate_before
    );
    drop(pin);

    ports.shared.invalidate_source_continuation();
    assert_eq!(control.reclaim_history_online().unwrap().0, 0);
    assert!(control.eligible_bytes.continued.is_none());
    assert!(control.online_license.is_none());
}

#[test]
fn completed_newer_source_byte_prefix_survives_an_older_qualified_mutation_proof() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    for ordinal in 0..300 {
        append(&ports, if ordinal % 2 == 0 { 17 } else { 31 });
    }
    let (older_source, older_history, epoch) = validated(&ports);
    let ceiling = older_history.tail().sequence().get();
    let now = Instant::now();
    let mut prior = ContinuedProbe::capture(
        None,
        older_source.clone(),
        older_history,
        ceiling,
        epoch,
        now,
    )
    .unwrap();
    let expected = finish(&mut prior, now)[5];
    append(&ports, 47);
    let (newer_source, newer_history, successor_epoch) = validated(&ports);
    assert_eq!(successor_epoch, epoch);
    let newer_publication = newer_source.publication();
    let mut current = ContinuedProbe::capture(
        Some(prior),
        newer_source,
        newer_history,
        ceiling,
        epoch,
        now + CAPTURE_INTERVAL,
    )
    .unwrap();
    let completed = finish(&mut current, now + CAPTURE_INTERVAL);
    assert_eq!(completed[5], expected);
    assert_eq!((completed[11], completed[9]), (0, 0));
    assert!(current.root.is_none());
    let mut observer = Observer {
        continued: Some(current),
        next_capture: Some(now + CAPTURE_INTERVAL * 2),
        ..Observer::default()
    };
    observer
        .share_qualified_pin(older_source, older_history, epoch)
        .unwrap();
    assert!(
        observer.continued.is_some(),
        "a qualified older mutation proof cannot revoke a completed exact prefix bound to a newer actual observation"
    );
    let kept = observer.continued.as_ref().unwrap();
    assert_eq!(kept.publication, newer_publication);
    assert_eq!(kept.captured, now + CAPTURE_INTERVAL);
    assert_eq!(kept.sample(now + CAPTURE_INTERVAL).unwrap(), completed);
    assert!(kept.root.is_none());
    assert_eq!(observer.next_capture, Some(now + CAPTURE_INTERVAL * 2));
    observer.check_qualification(epoch + 1);
    assert!(observer.continued.is_none());
}

#[test]
fn pending_newer_source_byte_capture_finishes_before_an_older_mutation_proof_is_offered() {
    let (_scope, mut ports, _) = crate::changelog_v3_control_tests::fixture();
    Arc::get_mut(&mut ports.shared)
        .unwrap()
        .application_commit_profile = crate::RedbCommitProfile::Standard;
    for ordinal in 0..1030 {
        append(&ports, if ordinal % 2 == 0 { 17 } else { 31 });
    }
    let (older_source, older_history, epoch) = validated(&ports);
    let ceiling = older_history.tail().sequence().get();
    let page = riffdb_storage_api::EvidencePageLimit::new(256).unwrap();
    let mut calibration = RetainedHistoryValidation::new_source(older_source.clone())
        .unwrap()
        .unwrap();
    calibration.bind_qualification(epoch);
    let mut pages = 0;
    loop {
        pages += 1;
        assert!(pages <= 32, "the bounded fixture must finish");
        if calibration.advance(page).unwrap() {
            break;
        }
    }
    assert!(pages > 1);
    drop(calibration);
    let mut walk = RetainedHistoryValidation::new_source(older_source.clone())
        .unwrap()
        .unwrap();
    walk.bind_qualification(epoch);
    for _ in 1..pages {
        assert!(!walk.advance(page).unwrap());
    }
    drop(older_source);
    append(&ports, 47);
    let (newer_source, newer_history, successor_epoch) = validated(&ports);
    assert_eq!(successor_epoch, epoch);
    assert!(newer_history.tail().sequence() > older_history.tail().sequence());
    let expected = oracle(&newer_source, newer_history, ceiling);
    let captured = Instant::now();
    let mut control = ports.replication_source_control();
    control.online_validation = Some(walk);
    control
        .eligible_bytes
        .capture_qualified(newer_source, newer_history, ceiling, epoch, captured)
        .unwrap();
    let cadence = control.eligible_bytes.next_capture;
    let gate_before = ports.shared.mutation_gate.source_census().unwrap();
    assert_eq!(control.reclaim_history_online().unwrap().0, 0);
    let pending = control
        .eligible_bytes
        .continued
        .as_ref()
        .expect("an older mutation proof must not discard unfinished newer accounting");
    assert!(pending.pending());
    assert_eq!(pending.captured, captured);
    assert_eq!(pending.checked_rows, 256);
    assert_eq!(control.eligible_bytes.next_capture, cadence);
    assert_eq!(
        ports.shared.mutation_gate.source_census().unwrap(),
        gate_before
    );
    assert!(control.online_validation.as_ref().unwrap().is_complete());
    for _ in 0..8 {
        control.advance_source_byte_observation_off_gate().unwrap();
    }
    let completed = control.eligible_bytes.continued.as_ref().unwrap();
    let sample = completed.sample(Instant::now()).unwrap();
    assert_eq!(sample[5], expected);
    assert_eq!(sample[11], sample[4]);
    assert_eq!(sample[10], 0);
    assert!(completed.root.is_none());
    assert_eq!(control.eligible_bytes.next_capture, cadence);
    // Accounting grants no mutation authority: the completed source proof
    // still needs its ordinary current validation and first observation.
    for _ in 0..8 {
        assert_eq!(control.reclaim_history_online().unwrap().0, 0);
        if control.reclamation_yields()[0] == 1 {
            break;
        }
    }
    assert!(control.online_validation.as_ref().unwrap().is_complete());
    assert_eq!(control.reclamation_yields()[0], 1);
}

#[test]
fn source_accounting_can_observe_a_checkpointed_prefix_before_reclamation_proof_finishes() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture_with_commit_profile(
        crate::RedbCommitProfile::Hardened,
    );
    for ordinal in 0..1030 {
        let _lease = ports.shared.mutation_gate.acquire().unwrap();
        append(&ports, if ordinal % 2 == 0 { 17 } else { 31 });
    }
    let lane = ports.online_retention_lane();
    let owner = ports.shared_source_control().unwrap();
    let qualification = ports
        .shared
        .source_continuation_epoch
        .load(Ordering::Acquire);
    let source = ports.shared.capture_source_validation_pin().unwrap();
    let history = source.history().unwrap().unwrap();
    assert!(
        history.tail().sequence().get() - history.minimum_resume().sequence().get() >= 1030,
        "fixture must expose its actual populated immutable source pin"
    );
    let ceiling = history.tail().sequence().get();
    let expected = oracle(&source, history, ceiling);
    let mut walk = RetainedHistoryValidation::new_source(source)
        .unwrap()
        .unwrap();
    walk.bind_qualification(qualification);
    owner.lock().unwrap().online_validation = Some(walk);
    lane.advance_source_validation_off_gate().unwrap();
    {
        let _lease = ports.shared.mutation_gate.acquire().unwrap();
        append(&ports, 47);
    }
    assert_eq!(
        ports
            .shared
            .source_continuation_epoch
            .load(Ordering::Acquire),
        qualification
    );
    let gate = ports.shared.mutation_gate.source_census().unwrap();
    lane.advance_source_validation_off_gate().unwrap();
    let mut control = owner.lock().unwrap();
    assert!(control.receipt_validation_pending());
    assert!(control.online_license.is_none());
    assert_eq!(ports.shared.mutation_gate.source_census().unwrap(), gate);
    assert!(
        control.eligible_bytes.pending(),
        "read-only accounting must capture actual independently checkpointed coverage without waiting for the complete deletion proof"
    );
    let rows_before = control.online_validation.as_ref().unwrap().rows_read();
    for _ in 0..16 {
        control.advance_source_byte_observation_off_gate().unwrap();
        if !control.eligible_bytes.pending() {
            break;
        }
    }
    let sample = control
        .eligible_bytes
        .continued
        .as_ref()
        .unwrap()
        .sample(Instant::now())
        .unwrap();
    assert_eq!((sample[3], sample[5]), (ceiling, expected));
    assert_eq!(
        sample[4],
        ceiling - history.minimum_resume().sequence().get()
    );
    assert_eq!(sample[11], sample[4]);
    assert!(control.receipt_validation_pending());
    assert_eq!(
        control.online_validation.as_ref().unwrap().rows_read(),
        rows_before
    );
    assert!(control.observed.is_none());
    assert!(control.online_license.is_none());
    assert_eq!(control.online_independent_sequence, 0);
    assert_eq!(ports.shared.mutation_gate.source_census().unwrap(), gate);
}

#[test]
fn source_accounting_preserves_source_rest_and_two_validation_pins() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture_with_commit_profile(
        crate::RedbCommitProfile::Hardened,
    );
    for ordinal in 0..1030 {
        let _lease = ports.shared.mutation_gate.acquire().unwrap();
        append(&ports, if ordinal % 2 == 0 { 17 } else { 31 });
    }
    let lane = ports.online_retention_lane();
    let owner = ports.shared_source_control().unwrap();
    let qualification = ports
        .shared
        .source_continuation_epoch
        .load(Ordering::Acquire);
    let source = ports.shared.capture_source_validation_pin().unwrap();
    let history = source.history().unwrap().unwrap();
    let ceiling = history.tail().sequence().get();
    assert!(ceiling - history.minimum_resume().sequence().get() >= 1030);
    let expected = oracle(&source, history, ceiling);
    let proof_pin = Arc::downgrade(source.checkpoint_pin());
    let mut walk = RetainedHistoryValidation::new_source(source)
        .unwrap()
        .unwrap();
    walk.bind_qualification(qualification);
    let rest_clock = Instant::now();
    let rest_before;
    {
        let mut control = owner.lock().unwrap();
        control.online_validation = Some(walk);
        control.bind_source_scheduler();
        control.charge_source_work(Instant::now().checked_sub(Duration::from_secs(1)).unwrap());
        rest_before = control.source_pacing.as_ref().unwrap().wait(rest_clock);
        assert!(!rest_before.is_zero());
    }
    let gate = ports.shared.mutation_gate.source_census().unwrap();
    let phases = lane.advance_source_validation_off_gate().unwrap();
    assert_eq!(
        (
            phases.source_initial_rows,
            phases.source_continuation_rows,
            phases.gate_held_us()
        ),
        (0, 0, 0)
    );
    {
        let _lease = ports.shared.mutation_gate.acquire().unwrap();
        append(&ports, 47);
    }
    assert_eq!(
        ports
            .shared
            .source_continuation_epoch
            .load(Ordering::Acquire),
        qualification
    );
    let phases = lane.advance_source_validation_off_gate().unwrap();
    assert_eq!(
        (
            phases.source_initial_rows,
            phases.source_continuation_rows,
            phases.gate_held_us()
        ),
        (0, 0, 0)
    );
    let census_pin = {
        let control = owner.lock().unwrap();
        assert!(
            control.eligible_bytes.pending(),
            "accounting capture must remain available during the actual unchanged source-proof rest"
        );
        let root = control
            .eligible_bytes
            .continued
            .as_ref()
            .unwrap()
            .root
            .as_ref()
            .unwrap();
        let pin = Arc::downgrade(root.checkpoint_pin());
        assert!(
            !pin.ptr_eq(&proof_pin),
            "the two actual independently published roots must be distinct"
        );
        assert_eq!(
            control.online_validation.as_ref().unwrap().rows_read(),
            [0; 2]
        );
        assert!(
            ports
                .shared
                .extent_roots
                .lock()
                .unwrap()
                .iter()
                .filter(|root| root.strong_count() != 0)
                .count()
                <= 2,
            "capture must retain at most the two actual validation roots"
        );
        pin
    };
    for _ in 0..16 {
        let phases = lane.advance_captured_observations_off_gate().unwrap();
        assert_eq!(
            (
                phases.source_initial_rows,
                phases.source_continuation_rows,
                phases.gate_held_us()
            ),
            (0, 0, 0)
        );
        if !owner.lock().unwrap().eligible_bytes.pending() {
            break;
        }
    }
    let mut control = owner.lock().unwrap();
    let completed = control.eligible_bytes.continued.as_ref().unwrap();
    assert!(
        completed.root.is_none(),
        "a completed census releases its validation pin"
    );
    let sample = completed.sample(Instant::now()).unwrap();
    assert_eq!((sample[3], sample[5]), (ceiling, expected));
    assert_eq!(
        control.online_validation.as_ref().unwrap().rows_read(),
        [0; 2]
    );
    assert!(control.observed.is_none());
    assert!(control.online_license.is_none());
    assert_eq!(control.online_independent_sequence, 0);
    assert_eq!(
        control.source_pacing.as_ref().unwrap().wait(rest_clock),
        rest_before
    );
    assert_eq!(ports.shared.mutation_gate.source_census().unwrap(), gate);
    assert!(proof_pin.upgrade().is_some());
    control.retire_online_validation();
    assert!(
        proof_pin.upgrade().is_none(),
        "retirement releases the original unfinished proof pin"
    );
    assert!(
        census_pin.upgrade().is_none(),
        "completed accounting and owner retirement release the census reader"
    );
}
