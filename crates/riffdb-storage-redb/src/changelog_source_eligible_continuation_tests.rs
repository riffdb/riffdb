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
    for _ in 0..2 {
        let phases = lane.advance_captured_observations_off_gate().unwrap();
        assert_eq!(phases.source_initial_rows, 0);
        assert_eq!(phases.source_continuation_rows, 0);
        assert_eq!(phases.reclaimed_receipts, 0);
        assert_eq!(phases.gate_held_us(), 0);
    }
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
