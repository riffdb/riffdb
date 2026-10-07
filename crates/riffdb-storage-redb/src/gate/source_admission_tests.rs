use super::*;
use std::sync::mpsc;

// req: STO-030, STO-031, STO-032, END-008
#[test]
fn retirement_discards_checkpoint_requests_in_both_held_source_turn_schedules() {
    for requested_before in [true, false] {
        let gate = Arc::new(ExclusiveGate::default());
        let worker_gate = Arc::clone(&gate);
        let (entered, ready) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let source = worker_gate.acquire_source().unwrap().unwrap();
            if requested_before {
                worker_gate.note_source_publication_request().unwrap();
            }
            entered.send(()).unwrap();
            released.recv().unwrap();
            if !requested_before {
                worker_gate.note_source_publication_request().unwrap();
            }
            drop(source);
        });
        ready.recv().unwrap();
        gate.retire_source_admission();
        release.send(()).unwrap();
        worker.join().unwrap();
        assert!(!gate.source_checkpoint_requested().unwrap());
        assert!(!gate.source_publication_requested().unwrap());
        assert!(gate.acquire_source().unwrap().is_none());
        drop(gate.acquire().unwrap());
    }
}

// req: STO-030, STO-031, END-008
#[test]
fn source_gate_bookkeeping_has_one_population_independent_fixed_capacity() {
    assert!(std::mem::size_of::<State>() <= 256);
    assert!(std::mem::size_of::<Request>() <= 128);
}

fn poll(request: &mut Request, now: Instant) -> Poll {
    let inner = Arc::clone(&request.inner);
    let mut state = inner.state.lock().unwrap();
    request.poll_locked(&mut state, now)
}

// req: STO-030, STO-031, STO-032, END-008
#[test]
fn source_turn_is_between_earlier_and_later_actual_writer_tickets() {
    let gate = Arc::new(ExclusiveGate::default());
    let initial = gate.acquire().unwrap();
    let (first_tx, first_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let earlier_gate = Arc::clone(&gate);
    let earlier = std::thread::spawn(move || {
        let lease = earlier_gate.acquire().unwrap();
        first_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        drop(lease);
        done_tx.send(()).unwrap();
    });
    gate.wait_for_queued(1);
    let now = Instant::now();
    let mut request = gate.source_request_at(now).unwrap().unwrap();
    let (later_tx, later_rx) = mpsc::channel();
    let later_gate = Arc::clone(&gate);
    let later = std::thread::spawn(move || {
        let _lease = later_gate.acquire().unwrap();
        later_tx.send(()).unwrap();
    });
    gate.wait_for_queued(3);
    assert!(matches!(poll(&mut request, now), Poll::Pending));
    assert!(gate.try_acquire().unwrap().is_none());
    drop(initial);
    first_rx.recv().unwrap();
    assert!(matches!(poll(&mut request, now), Poll::Pending));
    release_tx.send(()).unwrap();
    done_rx.recv().unwrap();
    let Poll::Finished(Some(source)) = poll(&mut request, now) else {
        panic!("source owns its FIFO turn before the later writer");
    };
    assert!(later_rx.try_recv().is_err());
    assert!(gate.try_acquire().unwrap().is_none());
    drop(source);
    later_rx.recv().unwrap();
    earlier.join().unwrap();
    later.join().unwrap();
    assert_eq!(&gate.source_census().unwrap()[..5], &[1, 1, 0, 0, 0]);
    assert!(!gate.is_held());
}

// req: STO-030, STO-031, END-008
#[test]
fn deadline_keeps_one_cancelled_slot_until_consumed_without_stranding_a_writer() {
    let gate = Arc::new(ExclusiveGate::default());
    let initial = gate.acquire().unwrap();
    let now = Instant::now();
    let mut request = gate.source_request_at(now).unwrap().unwrap();
    assert!(matches!(
        poll(&mut request, now + ADMISSION_WAIT),
        Poll::Finished(None)
    ));
    let issued = gate.tickets_issued();
    assert!(gate.source_request_at(now).unwrap().is_none());
    assert_eq!(gate.tickets_issued(), issued);
    let next_gate = Arc::clone(&gate);
    let writer = std::thread::spawn(move || drop(next_gate.acquire().unwrap()));
    gate.wait_for_queued(2);
    drop(initial);
    writer.join().unwrap();
    assert_eq!(&gate.source_census().unwrap()[..5], &[1, 0, 1, 0, 1]);
    assert!(gate.inner.state.lock().unwrap().source.slot.is_none());
    assert!(!gate.is_held());
    drop(gate.acquire_source().unwrap().unwrap());
}

// req: STO-030, STO-031, STO-032, REC-001
#[test]
fn cancellation_and_grant_linearize_both_ways_and_retirement_never_reopens() {
    for retire_first in [true, false] {
        let gate = ExclusiveGate::default();
        let now = Instant::now();
        let mut request = gate.source_request_at(now).unwrap().unwrap();
        if retire_first {
            gate.retire_source_admission();
            assert!(matches!(poll(&mut request, now), Poll::Finished(None)));
            assert_eq!(&gate.source_census().unwrap()[..4], &[1, 0, 0, 1]);
        } else {
            let Poll::Finished(Some(lease)) = poll(&mut request, now) else {
                panic!("grant wins before retirement");
            };
            gate.retire_source_admission();
            assert!(gate.is_held());
            assert!(gate.try_acquire().unwrap().is_none());
            drop(lease);
            assert_eq!(&gate.source_census().unwrap()[..4], &[1, 1, 0, 0]);
        }
        assert!(gate.acquire_source().unwrap().is_none());
        drop(gate.acquire().unwrap());
        assert!(!gate.is_held());
    }
}

// req: STO-030, STO-031, REC-001
#[test]
fn dropped_request_releases_only_its_fixed_slot_and_allows_a_later_owner() {
    let gate = ExclusiveGate::default();
    let initial = gate.acquire().unwrap();
    let request = gate.source_request_at(Instant::now()).unwrap().unwrap();
    drop(request);
    assert!(
        gate.inner
            .state
            .lock()
            .unwrap()
            .source
            .slot
            .as_ref()
            .unwrap()
            .cancelled
    );
    drop(initial);
    assert!(gate.inner.state.lock().unwrap().source.slot.is_none());
    drop(gate.acquire_source().unwrap().unwrap());
    assert_eq!(&gate.source_census().unwrap()[..4], &[2, 1, 0, 1]);
}

// req: STO-030, STO-031, STO-032, REC-001
#[test]
fn production_waiter_retires_without_acquiring_the_held_writer_or_stranding_its_successor() {
    let gate = Arc::new(ExclusiveGate::default());
    let initial = gate.acquire().unwrap();
    let worker_gate = Arc::clone(&gate);
    let waiter = std::thread::spawn(move || {
        assert!(worker_gate.acquire_source().unwrap().is_none());
    });
    gate.wait_for_queued(1);
    gate.retire_source_admission();
    waiter.join().unwrap();
    assert!(gate.is_held());
    let census = gate.source_census().unwrap();
    assert_eq!(census[0], 1);
    assert_eq!(census[1], 0);
    assert_eq!(census[2] + census[3], 1);
    assert_eq!(census[6], 0, "waiting consumes no source writer residence");
    let next_gate = Arc::clone(&gate);
    let next = std::thread::spawn(move || drop(next_gate.acquire().unwrap()));
    gate.wait_for_queued(2);
    drop(initial);
    next.join().unwrap();
    assert!(!gate.is_held());
}

// req: STO-030, STO-031, END-008
#[test]
fn source_ticket_exhaustion_changes_no_gate_ownership_or_pending_slot() {
    let gate = ExclusiveGate::default();
    {
        let mut state = gate.inner.state.lock().unwrap();
        state.next_ticket = u128::MAX;
        state.serving = u128::MAX;
    }
    assert_eq!(
        gate.acquire_source().err().unwrap().kind(),
        StorageErrorKind::InvariantViolation
    );
    let state = gate.inner.state.lock().unwrap();
    assert!(!state.held);
    assert!(state.source.slot.is_none());
    assert_eq!(state.next_ticket, u128::MAX);
    assert_eq!(state.source.census, [0; 8]);
}

// req: STO-030, STO-031, STO-032, REC-001, END-008
#[test]
fn publication_handoff_survives_checkpoint_consumption_until_drained_capture_or_retirement() {
    let gate = ExclusiveGate::default();
    gate.note_source_publication_request().unwrap();
    assert!(gate.source_checkpoint_requested().unwrap());
    assert!(gate.source_publication_requested().unwrap());
    gate.clear_source_checkpoint_request().unwrap();
    assert!(!gate.source_checkpoint_requested().unwrap());
    assert!(gate.source_publication_requested().unwrap());
    gate.clear_source_publication_request().unwrap();
    assert!(!gate.source_publication_requested().unwrap());
    gate.note_source_publication_request().unwrap();
    gate.retire_source_admission();
    gate.note_source_publication_request().unwrap();
    assert!(!gate.source_checkpoint_requested().unwrap());
    assert!(!gate.source_publication_requested().unwrap());
    drop(gate.acquire().unwrap());
}
