// req: REP-005, REC-001
use super::*;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

#[derive(Default)]
struct WakeCount(AtomicUsize);
impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn poll<T>(future: Pin<&mut impl Future<Output = T>>, wake: &Arc<WakeCount>) -> Poll<T> {
    future.poll(&mut Context::from_waker(&Waker::from(Arc::clone(wake))))
}

#[test]
fn pause_drains_admitted_submissions_and_never_claims_a_durable_fence() {
    let gate = Arc::new(PrimaryAdmissionGate::new());
    let first = gate.begin().unwrap();
    let second = gate.begin().unwrap();
    let pause = gate.pause().unwrap();
    assert!(matches!(
        gate.begin(),
        Err(PrimaryAdmissionRefusal::Draining)
    ));
    assert!(matches!(
        gate.pause(),
        Err(PrimaryAdmissionRefusal::Draining)
    ));
    let wake = Arc::new(WakeCount::default());
    let mut drain = Box::pin(pause.drain());
    assert!(poll(drain.as_mut(), &wake).is_pending());
    drop(first);
    assert_eq!(wake.0.load(Ordering::SeqCst), 0);
    assert!(poll(drain.as_mut(), &wake).is_pending());
    drop(second);
    assert!(wake.0.load(Ordering::SeqCst) > 0);
    let Poll::Ready(drained) = poll(drain.as_mut(), &wake) else {
        panic!("all admitted sends completed");
    };
    assert!(matches!(
        gate.begin(),
        Err(PrimaryAdmissionRefusal::Draining)
    ));
    drained.finish_fenced();
    assert!(matches!(gate.begin(), Err(PrimaryAdmissionRefusal::Fenced)));
    assert!(matches!(gate.pause(), Err(PrimaryAdmissionRefusal::Fenced)));
}

#[test]
fn cancelling_before_submission_releases_pause_with_or_without_pending_senders() {
    for point in 0..3 {
        let gate = Arc::new(PrimaryAdmissionGate::new());
        let sender = gate.begin().unwrap();
        let pause = gate.pause().unwrap();
        match point {
            0 => {
                drop(pause);
                drop(sender);
            }
            1 => {
                let mut drain = Box::pin(pause.drain());
                assert!(poll(drain.as_mut(), &Arc::default()).is_pending());
                drop(drain);
                drop(gate.begin().unwrap());
                drop(sender);
            }
            2 => {
                drop(sender);
                let mut drain = Box::pin(pause.drain());
                let Poll::Ready(drained) = poll(drain.as_mut(), &Arc::default()) else {
                    panic!("drained");
                };
                drop(drained);
            }
            _ => unreachable!(),
        }
        drop(gate.begin().unwrap());
        drop(gate.pause().unwrap());
        drop(gate.begin().unwrap());
    }
}

#[test]
fn every_drain_registration_order_observes_the_final_submission() {
    // Explicit scheduling on both sides of notifier registration, including
    // cancellation and a second pause, without sleeps or a scheduler assumption.
    for release_before_poll in [false, true] {
        let gate = Arc::new(PrimaryAdmissionGate::new());
        let sender = gate.begin().unwrap();
        let pause = gate.pause().unwrap();
        let wake = Arc::new(WakeCount::default());
        let mut drain = Box::pin(pause.drain());
        if !release_before_poll {
            assert!(poll(drain.as_mut(), &wake).is_pending());
        }
        drop(sender);
        let Poll::Ready(drained) = poll(drain.as_mut(), &wake) else {
            panic!("no lost final wake");
        };
        drop(drained);
        let sender = gate.begin().unwrap();
        let mut next = Box::pin(gate.pause().unwrap().drain());
        assert!(poll(next.as_mut(), &wake).is_pending());
        drop(sender);
        let Poll::Ready(drained) = poll(next.as_mut(), &wake) else {
            panic!("second pause drains");
        };
        drained.finish_fenced();
        assert!(matches!(gate.begin(), Err(PrimaryAdmissionRefusal::Fenced)));
    }
}

#[test]
fn admitted_send_reaches_the_queue_before_the_fence_barrier() {
    for sends in [1, 2, 8] {
        let gate = Arc::new(PrimaryAdmissionGate::new());
        let (tx, mut rx) = tokio::sync::mpsc::channel(sends + 1);
        let mut workers = Vec::new();
        for item in 0..sends {
            let permit = tx.clone().try_reserve_owned().unwrap();
            let admitted = gate.begin().unwrap();
            let (release, released) = std::sync::mpsc::sync_channel(0);
            let worker = std::thread::spawn(move || {
                released.recv().unwrap();
                permit.send(item);
                drop(admitted);
            });
            workers.push((release, worker));
        }
        let barrier = tx.try_reserve_owned().unwrap();
        let mut drain = Box::pin(gate.pause().unwrap().drain());
        let wake = Arc::new(WakeCount::default());
        assert!(poll(drain.as_mut(), &wake).is_pending());
        assert!(matches!(
            gate.begin(),
            Err(PrimaryAdmissionRefusal::Draining)
        ));
        // Reverse release order tests actual sends, not reservation order.
        for (release, worker) in workers.into_iter().rev() {
            release.send(()).unwrap();
            worker.join().unwrap();
        }
        let Poll::Ready(drained) = poll(drain.as_mut(), &wake) else {
            panic!("senders drained");
        };
        barrier.send(sends);
        for expected in (0..sends).rev().chain(std::iter::once(sends)) {
            assert_eq!(rx.try_recv().unwrap(), expected);
        }
        drained.finish_fenced();
        assert!(matches!(gate.begin(), Err(PrimaryAdmissionRefusal::Fenced)));
    }
}

#[test]
fn fence_retry_admission_never_reopens_a_permanent_fence() {
    let gate = Arc::new(PrimaryAdmissionGate::new());
    let mut initial = Box::pin(gate.pause_for_fence().unwrap().drain());
    let Poll::Ready(drained) = poll(initial.as_mut(), &Arc::default()) else {
        panic!("empty gate");
    };
    drained.finish_fenced();
    for stage in 0..3 {
        let retry = gate.pause_for_fence().unwrap();
        let concurrent = gate.pause_for_fence().unwrap();
        assert!(matches!(gate.begin(), Err(PrimaryAdmissionRefusal::Fenced)));
        drop(concurrent);
        if stage == 0 {
            drop(retry);
        } else {
            let mut pending = Box::pin(retry.drain());
            let Poll::Ready(drained) = poll(pending.as_mut(), &Arc::default()) else {
                panic!("fenced source has no admitted senders");
            };
            if stage == 1 {
                drop(drained);
            } else {
                drained.finish_fenced();
            }
        }
        assert!(matches!(gate.begin(), Err(PrimaryAdmissionRefusal::Fenced)));
        assert!(matches!(gate.pause(), Err(PrimaryAdmissionRefusal::Fenced)));
    }
}

#[test]
fn fence_attempt_never_borrows_another_pending_pause() {
    let gate = Arc::new(PrimaryAdmissionGate::new());
    let sender = gate.begin().unwrap();
    let owned = gate.pause_for_fence().unwrap();
    assert!(matches!(
        gate.pause_for_fence(),
        Err(PrimaryAdmissionRefusal::Draining)
    ));
    assert!(matches!(
        gate.begin(),
        Err(PrimaryAdmissionRefusal::Draining)
    ));
    drop(owned);
    drop(sender);
    drop(gate.begin().unwrap());
    let mut pending = Box::pin(gate.pause_for_fence().unwrap().drain());
    let Poll::Ready(drained) = poll(pending.as_mut(), &Arc::default()) else {
        panic!("released prior sender");
    };
    drained.finish_fenced();
    assert!(matches!(gate.begin(), Err(PrimaryAdmissionRefusal::Fenced)));
}
