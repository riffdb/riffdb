//! A held receipt must be visible to the instrument before its ready recheck.

use super::*;

fn fence() -> JournalFence {
    JournalFence {
        covered_sequence: CommitSequence::new(1),
        covered_administration_sequence: None,
        frame_hash: [0; HASH_BYTES],
        durable_at: Instant::now(),
    }
}

// req: REP-007
#[test]
fn receipt_wait_census_retains_first_wait_when_publication_recheck_is_ready() {
    let completion = JournalFenceCompletion {
        command_bearing: true,
        ..JournalFenceCompletion::default()
    };
    let census = CommandReceiptWaitCensus::default();
    let base = Instant::now();
    let elapsed_us = AtomicU64::new(0);
    let now = || base + Duration::from_micros(elapsed_us.load(Ordering::SeqCst));
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let expected = fence();
    assert_eq!(completion.try_wait(), Ok(None));
    thread::scope(|scope| {
        let waiter = scope.spawn(|| {
            completion.wait_observed(&census, now, || {
                let _ = entered_tx.try_send(());
            })
        });
        entered_rx.recv().expect("initial receipt wait is pending");
        assert_eq!(census.snapshot(), [0; 4]);
        elapsed_us.store(25_000, Ordering::SeqCst);
        completion.complete(Ok(expected));
        assert_eq!(waiter.join().expect("waiter"), Ok(expected));
    });
    assert_eq!(census.snapshot(), [1, 0, 25_000, 25_000]);
    // Publication waits on the same ready receipt; its zero elapsed time must
    // not replace, erase, or duplicate the first wait's 25 ms.
    assert_eq!(completion.wait_observed(&census, now, || {}), Ok(expected));
    assert_eq!(completion.try_wait(), Ok(Some(expected)));
    assert_eq!(census.snapshot(), [2, 0, 25_000, 25_000]);
}

// req: REP-007
#[test]
fn receipt_wait_census_retains_failed_waits_and_excludes_noncommand_receipts() {
    for command_bearing in [true, false] {
        let completion = JournalFenceCompletion {
            command_bearing,
            ..JournalFenceCompletion::default()
        };
        let census = CommandReceiptWaitCensus::default();
        let base = Instant::now();
        let ticks = AtomicU64::new(0);
        let now = || base + Duration::from_micros(ticks.fetch_add(7, Ordering::SeqCst));
        completion.complete(Err(JournalIoError::Io));
        assert_eq!(
            completion.wait_observed(&census, now, || {}),
            Err(JournalIoError::Io)
        );
        assert_eq!(
            census.snapshot(),
            if command_bearing {
                [1, 1, 7, 7]
            } else {
                [0; 4]
            }
        );
    }
}
