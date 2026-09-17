//! Deterministic cancellation, draining and bounded scheduling proofs.
// req: REP-006
use super::*;
use std::cell::Cell;

#[tokio::test(start_paused = true)]
async fn policy_worker_bounds_each_pass_and_stops_before_new_work() {
    let stop = Cancellation::new();
    let observations = Cell::new(0);
    let submissions = Cell::new(0);
    let mut run = Box::pin(pump(
        || {
            observations.set(observations.get() + 1);
            Ok(true)
        },
        || async {
            submissions.set(submissions.get() + 1);
            Ok(Step::Complete)
        },
        &stop,
    ));
    assert!(futures_util::poll!(&mut run).is_pending());
    assert_eq!(submissions.get(), MAX_STEPS_PER_PASS);
    tokio::time::advance(RETRY_DELAY).await;
    assert!(futures_util::poll!(&mut run).is_pending());
    assert_eq!(submissions.get(), MAX_STEPS_PER_PASS * 2);
    stop.stop();
    run.await.unwrap();
    assert_eq!(observations.get(), MAX_STEPS_PER_PASS * 2);
}

#[tokio::test(start_paused = true)]
async fn policy_worker_does_not_submit_on_idle_or_unreadable_observations() {
    for failure in [false, true] {
        let stop = Cancellation::new();
        let mut run = Box::pin(pump(
            || if failure { Err(()) } else { Ok(false) },
            || async { panic!("no work may be submitted") },
            &stop,
        ));
        if failure {
            assert!(run.await.is_err());
        } else {
            assert!(futures_util::poll!(&mut run).is_pending());
            stop.stop();
            run.await.unwrap();
        }
    }
}

#[tokio::test]
async fn policy_worker_cancels_capacity_wait_but_drains_accepted_work() {
    let stop = Cancellation::new();
    let mut waiting = Box::pin(unless_cancelled(std::future::pending::<()>(), &stop));
    assert!(futures_util::poll!(&mut waiting).is_pending());
    stop.stop();
    assert_eq!(waiting.await, None);
    assert_eq!(unless_cancelled(std::future::ready(()), &stop).await, None);

    let stop = Cancellation::new();
    let accepted = Cell::new(false);
    let completed = Cell::new(false);
    let release = Notify::new();
    let mut run = Box::pin(pump(
        || Ok(true),
        || async {
            assert!(!accepted.replace(true), "one accepted continuation");
            release.notified().await;
            completed.set(true);
            Ok(Step::Complete)
        },
        &stop,
    ));
    assert!(futures_util::poll!(&mut run).is_pending());
    assert!(accepted.get());
    stop.stop();
    assert!(futures_util::poll!(&mut run).is_pending());
    assert!(!completed.get());
    release.notify_one();
    run.await.unwrap();
    assert!(completed.get());
}

#[tokio::test(start_paused = true)]
async fn policy_worker_retries_proven_unavailable_work_after_a_bounded_backoff() {
    let stop = Cancellation::new();
    let attempts = Cell::new(0);
    let mut run = Box::pin(pump(
        || Ok(true),
        || async {
            attempts.set(attempts.get() + 1);
            Ok(Step::RetryLater)
        },
        &stop,
    ));
    assert!(futures_util::poll!(&mut run).is_pending());
    assert_eq!(attempts.get(), 1);
    tokio::time::advance(RETRY_DELAY).await;
    assert!(futures_util::poll!(&mut run).is_pending());
    assert_eq!(attempts.get(), 2);
    stop.stop();
    run.await.unwrap();
}
