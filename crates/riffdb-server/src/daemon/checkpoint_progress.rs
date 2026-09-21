//! Bounded live copies of checkpoint counters, independent of graceful close.

use std::time::Duration;

use tokio::time::{Instant, Interval, MissedTickBehavior};

const MAX_SAMPLES: u32 = 600;
const PERIOD: Duration = Duration::from_secs(1);

pub(super) struct CheckpointProgress {
    enabled: bool,
    samples: u32,
    started: Instant,
    interval: Interval,
}

impl CheckpointProgress {
    pub(super) fn new(enabled: bool) -> Self {
        let mut interval = tokio::time::interval(PERIOD);
        // A busy supervisor must not emit a burst to catch up with old ticks.
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        Self {
            enabled,
            samples: 0,
            started: Instant::now(),
            interval,
        }
    }

    pub(super) fn enabled(&self) -> bool {
        self.enabled && self.samples < MAX_SAMPLES
    }

    pub(super) async fn next_line(&mut self, snapshot: impl FnOnce() -> [u64; 15]) -> String {
        self.interval.tick().await;
        self.samples += 1;
        let counts = snapshot().map(|value| value.to_string()).join(",");
        format!(
            "riffdb-checkpoint-progress-v1\tpid={}\tsample={}\tlimit={}\telapsed_ms={}\tcounts={counts}",
            std::process::id(),
            self.samples,
            MAX_SAMPLES,
            self.started.elapsed().as_millis(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::Poll;

    // req: REP-007
    #[tokio::test(start_paused = true)]
    async fn live_checkpoint_evidence_precedes_close_and_has_a_fixed_budget() {
        let mut progress = CheckpointProgress::new(true);
        let mut counts = [0_u64; 15];
        counts[0] = 1;
        counts[1] = 3_050;
        counts[9] = 1;
        let first = progress.next_line(|| counts).await;
        assert!(first.contains("\tsample=1\tlimit=600\telapsed_ms=0\t"));
        assert!(first.ends_with("counts=1,3050,0,0,0,0,0,0,0,1,0,0,0,0,0"));
        // No process close or final shutdown census is needed for the row.
        counts[12] = 1;
        let second = progress.next_line(|| counts).await;
        assert!(second.contains("\tsample=2\tlimit=600\telapsed_ms=1000\t"));
        assert!(second.ends_with("counts=1,3050,0,0,0,0,0,0,0,1,0,0,1,0,0"));
        for _ in 2..MAX_SAMPLES {
            assert!(progress.enabled());
            progress.next_line(|| counts).await;
        }
        assert!(!progress.enabled());
        assert!(!CheckpointProgress::new(false).enabled());
    }

    // req: REP-007
    #[tokio::test(start_paused = true)]
    async fn delayed_supervisor_skips_missed_samples_instead_of_bursting() {
        let mut progress = CheckpointProgress::new(true);
        progress.next_line(|| [0; 15]).await;
        tokio::time::advance(Duration::from_secs(10)).await;
        let late = progress.next_line(|| [0; 15]).await;
        assert!(late.contains("\tsample=2\tlimit=600\telapsed_ms=10000\t"));
        let next = progress.next_line(|| [0; 15]);
        tokio::pin!(next);
        std::future::poll_fn(|cx| {
            assert!(matches!(next.as_mut().poll(cx), Poll::Pending));
            Poll::Ready(())
        })
        .await;
        tokio::time::advance(PERIOD).await;
        assert!(next.await.contains("\telapsed_ms=11000\t"));
    }
}
