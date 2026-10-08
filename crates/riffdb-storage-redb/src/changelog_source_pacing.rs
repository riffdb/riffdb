//! One scalar rest deadline for the graph's shared native source-proof owner.
//! Scheduling time grants no source, checkpoint or acknowledgement authority.
use std::time::{Duration, Instant};

const REST_FACTOR: u32 = 19;
const MAX_REST: Duration = Duration::from_secs(19);

#[derive(Default)]
pub(super) struct SourcePacing {
    not_before: Option<Instant>,
    overflowed: bool,
}
impl SourcePacing {
    pub(super) fn wait(&self, now: Instant) -> Duration {
        if self.overflowed {
            return MAX_REST;
        }
        self.not_before.map_or(Duration::ZERO, |deadline| {
            deadline.saturating_duration_since(now)
        })
    }

    /// Charge actual native background and writer work once, using the existing
    /// factor and maximum rest. Other source users honor this same deadline.
    pub(super) fn charge(&mut self, now: Instant, work: Duration) {
        let rest = self
            .wait(now)
            .saturating_add(work.saturating_mul(REST_FACTOR))
            .min(MAX_REST);
        self.not_before = now.checked_add(rest);
        self.overflowed |= self.not_before.is_none();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // req: REP-003, REC-001, END-008
    #[test]
    fn source_pacing_charges_shared_work_and_preserves_original_bounded_rest() {
        let start = Instant::now();
        let mut pacing = SourcePacing::default();
        assert!(pacing.wait(start).is_zero());
        pacing.charge(start, Duration::from_millis(2));
        assert_eq!(pacing.wait(start), Duration::from_millis(38));
        let later = start + Duration::from_millis(10);
        pacing.charge(later, Duration::from_millis(1));
        assert_eq!(pacing.wait(later), Duration::from_millis(47));
        assert_eq!(
            pacing.wait(start + Duration::from_millis(57)),
            Duration::ZERO
        );
        pacing.charge(later, Duration::from_secs(100));
        assert_eq!(pacing.wait(later), MAX_REST);
        assert!(pacing.wait(later + MAX_REST).is_zero());
    }
}
