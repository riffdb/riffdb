//! Shared cursor-clock admission for all exact-provider registries.

use super::*;
use riffdb_service::CursorClockError;

struct FixedClock(Result<CursorTick, CursorClockError>);

impl CursorMonotonicClock for FixedClock {
    fn now(&self) -> Result<CursorTick, CursorClockError> {
        self.0
    }
}

fn clock(seconds: u64) -> FixedClock {
    FixedClock(Ok(CursorTick::from_process_elapsed(Duration::from_secs(
        seconds,
    ))
    .unwrap()))
}

// req: OQ-040
#[test]
fn exact_provider_clock_refuses_regression_across_families_without_shortening_retention() {
    let mut registry = ExactProviderRegistries::default();
    assert_eq!(
        registry.observe_access(&clock(10)),
        Ok(clock(10).0.unwrap())
    );
    assert_eq!(
        registry.observe_access(&clock(11)),
        Ok(clock(11).0.unwrap())
    );
    assert_eq!(
        registry.observe_access(&clock(9)),
        Err(ExactTextProjectionPortError::Unavailable)
    );
    assert_eq!(registry.observed_clock, Some(clock(11).0.unwrap()));
    assert_eq!(
        registry.observe_access(&clock(11)),
        Ok(clock(11).0.unwrap())
    );
}

// req: OQ-040
#[test]
fn exact_provider_clock_refuses_failure_and_expiry_overflow_before_admission() {
    let mut registry = ExactProviderRegistries::default();
    registry.observe_access(&clock(1)).unwrap();
    let unavailable = FixedClock(Err(CursorClockError));
    assert_eq!(
        registry.observe_access(&unavailable),
        Err(ExactTextProjectionPortError::Unavailable)
    );
    let overflow = FixedClock(Ok(CursorTick::from_process_elapsed(Duration::from_nanos(
        u64::MAX,
    ))
    .unwrap()));
    assert_eq!(
        registry.observe_access(&overflow),
        Err(ExactTextProjectionPortError::Unavailable)
    );
    assert_eq!(registry.observed_clock, Some(clock(1).0.unwrap()));
    assert_eq!(registry.observe_access(&clock(2)), Ok(clock(2).0.unwrap()));
}

// req: OQ-040
#[test]
fn primary_and_follower_providers_share_the_service_cursor_clock_owner() {
    for source in [
        include_str!("process_graph.rs"),
        include_str!("follower_service.rs"),
    ] {
        assert_eq!(
            source
                .matches("ProductionCursorMonotonicClock::new()")
                .count(),
            1
        );
        let exact = source.find("ExactTextRuntime::open(").unwrap();
        assert!(source[..exact].contains("let cursor_clock: Arc<dyn CursorMonotonicClock>"));
        let call_end = source[exact..].find("\n        )").unwrap() + exact;
        assert!(source[exact..call_end].contains("Arc::clone(&cursor_clock)"));
        assert!(source[call_end..].contains("cursor_clock,"));
    }
}
