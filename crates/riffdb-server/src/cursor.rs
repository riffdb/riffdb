//! Independent process-local cursor entropy and monotonic time providers.

// These providers are consumed by the WP-130 production graph assembled in this crate.
#![allow(dead_code)]

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use riffdb_service::{
    CURSOR_TOKEN_BYTES, CursorClockError, CursorMonotonicClock, CursorTick,
    CursorTokenGenerationError, CursorTokenGenerator,
};

trait CursorEntropy: Send + Sync {
    fn fill(&self, destination: &mut [u8; CURSOR_TOKEN_BYTES]) -> Result<(), CursorEntropyError>;
}

struct OperatingSystemCursorEntropy;

impl CursorEntropy for OperatingSystemCursorEntropy {
    fn fill(&self, destination: &mut [u8; CURSOR_TOKEN_BYTES]) -> Result<(), CursorEntropyError> {
        getrandom::fill(destination).map_err(|_| CursorEntropyError)
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct CursorEntropyError;

impl fmt::Debug for CursorEntropyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CursorEntropyError([REDACTED])")
    }
}

/// Independent entropy provider for opaque cursor tokens.
pub(crate) struct ProductionCursorTokenGenerator {
    entropy: Arc<dyn CursorEntropy>,
}

impl ProductionCursorTokenGenerator {
    pub(crate) fn new() -> Self {
        Self {
            entropy: Arc::new(OperatingSystemCursorEntropy),
        }
    }
}

impl Default for ProductionCursorTokenGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl CursorTokenGenerator for ProductionCursorTokenGenerator {
    fn fill_cursor_token(
        &self,
        destination: &mut [u8; CURSOR_TOKEN_BYTES],
    ) -> Result<(), CursorTokenGenerationError> {
        if self.entropy.fill(destination).is_err() {
            destination.fill(0);
            return Err(CursorTokenGenerationError);
        }
        Ok(())
    }
}

impl fmt::Debug for ProductionCursorTokenGenerator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProductionCursorTokenGenerator([REDACTED])")
    }
}

trait MonotonicInstantSource: Send + Sync {
    fn now(&self) -> Instant;
}

struct OperatingSystemMonotonicClock;

impl MonotonicInstantSource for OperatingSystemMonotonicClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Process-relative cursor clock with one private origin per server construction.
pub(crate) struct ProductionCursorMonotonicClock {
    origin: Instant,
    source: Arc<dyn MonotonicInstantSource>,
}

impl ProductionCursorMonotonicClock {
    pub(crate) fn new() -> Self {
        let source: Arc<dyn MonotonicInstantSource> = Arc::new(OperatingSystemMonotonicClock);
        let origin = source.now();
        Self { origin, source }
    }

    fn tick_from_elapsed(elapsed: Duration) -> Result<CursorTick, CursorClockError> {
        CursorTick::from_process_elapsed(elapsed).map_err(|_| CursorClockError)
    }
}

impl Default for ProductionCursorMonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

impl CursorMonotonicClock for ProductionCursorMonotonicClock {
    fn now(&self) -> Result<CursorTick, CursorClockError> {
        let elapsed = self
            .source
            .now()
            .checked_duration_since(self.origin)
            .ok_or(CursorClockError)?;
        Self::tick_from_elapsed(elapsed)
    }
}

impl fmt::Debug for ProductionCursorMonotonicClock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProductionCursorMonotonicClock([REDACTED])")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    struct RecordingEntropy {
        result: Result<[u8; CURSOR_TOKEN_BYTES], CursorEntropyError>,
        calls: Mutex<u8>,
    }

    impl CursorEntropy for RecordingEntropy {
        fn fill(
            &self,
            destination: &mut [u8; CURSOR_TOKEN_BYTES],
        ) -> Result<(), CursorEntropyError> {
            *self.calls.lock().expect("entropy calls") += 1;
            *destination = self.result?;
            Ok(())
        }
    }

    struct FixedInstant(Instant);

    impl MonotonicInstantSource for FixedInstant {
        fn now(&self) -> Instant {
            self.0
        }
    }

    #[test]
    fn token_generation_uses_one_independent_complete_fill() {
        let entropy = Arc::new(RecordingEntropy {
            result: Ok([0xa5; CURSOR_TOKEN_BYTES]),
            calls: Mutex::new(0),
        });
        let generator = ProductionCursorTokenGenerator {
            entropy: entropy.clone(),
        };
        let mut destination = [0; CURSOR_TOKEN_BYTES];

        assert_eq!(generator.fill_cursor_token(&mut destination), Ok(()));
        assert_eq!(destination, [0xa5; CURSOR_TOKEN_BYTES]);
        assert_eq!(*entropy.calls.lock().expect("entropy calls"), 1);
    }

    #[test]
    fn token_entropy_failure_is_redacted_and_exposes_no_partial_candidate() {
        let entropy = Arc::new(RecordingEntropy {
            result: Err(CursorEntropyError),
            calls: Mutex::new(0),
        });
        let generator = ProductionCursorTokenGenerator {
            entropy: entropy.clone(),
        };
        let mut destination = [0x7f; CURSOR_TOKEN_BYTES];

        assert_eq!(
            generator.fill_cursor_token(&mut destination),
            Err(CursorTokenGenerationError)
        );
        assert_eq!(destination, [0; CURSOR_TOKEN_BYTES]);
        assert_eq!(*entropy.calls.lock().expect("entropy calls"), 1);
    }

    #[test]
    fn monotonic_clock_uses_its_private_origin_without_sleeping() {
        let origin = Instant::now();
        let first = ProductionCursorMonotonicClock {
            origin,
            source: Arc::new(FixedInstant(origin + Duration::from_nanos(17))),
        };
        let restarted_origin = origin + Duration::from_nanos(7);
        let restarted = ProductionCursorMonotonicClock {
            origin: restarted_origin,
            source: Arc::new(FixedInstant(origin + Duration::from_nanos(17))),
        };

        assert_ne!(
            first.now().expect("first tick"),
            restarted.now().expect("restart tick")
        );
    }

    #[test]
    fn monotonic_regression_and_tick_overflow_fail_closed() {
        let origin = Instant::now();
        let regressed = ProductionCursorMonotonicClock {
            origin,
            source: Arc::new(FixedInstant(
                origin
                    .checked_sub(Duration::from_nanos(1))
                    .expect("representable earlier instant"),
            )),
        };
        assert_eq!(regressed.now(), Err(CursorClockError));
        assert_eq!(
            ProductionCursorMonotonicClock::tick_from_elapsed(Duration::from_secs(u64::MAX)),
            Err(CursorClockError)
        );
    }

    #[test]
    fn debug_output_redacts_cursor_provider_state() {
        assert_eq!(
            format!("{:?}", ProductionCursorTokenGenerator::new()),
            "ProductionCursorTokenGenerator([REDACTED])"
        );
        assert_eq!(
            format!("{:?}", ProductionCursorMonotonicClock::new()),
            "ProductionCursorMonotonicClock([REDACTED])"
        );
        assert_eq!(
            format!("{:?}", CursorEntropyError),
            "CursorEntropyError([REDACTED])"
        );
    }
}
