//! Monotonic access ordering and minimum retention, under the registry lock.

use riffdb_service::{CURSOR_LIFETIME, CursorTick};

use riffdb_service::ExactTextProjectionPortError;

const ACCESS_UNAVAILABLE: ExactTextProjectionPortError = ExactTextProjectionPortError::Unavailable;

pub(super) struct ProviderAccess {
    ordinal: u64,
    observed: CursorTick,
    protected_until: CursorTick,
}

impl ProviderAccess {
    pub(super) fn new(
        ordinal: u64,
        observed: CursorTick,
    ) -> Result<Self, ExactTextProjectionPortError> {
        let protected_until = observed
            .checked_add(CURSOR_LIFETIME)
            .ok_or(ACCESS_UNAVAILABLE)?;
        Ok(Self {
            ordinal,
            observed,
            protected_until,
        })
    }

    pub(super) fn touch(
        &mut self,
        ordinal: u64,
        observed: CursorTick,
    ) -> Result<(), ExactTextProjectionPortError> {
        if observed < self.observed {
            return Err(ACCESS_UNAVAILABLE);
        }
        let protected_until = observed
            .checked_add(CURSOR_LIFETIME)
            .ok_or(ACCESS_UNAVAILABLE)?;
        self.ordinal = ordinal;
        self.observed = observed;
        self.protected_until = protected_until;
        Ok(())
    }

    pub(super) fn retention_expired_at(
        &self,
        observed: CursorTick,
    ) -> Result<bool, ExactTextProjectionPortError> {
        if observed < self.observed {
            return Err(ACCESS_UNAVAILABLE);
        }
        Ok(observed >= self.protected_until)
    }

    pub(super) fn ordinal(&self) -> u64 {
        self.ordinal
    }

    /// The registry rebases all active ordinals together, preserving order.
    pub(super) fn rebase(&mut self, ordinal: u64) {
        self.ordinal = ordinal;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn tick(seconds: u64) -> CursorTick {
        CursorTick::from_process_elapsed(Duration::from_secs(seconds)).unwrap()
    }

    // req: OQ-040
    #[test]
    fn pressure_cannot_expire_the_minimum_retention_before_the_cursor_lifetime() {
        let mut access = ProviderAccess::new(1, tick(0)).unwrap();
        assert!(!access.retention_expired_at(tick(299)).unwrap());
        assert!(access.retention_expired_at(tick(300)).unwrap());
        access.touch(2, tick(10)).unwrap();
        assert!(!access.retention_expired_at(tick(300)).unwrap());
        assert!(access.retention_expired_at(tick(310)).unwrap());
    }

    // req: OQ-040
    #[test]
    fn a_backwards_clock_or_overflow_cannot_shorten_an_existing_obligation() {
        let mut access = ProviderAccess::new(7, tick(10)).unwrap();
        assert!(access.touch(8, tick(9)).is_err());
        assert!(access.retention_expired_at(tick(9)).is_err());
        let maximum = CursorTick::from_process_elapsed(Duration::from_nanos(u64::MAX)).unwrap();
        assert!(access.touch(8, maximum).is_err());
        assert_eq!(access.ordinal(), 7);
        assert!(!access.retention_expired_at(tick(309)).unwrap());
        assert!(access.retention_expired_at(tick(310)).unwrap());
        assert!(ProviderAccess::new(8, maximum).is_err());
    }

    // req: OQ-040
    #[test]
    fn ordinal_rebasing_preserves_the_lease_deadline() {
        let mut access = ProviderAccess::new(u64::MAX - 1, tick(10)).unwrap();
        access.rebase(3);
        assert_eq!(access.ordinal(), 3);
        assert!(!access.retention_expired_at(tick(309)).unwrap());
        assert!(access.retention_expired_at(tick(310)).unwrap());
    }
}
