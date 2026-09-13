use std::num::NonZeroU64;

/// Nonzero lineage-shared leadership fence, distinct from a transaction position.
/// Constructing a value never grants leadership or authorizes its persistence.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LeadershipEpochV1(NonZeroU64);

impl LeadershipEpochV1 {
    /// Reconstructs a nonzero fence; zero is never an active leadership epoch.
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Initial leadership fence for receipted activation.
    #[must_use]
    pub const fn initial() -> Self {
        Self(NonZeroU64::MIN)
    }

    /// Returns the exact nonzero position.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Preflights a promotion fence without mutation; exhaustion never wraps.
    #[must_use]
    pub const fn checked_next(self) -> Option<Self> {
        match self.get().checked_add(1) {
            Some(next) => Self::new(next),
            None => None,
        }
    }
}
