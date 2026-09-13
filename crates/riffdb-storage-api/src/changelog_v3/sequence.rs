use std::num::NonZeroU64;

use super::ChangelogV3Error;

/// Nonzero physical authoritative transaction position, not an application sequence.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ChangelogTransactionSequence(NonZeroU64);

impl ChangelogTransactionSequence {
    /// Reconstructs a position; zero always means unassigned, never a transaction.
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the physical transaction position.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Checked successor; exhaustion never wraps.
    #[must_use]
    pub const fn checked_next(self) -> Option<Self> {
        match self.get().checked_add(1) {
            Some(next) => Self::new(next),
            None => None,
        }
    }
}

/// Allocator state persisted in the same transaction as its assigned receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChangelogTransactionAllocator {
    /// Next assignable physical transaction position.
    Next(ChangelogTransactionSequence),
    /// Maximum sequence was assigned; no further transaction can be accepted.
    Exhausted,
}

impl ChangelogTransactionAllocator {
    /// Receipted activation starts at the first nonzero physical position.
    #[must_use]
    pub const fn initial() -> Self {
        Self::Next(ChangelogTransactionSequence(NonZeroU64::MIN))
    }

    /// Preflights one assignment without changing storage or this value.
    pub fn allocate_one(self) -> Result<(ChangelogTransactionSequence, Self), ChangelogV3Error> {
        let Self::Next(assigned) = self else {
            return Err(ChangelogV3Error::SequenceExhausted);
        };
        Ok((
            assigned,
            assigned.checked_next().map_or(Self::Exhausted, Self::Next),
        ))
    }
}
