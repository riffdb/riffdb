//! Closed nonzero sequence allocator values and checked preflight.

use std::error::Error;
use std::fmt;

use riffdb_types::{AdministrationSequence, CommitSequence};

/// Failure to allocate a complete consecutive sequence range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SequenceAllocationError {
    /// A zero-length allocation is not meaningful.
    ZeroCount,
    /// The complete requested range cannot be represented.
    Exhausted,
    /// A bounded operation requested more sequence slots than its accepted limit.
    TooMany,
}

impl fmt::Display for SequenceAllocationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ZeroCount => "sequence allocation count must be nonzero",
            Self::Exhausted => "sequence allocation is exhausted",
            Self::TooMany => "sequence allocation count exceeds its bounded operation limit",
        })
    }
}

impl Error for SequenceAllocationError {}

/// Durable application-sequence allocator state.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ApplicationSequenceAllocator {
    /// The next sequence that may be assigned.
    Next(CommitSequence),
    /// No further application sequence can be assigned.
    Exhausted,
}

impl ApplicationSequenceAllocator {
    /// Returns canonical empty-database state, `Next(1)`.
    #[must_use]
    pub const fn initial() -> Self {
        Self::Next(CommitSequence::first())
    }

    /// Constructs a non-exhausted allocator state.
    #[must_use]
    pub const fn next(sequence: CommitSequence) -> Self {
        Self::Next(sequence)
    }

    /// Preflights and allocates one sequence without mutating this value.
    pub fn allocate_one(&self) -> Result<ApplicationSequenceAllocation, SequenceAllocationError> {
        let range = self.allocate_consecutive(1)?;
        Ok(ApplicationSequenceAllocation {
            assigned: range.assigned[0],
            next: range.next,
        })
    }

    /// Preflights an entire bounded consecutive range before assigning any value.
    pub fn allocate_consecutive(
        &self,
        count: u16,
    ) -> Result<ApplicationSequenceRange, SequenceAllocationError> {
        validate_application_count(count)?;
        let Self::Next(mut current) = *self else {
            return Err(SequenceAllocationError::Exhausted);
        };
        let mut assigned = Vec::with_capacity(usize::from(count));
        for index in 0..count {
            assigned.push(current);
            if index + 1 < count {
                current = current
                    .checked_next()
                    .ok_or(SequenceAllocationError::Exhausted)?;
            }
        }
        let next = current.checked_next().map_or(Self::Exhausted, Self::Next);
        Ok(ApplicationSequenceRange { assigned, next })
    }
}

/// One checked application-sequence allocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplicationSequenceAllocation {
    assigned: CommitSequence,
    next: ApplicationSequenceAllocator,
}

impl ApplicationSequenceAllocation {
    /// Returns the sequence assigned by the operation.
    #[must_use]
    pub const fn assigned(self) -> CommitSequence {
        self.assigned
    }

    /// Returns allocator state to persist atomically with the operation.
    #[must_use]
    pub const fn next(self) -> ApplicationSequenceAllocator {
        self.next
    }
}

/// A checked bounded consecutive application-sequence allocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationSequenceRange {
    assigned: Vec<CommitSequence>,
    next: ApplicationSequenceAllocator,
}

impl ApplicationSequenceRange {
    /// Borrows assigned values in increasing order.
    #[must_use]
    pub fn assigned(&self) -> &[CommitSequence] {
        &self.assigned
    }

    /// Returns allocator state to persist atomically with the operation.
    #[must_use]
    pub const fn next(&self) -> ApplicationSequenceAllocator {
        self.next
    }
}

/// Durable administration-sequence allocator state.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AdministrationSequenceAllocator {
    /// The next sequence that may be assigned.
    Next(AdministrationSequence),
    /// No further administration sequence can be assigned.
    Exhausted,
}

impl AdministrationSequenceAllocator {
    /// Returns canonical empty-database state, `Next(1)`.
    #[must_use]
    pub const fn initial() -> Self {
        Self::Next(AdministrationSequence::first())
    }

    /// Constructs a non-exhausted allocator state.
    #[must_use]
    pub const fn next(sequence: AdministrationSequence) -> Self {
        Self::Next(sequence)
    }

    /// Preflights and allocates one sequence without mutating this value.
    pub fn allocate_one(
        &self,
    ) -> Result<AdministrationSequenceAllocation, SequenceAllocationError> {
        let range = self.allocate_consecutive(1)?;
        Ok(AdministrationSequenceAllocation {
            assigned: range.assigned[0],
            next: range.next,
        })
    }

    /// Preflights an entire bounded consecutive range before assigning any value.
    pub fn allocate_consecutive(
        &self,
        count: u16,
    ) -> Result<AdministrationSequenceRange, SequenceAllocationError> {
        validate_administration_count(count)?;
        let Self::Next(mut current) = *self else {
            return Err(SequenceAllocationError::Exhausted);
        };
        let mut assigned = Vec::with_capacity(usize::from(count));
        for index in 0..count {
            assigned.push(current);
            if index + 1 < count {
                current = current
                    .checked_next()
                    .ok_or(SequenceAllocationError::Exhausted)?;
            }
        }
        let next = current.checked_next().map_or(Self::Exhausted, Self::Next);
        Ok(AdministrationSequenceRange { assigned, next })
    }
}

/// One checked administration-sequence allocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdministrationSequenceAllocation {
    assigned: AdministrationSequence,
    next: AdministrationSequenceAllocator,
}

impl AdministrationSequenceAllocation {
    /// Returns the sequence assigned by the operation.
    #[must_use]
    pub const fn assigned(self) -> AdministrationSequence {
        self.assigned
    }

    /// Returns allocator state to persist atomically with the operation.
    #[must_use]
    pub const fn next(self) -> AdministrationSequenceAllocator {
        self.next
    }
}

/// A checked bounded consecutive administration-sequence allocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdministrationSequenceRange {
    assigned: Vec<AdministrationSequence>,
    next: AdministrationSequenceAllocator,
}

impl AdministrationSequenceRange {
    /// Borrows assigned values in increasing order.
    #[must_use]
    pub fn assigned(&self) -> &[AdministrationSequence] {
        &self.assigned
    }

    /// Returns allocator state to persist atomically with the operation.
    #[must_use]
    pub const fn next(&self) -> AdministrationSequenceAllocator {
        self.next
    }
}

fn validate_application_count(count: u16) -> Result<(), SequenceAllocationError> {
    if count == 0 {
        Err(SequenceAllocationError::ZeroCount)
    } else if usize::from(count) <= crate::MAX_STAGED_COMMANDS {
        Ok(())
    } else {
        Err(SequenceAllocationError::TooMany)
    }
}

fn validate_administration_count(count: u16) -> Result<(), SequenceAllocationError> {
    if count == 0 {
        Err(SequenceAllocationError::ZeroCount)
    } else if usize::from(count) <= crate::MAX_GROUPED_WRITE_TRANSITIONS.saturating_mul(2) {
        // One fused command transition contains Started plus one terminal row.
        Ok(())
    } else {
        Err(SequenceAllocationError::TooMany)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AdministrationSequenceAllocator, ApplicationSequenceAllocator, SequenceAllocationError,
    };

    #[test]
    fn application_allocation_accepts_256_and_rejects_257() {
        let allocation = ApplicationSequenceAllocator::initial()
            .allocate_consecutive(256)
            .expect("256 application sequences are bounded");
        assert_eq!(allocation.assigned().len(), 256);
        assert_eq!(
            ApplicationSequenceAllocator::initial().allocate_consecutive(257),
            Err(SequenceAllocationError::TooMany)
        );
    }

    #[test]
    fn administration_allocation_covers_two_rows_per_maximum_command_group() {
        let allocation = AdministrationSequenceAllocator::initial()
            .allocate_consecutive(512)
            .expect("512 fused service-audit rows are bounded");
        assert_eq!(allocation.assigned().len(), 512);
        assert_eq!(
            AdministrationSequenceAllocator::initial().allocate_consecutive(513),
            Err(SequenceAllocationError::TooMany)
        );
    }
}
