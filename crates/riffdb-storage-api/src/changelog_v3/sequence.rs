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

    /// Canonical allocator payload: version u16 BE, closed state tag, u64 BE.
    /// Exhaustion uses tag 2 and eight zero bytes; Next uses tag 1 and nonzero.
    #[must_use]
    pub fn to_canonical_bytes(self) -> [u8; 11] {
        let mut bytes = [0; 11];
        bytes[1] = 3;
        match self {
            Self::Next(sequence) => {
                bytes[2] = 1;
                bytes[3..].copy_from_slice(&sequence.get().to_be_bytes());
            }
            Self::Exhausted => bytes[2] = 2,
        }
        bytes
    }

    /// Refuses unknown versions/tags, zero Next, malformed exhaustion and trailing bytes.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, ChangelogV3Error> {
        if bytes.len() != 11 || bytes[..2] != [0, 3] {
            return Err(ChangelogV3Error::InvalidEncoding);
        }
        let value = u64::from_be_bytes(
            bytes[3..]
                .try_into()
                .map_err(|_| ChangelogV3Error::InvalidEncoding)?,
        );
        match (bytes[2], value) {
            (1, value) => ChangelogTransactionSequence::new(value)
                .map(Self::Next)
                .ok_or(ChangelogV3Error::InvalidEncoding),
            (2, 0) => Ok(Self::Exhausted),
            _ => Err(ChangelogV3Error::InvalidEncoding),
        }
    }
}
