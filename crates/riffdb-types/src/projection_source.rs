//! Structural replay-source locators. A matching tuple is never authority.

use std::num::NonZeroU64;

use crate::DatabaseId;

/// Maximum complete ADR-0316 descriptor/root/maintenance/control envelope.
pub const MAX_PROJECTION_SOURCE_CONTROL_BYTES: usize = 4 * 1024;

/// Coordinator-allocated nonzero checkpoint ordinal within one allocation incarnation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProjectionCheckpointOrdinal(NonZeroU64);

impl ProjectionCheckpointOrdinal {
    /// Reconstructs a nonzero ordinal; this does not grant allocation authority.
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Initial position of a checked new-incarnation allocator.
    #[must_use]
    pub const fn first() -> Self {
        Self(NonZeroU64::MIN)
    }

    /// Numeric ordinal, never a zero sentinel.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Canonical ordered numeric representation.
    #[must_use]
    pub const fn to_be_bytes(self) -> [u8; 8] {
        self.get().to_be_bytes()
    }

    /// Checked successor; exhaustion never wraps or reuses an ordinal.
    #[must_use]
    pub const fn checked_next(self) -> Option<Self> {
        match self.get().checked_add(1) {
            Some(value) => Self::new(value),
            None => None,
        }
    }
}

/// Full structural locator of one authoritative replay source.
///
/// The allocation incarnation belongs to the original allocation, including
/// after checked restore/promotion/bootstrap. Matching fields do not establish
/// transition validity or cross-fork uniqueness; references also require the
/// immutable digest/origin commitment and verified source-transition evidence.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProjectionSourceLocatorV1 {
    database_id: DatabaseId,
    allocation_history_incarnation: NonZeroU64,
    ordinal: ProjectionCheckpointOrdinal,
}

impl ProjectionSourceLocatorV1 {
    /// Reconstructs the locator without changing its inherited origin identity.
    #[must_use]
    pub const fn new(
        database_id: DatabaseId,
        allocation_history_incarnation: u64,
        ordinal: ProjectionCheckpointOrdinal,
    ) -> Option<Self> {
        match NonZeroU64::new(allocation_history_incarnation) {
            Some(allocation_history_incarnation) => Some(Self {
                database_id,
                allocation_history_incarnation,
                ordinal,
            }),
            None => None,
        }
    }

    /// Permanent database identity of the original allocation.
    #[must_use]
    pub const fn database_id(self) -> DatabaseId {
        self.database_id
    }

    /// Nonzero incarnation in which the coordinator assigned the ordinal.
    #[must_use]
    pub const fn allocation_history_incarnation(self) -> u64 {
        self.allocation_history_incarnation.get()
    }

    /// Original nonzero allocation ordinal.
    #[must_use]
    pub const fn ordinal(self) -> ProjectionCheckpointOrdinal {
        self.ordinal
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ContentDigest, DIGEST_SCHEME_V1, HashDomain, hash};

    fn database_id(seed: u8) -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(42, [seed; 10]).unwrap()
    }

    // req: STO-012, REC-001
    #[test]
    fn replay_source_locator_preserves_full_original_allocation_identity() {
        let database = database_id(3);
        let ordinal = ProjectionCheckpointOrdinal::first();
        assert!(ProjectionCheckpointOrdinal::new(0).is_none());
        assert!(ProjectionSourceLocatorV1::new(database, 0, ordinal).is_none());
        let original = ProjectionSourceLocatorV1::new(database, 4, ordinal).unwrap();
        let later_allocation = ProjectionSourceLocatorV1::new(database, 5, ordinal).unwrap();
        let foreign = ProjectionSourceLocatorV1::new(database_id(4), 4, ordinal).unwrap();
        assert_ne!(original, later_allocation);
        assert_ne!(original, foreign);
        let captured_before_restore = original;
        assert_eq!(captured_before_restore.allocation_history_incarnation(), 4);
        assert_eq!(captured_before_restore.ordinal(), ordinal);
        assert_eq!(captured_before_restore.database_id(), database);
    }

    // req: STO-012
    #[test]
    fn replay_checkpoint_ordinal_exhaustion_never_wraps() {
        let last = ProjectionCheckpointOrdinal::new(u64::MAX).unwrap();
        assert!(last.checked_next().is_none());
        let prior = ProjectionCheckpointOrdinal::new(u64::MAX - 1).unwrap();
        assert_eq!(prior.checked_next(), Some(last));
        assert!(prior.to_be_bytes() < last.to_be_bytes());
    }

    // req: PRJ-004, REC-003
    #[test]
    fn replay_source_hash_roles_are_separate_and_stored_scheme_is_checked() {
        let input = b"same bounded canonical preimage";
        let rows = hash(HashDomain::ProjectionSourceRows, input);
        let descriptor = hash(HashDomain::ProjectionSourceDescriptor, input);
        let origin = hash(HashDomain::ProjectionSourceOrigin, input);
        assert_ne!(rows, descriptor);
        assert_ne!(descriptor, origin);
        assert_ne!(rows, origin);
        assert_eq!(
            ContentDigest::from_stored_parts(DIGEST_SCHEME_V1, rows.domain(), *rows.as_bytes()),
            Some(rows)
        );
        assert!(ContentDigest::from_stored_parts(0, rows.domain(), *rows.as_bytes()).is_none());
        assert!(ContentDigest::from_stored_parts(2, rows.domain(), *rows.as_bytes()).is_none());
    }
}
