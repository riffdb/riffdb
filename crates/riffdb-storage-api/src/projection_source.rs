//! ADR-0316 structural authoritative replay-source contracts.
//!
//! These values check representation and bounded arithmetic only. They never
//! certify independent evaluation, immutable staging custody, transition
//! ancestry, publication, or permission to seed a derived generation/prune.

mod admission;
mod descriptor;
mod maintenance;
mod origin;
mod row;

pub use admission::*;
pub use descriptor::*;
pub use maintenance::*;
pub use origin::*;
pub use row::*;

use std::num::NonZeroU64;

use riffdb_types::{DatabaseId, ProjectionCheckpointOrdinal, ProjectionSourceLocatorV1};

use crate::StorageValueError;

/// Closed next-or-exhausted position of the permanent source ordinal allocator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectionCheckpointOrdinalPositionV1 {
    /// Next nonzero ordinal available in this allocation incarnation.
    Next(ProjectionCheckpointOrdinal),
    /// The maximum ordinal has already been assigned and cannot be reused.
    Exhausted,
}

/// Structural permanent allocator state; only the coordinator persists advances.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectionCheckpointOrdinalAllocatorV1 {
    allocation_history_incarnation: NonZeroU64,
    position: ProjectionCheckpointOrdinalPositionV1,
}

impl ProjectionCheckpointOrdinalAllocatorV1 {
    /// Reconstructs a nonzero stored incarnation and closed allocator position.
    pub const fn new(
        allocation_history_incarnation: u64,
        position: ProjectionCheckpointOrdinalPositionV1,
    ) -> Result<Self, StorageValueError> {
        match NonZeroU64::new(allocation_history_incarnation) {
            Some(allocation_history_incarnation) => Ok(Self {
                allocation_history_incarnation,
                position,
            }),
            None => Err(StorageValueError::InvalidShape),
        }
    }

    /// Constructs Next(1) for a separately verified new-incarnation transition.
    /// This value does not prove that a transition occurred or permit a reset.
    pub const fn initial(allocation_history_incarnation: u64) -> Result<Self, StorageValueError> {
        Self::new(
            allocation_history_incarnation,
            ProjectionCheckpointOrdinalPositionV1::Next(ProjectionCheckpointOrdinal::first()),
        )
    }

    /// Incarnation in which this allocator may allocate new source identities.
    #[must_use]
    pub const fn allocation_history_incarnation(self) -> u64 {
        self.allocation_history_incarnation.get()
    }

    /// Stored next-or-exhausted position, never a synthetic zero.
    #[must_use]
    pub const fn position(self) -> ProjectionCheckpointOrdinalPositionV1 {
        self.position
    }

    /// Preflights one allocation against the transaction-current incarnation.
    /// The candidate and returned allocator post-image must commit atomically.
    pub fn allocate_one(
        self,
        database: DatabaseId,
        current_incarnation: u64,
    ) -> Result<ProjectionCheckpointOrdinalAllocationV1, ProjectionSourceAllocationErrorV1> {
        if self.allocation_history_incarnation() != current_incarnation {
            return Err(ProjectionSourceAllocationErrorV1::IncarnationMismatch);
        }
        let ProjectionCheckpointOrdinalPositionV1::Next(ordinal) = self.position else {
            return Err(ProjectionSourceAllocationErrorV1::Exhausted);
        };
        let locator = ProjectionSourceLocatorV1::new(database, current_incarnation, ordinal)
            .ok_or(ProjectionSourceAllocationErrorV1::IncarnationMismatch)?;
        let position = ordinal.checked_next().map_or(
            ProjectionCheckpointOrdinalPositionV1::Exhausted,
            ProjectionCheckpointOrdinalPositionV1::Next,
        );
        Ok(ProjectionCheckpointOrdinalAllocationV1 {
            locator,
            next: Self { position, ..self },
        })
    }
}

/// One preflight result, not an allocated durable source or write capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectionCheckpointOrdinalAllocationV1 {
    locator: ProjectionSourceLocatorV1,
    next: ProjectionCheckpointOrdinalAllocatorV1,
}

impl ProjectionCheckpointOrdinalAllocationV1 {
    /// Full candidate locator that must be committed with its allocator advance.
    #[must_use]
    pub const fn locator(self) -> ProjectionSourceLocatorV1 {
        self.locator
    }

    /// Permanent allocator post-image, including explicit maximum exhaustion.
    #[must_use]
    pub const fn next(self) -> ProjectionCheckpointOrdinalAllocatorV1 {
        self.next
    }
}

/// Closed precommit allocation refusal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectionSourceAllocationErrorV1 {
    /// The transaction belongs to a different history incarnation.
    IncarnationMismatch,
    /// No ordinal remains in this allocation incarnation.
    Exhausted,
}

/// Closed source-object lifecycle. It is separate from immutable source identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectionSourceLifecycleV1 {
    /// Hidden candidate rows are being copied and cannot justify source closure.
    Copying,
    /// Complete object selected by the exact native published root.
    Published,
    /// No new seeding; existing valid bases/pins retain exact base-and-tail access.
    Superseded,
    /// Unreferenced superseded namespace is being removed in bounded chunks.
    Collecting,
    /// An incomplete candidate is being removed in bounded chunks.
    Aborting,
    /// Complete removal; the ordinal remains permanently consumed.
    Removed,
}

impl ProjectionSourceLifecycleV1 {
    /// Explicit semantic tag for the separately reviewed versioned codec.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::Copying => 1,
            Self::Published => 2,
            Self::Superseded => 3,
            Self::Collecting => 4,
            Self::Aborting => 5,
            Self::Removed => 6,
        }
    }

    /// Reconstructs a closed known lifecycle without granting publication.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::Copying),
            2 => Some(Self::Published),
            3 => Some(Self::Superseded),
            4 => Some(Self::Collecting),
            5 => Some(Self::Aborting),
            6 => Some(Self::Removed),
            _ => None,
        }
    }

    /// Checks the closed lifecycle edge only; native closure checks remain required.
    pub const fn transition(self, next: Self) -> Result<Self, StorageValueError> {
        match (self, next) {
            (Self::Copying, Self::Published | Self::Aborting)
            | (Self::Published, Self::Superseded)
            | (Self::Superseded, Self::Collecting)
            | (Self::Collecting | Self::Aborting, Self::Removed) => Ok(next),
            _ => Err(StorageValueError::InvalidShape),
        }
    }
}

riffdb_types::inspect_owned_capacity_copy_v1!(ProjectionCheckpointOrdinalAllocatorV1);

#[cfg(test)]
mod tests;
