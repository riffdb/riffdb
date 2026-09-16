//! Bounded multi-sequence projection application (ADR-0230).

use std::collections::BTreeMap;

use super::*;

/// Maximum contiguous members in one derived projection transaction.
pub const MAX_PROJECTION_BATCH_MEMBERS: usize = 64;

/// A pinned immutable projection base, retained only during bounded preparation.
pub trait ProjectionBatchSnapshot: ProjectionApplySnapshotReader {
    /// Complete control captured in the same immutable view as all row reads.
    fn control(&self) -> &StoredProjectionControlV1;
}

/// Complete checked sequence batch and its independently observed base rows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionApplyBatchV1 {
    expected: StoredProjectionControlV1,
    members: Vec<ProjectionApplyRequestV1>,
    observations: Vec<ProjectionApplyRowObservation>,
    semantic_bytes: usize,
}

impl ProjectionApplyBatchV1 {
    /// Conservative fixed control reservation shared with incremental builders.
    pub fn control_semantic_bytes(
        identity: &ProjectionIdentity,
    ) -> Result<usize, StorageValueError> {
        maximum_projection_control_semantic_bytes(identity)
    }

    /// Checks the exact base, contiguous chain, reciprocal row evidence and total bounds.
    pub fn new(
        expected: StoredProjectionControlV1,
        members: Vec<ProjectionApplyRequestV1>,
        observations: Vec<ProjectionApplyRowObservation>,
    ) -> Result<Self, StorageValueError> {
        let first = members.first().ok_or(StorageValueError::InvalidShape)?;
        if members.len() > MAX_PROJECTION_BATCH_MEMBERS
            || observations.len() > MAX_PROJECTION_ROW_UPDATES
        {
            return Err(StorageValueError::LimitExceeded);
        }
        if expected.identity() != first.identity()
            || !expected.permits_application(first.generation())
            || expected.frontier_for(first.generation()) != Some(first.expected_frontier())
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        let mut bytes = maximum_projection_control_semantic_bytes(first.identity())?;
        let mut previous = None;
        let mut rows = BTreeMap::new();
        for observation in &observations {
            let key = observation.key();
            if previous.is_some_and(|prior| prior >= key.as_bytes()) {
                return Err(StorageValueError::NonCanonicalOrder);
            }
            first.schema().validate_group_key(key)?;
            if key.generation() != first.generation() {
                return Err(StorageValueError::IdentityMismatch);
            }
            if let ProjectionApplyRowObservation::Present(row) = observation {
                first.schema().validate_measure_record(row.measures())?;
            }
            // Reserve both original evidence and the mutable preparation overlay.
            bytes = checked_projection_sum([
                bytes,
                projection_snapshot_observation_semantic_bytes(
                    observation,
                    first.expected_frontier(),
                )?
                .checked_mul(2)
                .ok_or(StorageValueError::SizeOverflow)?,
            ])?;
            validate_projection_snapshot_semantic_bytes(bytes)?;
            previous = Some(key.as_bytes());
            rows.insert(key, (observation.prior(), false));
        }
        let mut frontier = first.expected_frontier();
        let mut updates = 0usize;
        for member in &members {
            if member.schema() != first.schema()
                || member.generation() != first.generation()
                || member.expected_frontier() != frontier
            {
                return Err(StorageValueError::IdentityMismatch);
            }
            // Member constructors already prove exact succession and canonical hashes.
            updates = updates
                .checked_add(member.row_updates().len())
                .ok_or(StorageValueError::SizeOverflow)?;
            if updates > MAX_PROJECTION_ROW_UPDATES {
                return Err(StorageValueError::LimitExceeded);
            }
            bytes = checked_projection_sum([
                bytes,
                member.semantic_bytes(),
                member.write_set_semantic_bytes(),
            ])?;
            validate_projection_write_set_semantic_bytes(bytes)?;
            for update in member.row_updates() {
                let (prior, used) = rows
                    .get_mut(update.key())
                    .ok_or(StorageValueError::InvalidShape)?;
                if *prior != update.prior() {
                    return Err(StorageValueError::InvalidShape);
                }
                *prior = ProjectionRowPrior::Present(member.sequence());
                *used = true;
            }
            frontier = FrontierPosition::AppliedThrough(member.sequence());
        }
        if rows.values().any(|(_, used)| !used) {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self {
            expected,
            members,
            observations,
            semantic_bytes: bytes,
        })
    }

    /// Complete expected lifecycle/control, not merely a frontier.
    #[must_use]
    pub const fn expected(&self) -> &StoredProjectionControlV1 {
        &self.expected
    }
    /// Contiguous canonical requests, including every irrelevant commit.
    #[must_use]
    pub fn members(&self) -> &[ProjectionApplyRequestV1] {
        &self.members
    }
    /// Exact first-access row evidence from the immutable base.
    #[must_use]
    pub fn observations(&self) -> &[ProjectionApplyRowObservation] {
        &self.observations
    }
    /// Aggregate charge for requests, evidence, overlay, markers and control.
    #[must_use]
    pub const fn semantic_bytes(&self) -> usize {
        self.semantic_bytes
    }
}

impl ProjectionApplyRowObservation {
    /// Prior identity used by checked request construction.
    #[must_use]
    pub const fn prior(&self) -> ProjectionRowPrior {
        match self {
            Self::Absent(_) => ProjectionRowPrior::Absent,
            Self::Present(row) => ProjectionRowPrior::Present(row.last_changed_sequence()),
        }
    }

    /// Checked semantic state charge for retaining this observation at a frontier.
    pub fn semantic_bytes_at(
        &self,
        frontier: FrontierPosition,
    ) -> Result<usize, StorageValueError> {
        projection_snapshot_observation_semantic_bytes(self, frontier)
    }
}

/// Outcome of one atomic derived sequence batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProjectionApplyBatchResult {
    /// Every new member and the final control became durable together.
    Applied(StoredProjectionControlV1),
    /// Every retained historical marker matched; old base evidence is obsolete.
    AlreadyApplied,
    /// Changed control/base or a checked already-applied prefix requires fresh preparation.
    StateChanged,
}
