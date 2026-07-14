//! Bounded structural integrity checks for the derived projection tables.

use std::cmp::Ordering;
use std::ops::Range;

use riffdb_storage_api::{
    ApplicationSequenceAllocator, ProjectionGenerationPosition, ProjectionLifecycleV1,
    PublishedApplyModeV1, StoredProjectionControlV1, StructuralFinding, StructuralFindingCode,
    StructuralFindingScope,
};
use riffdb_types::{
    CommitSequence, FrontierPosition, ProjectionApplyKey, ProjectionGeneration, ProjectionIdentity,
};

use crate::state::{MemoryMetadataSlot, MemoryState};

/// Inspects one control record without interpreting projection worker policy.
pub(crate) fn inspect_projection_control(
    state: &MemoryState,
    index: usize,
) -> Option<StructuralFinding> {
    let Some(control) = state.projection_controls.get(index) else {
        return Some(projection_finding(StructuralFindingCode::MalformedRecord));
    };
    if index > 0
        && compare_identity_bytes(
            state.projection_controls[index - 1].identity(),
            control.identity(),
        ) != Ordering::Less
    {
        return Some(projection_finding(StructuralFindingCode::CrossLinkMismatch));
    }
    if !control_shape_is_valid(control) {
        return Some(projection_finding(
            StructuralFindingCode::ProjectionStateMismatch,
        ));
    }
    if let Some(failure) = control.failure()
        && let Some(sequence) = failure.at_sequence()
    {
        let Some(position) = retained_position(control, failure.generation()) else {
            return Some(projection_finding(
                StructuralFindingCode::ProjectionStateMismatch,
            ));
        };
        if !is_exact_successor(position.frontier(), sequence) {
            return Some(projection_finding(
                StructuralFindingCode::ProjectionStateMismatch,
            ));
        }
        if let Some(finding) = inspect_authoritative_commit_source(state, sequence) {
            return Some(finding);
        }
    }

    for position in [control.published(), control.candidate()]
        .into_iter()
        .flatten()
    {
        if !retained_generation_is_reciprocal(state, control.identity(), position) {
            return Some(projection_finding(
                StructuralFindingCode::ProjectionStateMismatch,
            ));
        }
        if let FrontierPosition::AppliedThrough(sequence) = position.frontier()
            && let Some(finding) = inspect_authoritative_commit_source(state, sequence)
        {
            return Some(finding);
        }
    }
    None
}

/// Inspects one durable projection state row.
pub(crate) fn inspect_projection_state(
    state: &MemoryState,
    index: usize,
) -> Option<StructuralFinding> {
    let Some(row) = state.projection_states.get(index) else {
        return Some(projection_finding(StructuralFindingCode::MalformedRecord));
    };
    if index > 0 && state.projection_states[index - 1].key() >= row.key() {
        return Some(projection_finding(StructuralFindingCode::CrossLinkMismatch));
    }

    let Some(control) = find_unique_control(state, row.identity()) else {
        return Some(projection_finding(StructuralFindingCode::MissingCrossLink));
    };
    if row.generation() > control.highest_allocated_generation() {
        return Some(projection_finding(
            StructuralFindingCode::ProjectionStateMismatch,
        ));
    }

    let Some(position) = retained_position(control, row.generation()) else {
        // Canonical rows of retired generations are explicitly inert and may
        // remain without a retained frontier or complete marker tail.
        return None;
    };
    let FrontierPosition::AppliedThrough(frontier) = position.frontier() else {
        return Some(projection_finding(
            StructuralFindingCode::ProjectionStateMismatch,
        ));
    };
    if row.last_changed_sequence() > frontier {
        return Some(projection_finding(
            StructuralFindingCode::ProjectionStateMismatch,
        ));
    }
    if let Some(finding) = inspect_authoritative_commit_source(state, row.last_changed_sequence()) {
        return Some(finding);
    }
    if find_unique_apply(
        state,
        row.identity(),
        row.generation(),
        row.last_changed_sequence(),
    )
    .is_none()
    {
        return Some(projection_finding(StructuralFindingCode::MissingCrossLink));
    }
    None
}

/// Inspects one durable projection apply marker.
pub(crate) fn inspect_projection_apply(
    state: &MemoryState,
    index: usize,
) -> Option<StructuralFinding> {
    let Some(marker) = state.projection_applies.get(index) else {
        return Some(projection_finding(StructuralFindingCode::MalformedRecord));
    };
    if index > 0 && state.projection_applies[index - 1].key() >= marker.key() {
        return Some(projection_finding(StructuralFindingCode::CrossLinkMismatch));
    }

    let key = marker.key();
    let Some(control) = find_unique_control(state, key.identity()) else {
        return Some(projection_finding(StructuralFindingCode::MissingCrossLink));
    };
    if key.generation() > control.highest_allocated_generation() {
        return Some(projection_finding(
            StructuralFindingCode::ProjectionStateMismatch,
        ));
    }
    if let Some(position) = retained_position(control, key.generation()) {
        match position.frontier() {
            FrontierPosition::AppliedThrough(frontier) if key.commit_sequence() <= frontier => {}
            FrontierPosition::BeforeFirst | FrontierPosition::AppliedThrough(_) => {
                return Some(projection_finding(
                    StructuralFindingCode::ProjectionStateMismatch,
                ));
            }
        }
    }

    inspect_authoritative_commit_source(state, key.commit_sequence())
}

fn projection_finding(code: StructuralFindingCode) -> StructuralFinding {
    StructuralFinding::new(StructuralFindingScope::Projection, code)
}

fn authoritative_finding(code: StructuralFindingCode) -> StructuralFinding {
    StructuralFinding::new(StructuralFindingScope::Authoritative, code)
}

fn control_shape_is_valid(control: &StoredProjectionControlV1) -> bool {
    let highest = control.highest_allocated_generation();
    let published = control.published();
    let candidate = control.candidate();
    let mode = control.published_apply_mode();
    let failure = control.failure();

    if [published, candidate]
        .into_iter()
        .flatten()
        .any(|position| position.generation() > highest)
        || published
            .zip(candidate)
            .is_some_and(|(left, right)| left.generation() == right.generation())
        || candidate.is_some_and(|position| position.generation() != highest)
        || (published.is_some() != mode.is_some())
    {
        return false;
    }

    match control.lifecycle() {
        ProjectionLifecycleV1::Building => {
            published.is_none()
                && candidate.is_some_and(|position| {
                    position.generation() == highest
                        && position.frontier() == FrontierPosition::BeforeFirst
                })
                && mode.is_none()
                && failure.is_none()
        }
        ProjectionLifecycleV1::CatchingUp => {
            published.is_none()
                && candidate.is_some_and(|position| position.generation() == highest)
                && mode.is_none()
                && failure.is_none()
        }
        ProjectionLifecycleV1::Ready => {
            published.is_some_and(|position| position.generation() == highest)
                && candidate.is_none()
                && mode == Some(PublishedApplyModeV1::Enabled)
                && failure.is_none()
        }
        ProjectionLifecycleV1::Rebuilding => {
            published.is_some_and(|position| position.generation() < highest)
                && candidate.is_some_and(|position| position.generation() == highest)
                && failure.is_none()
        }
        ProjectionLifecycleV1::Degraded | ProjectionLifecycleV1::Invalid => {
            let Some(failure) = failure else {
                return false;
            };
            let retained = retained_position(control, failure.generation()).is_some();
            let failed_published =
                published.is_some_and(|position| position.generation() == failure.generation());
            retained
                && (published.is_some() || candidate.is_some())
                && (!failed_published || mode == Some(PublishedApplyModeV1::Suspended))
        }
    }
}

fn retained_generation_is_reciprocal(
    state: &MemoryState,
    identity: &ProjectionIdentity,
    position: ProjectionGenerationPosition,
) -> bool {
    let marker_range = apply_namespace_range(state, identity, position.generation());
    let state_range = state_namespace_range(state, identity, position.generation());
    match position.frontier() {
        FrontierPosition::BeforeFirst => marker_range.is_empty() && state_range.is_empty(),
        FrontierPosition::AppliedThrough(frontier) => {
            let Ok(marker_count) = u64::try_from(marker_range.len()) else {
                return false;
            };
            let first = marker_range.start;
            let Some(last) = marker_range.end.checked_sub(1) else {
                return false;
            };
            marker_count == frontier.get()
                && state.projection_applies[first].key().commit_sequence()
                    == CommitSequence::first()
                && state.projection_applies[last].key().commit_sequence() == frontier
        }
    }
}

fn inspect_authoritative_commit_source(
    state: &MemoryState,
    sequence: CommitSequence,
) -> Option<StructuralFinding> {
    let record = sequence
        .get()
        .checked_sub(1)
        .and_then(|offset| usize::try_from(offset).ok())
        .and_then(|index| state.commits.get(index));
    if record.is_some_and(|record| record.commit_sequence() == sequence) {
        return None;
    }

    let allocated = match &state.metadata {
        MemoryMetadataSlot::Retained(metadata) => match metadata.application_sequence() {
            ApplicationSequenceAllocator::Next(next) => sequence < next,
            ApplicationSequenceAllocator::Exhausted => true,
        },
        MemoryMetadataSlot::Absent => {
            return Some(authoritative_finding(
                StructuralFindingCode::MalformedRecord,
            ));
        }
        #[cfg(test)]
        MemoryMetadataSlot::Corrupt => {
            return Some(authoritative_finding(
                StructuralFindingCode::MalformedRecord,
            ));
        }
    };
    Some(if allocated {
        authoritative_finding(StructuralFindingCode::MissingCrossLink)
    } else {
        projection_finding(StructuralFindingCode::ProjectionStateMismatch)
    })
}

fn retained_position(
    control: &StoredProjectionControlV1,
    generation: ProjectionGeneration,
) -> Option<ProjectionGenerationPosition> {
    control
        .published()
        .filter(|position| position.generation() == generation)
        .or_else(|| {
            control
                .candidate()
                .filter(|position| position.generation() == generation)
        })
}

fn is_exact_successor(frontier: FrontierPosition, sequence: CommitSequence) -> bool {
    match frontier {
        FrontierPosition::BeforeFirst => sequence == CommitSequence::first(),
        FrontierPosition::AppliedThrough(previous) => previous.checked_next() == Some(sequence),
    }
}

fn find_unique_control<'a>(
    state: &'a MemoryState,
    identity: &ProjectionIdentity,
) -> Option<&'a StoredProjectionControlV1> {
    let range = control_identity_range(state, identity);
    (range.len() == 1).then_some(&state.projection_controls[range.start])
}

fn find_unique_apply<'a>(
    state: &'a MemoryState,
    identity: &ProjectionIdentity,
    generation: ProjectionGeneration,
    sequence: CommitSequence,
) -> Option<&'a riffdb_storage_api::StoredProjectionApplyV1> {
    let key = ProjectionApplyKey::new(identity.clone(), generation, sequence);
    let index = state
        .projection_applies
        .binary_search_by(|marker| marker.key().as_bytes().cmp(key.as_bytes()))
        .ok()?;
    let duplicate_before = index > 0 && state.projection_applies[index - 1].key() == &key;
    let duplicate_after = index + 1 < state.projection_applies.len()
        && state.projection_applies[index + 1].key() == &key;
    (!duplicate_before && !duplicate_after).then_some(&state.projection_applies[index])
}

fn control_identity_range(state: &MemoryState, identity: &ProjectionIdentity) -> Range<usize> {
    let start = state
        .projection_controls
        .partition_point(|control| compare_identity_bytes(control.identity(), identity).is_lt());
    let end = state
        .projection_controls
        .partition_point(|control| !compare_identity_bytes(control.identity(), identity).is_gt());
    start..end
}

fn apply_namespace_range(
    state: &MemoryState,
    identity: &ProjectionIdentity,
    generation: ProjectionGeneration,
) -> Range<usize> {
    let start = state.projection_applies.partition_point(|marker| {
        compare_projection_namespace(
            marker.key().identity(),
            marker.key().generation(),
            identity,
            generation,
        )
        .is_lt()
    });
    let end = state.projection_applies.partition_point(|marker| {
        !compare_projection_namespace(
            marker.key().identity(),
            marker.key().generation(),
            identity,
            generation,
        )
        .is_gt()
    });
    start..end
}

fn state_namespace_range(
    state: &MemoryState,
    identity: &ProjectionIdentity,
    generation: ProjectionGeneration,
) -> Range<usize> {
    let start = state.projection_states.partition_point(|row| {
        compare_projection_namespace(row.identity(), row.generation(), identity, generation).is_lt()
    });
    let end = state.projection_states.partition_point(|row| {
        !compare_projection_namespace(row.identity(), row.generation(), identity, generation)
            .is_gt()
    });
    start..end
}

fn compare_projection_namespace(
    left_identity: &ProjectionIdentity,
    left_generation: ProjectionGeneration,
    right_identity: &ProjectionIdentity,
    right_generation: ProjectionGeneration,
) -> Ordering {
    compare_identity_bytes(left_identity, right_identity)
        .then_with(|| left_generation.cmp(&right_generation))
}

// Projection table ordering follows the durable identity bytes. The length
// comparison is significant and deliberately differs from plain string order.
fn compare_identity_bytes(left: &ProjectionIdentity, right: &ProjectionIdentity) -> Ordering {
    left.contract_lineage()
        .as_bytes()
        .len()
        .cmp(&right.contract_lineage().as_bytes().len())
        .then_with(|| {
            left.contract_lineage()
                .as_bytes()
                .cmp(right.contract_lineage().as_bytes())
        })
        .then_with(|| left.projection_id().cmp(&right.projection_id()))
        .then_with(|| left.plan_hash().cmp(&right.plan_hash()))
}

#[cfg(test)]
mod tests {
    use riffdb_storage_api::{
        ProjectionGenerationPosition, ProjectionLifecycleV1, StoredProjectionApplyV1,
        StoredProjectionControlV1,
    };
    use riffdb_types::{
        CommitSequence, ContractLineage, FrontierPosition, ProjectionApplyHash, ProjectionApplyKey,
        ProjectionId, ProjectionPlanHash,
    };

    use super::*;

    fn identity(lineage: &str) -> ProjectionIdentity {
        ProjectionIdentity::new(
            ContractLineage::new(lineage).expect("lineage"),
            ProjectionId::try_from(1).expect("projection ID"),
            ProjectionPlanHash::from_bytes([7; 32]),
        )
    }

    #[test]
    fn initial_before_first_control_is_structurally_empty() {
        let mut state = MemoryState::default();
        state
            .projection_controls
            .push(StoredProjectionControlV1::initial(identity("budget")));

        assert_eq!(inspect_projection_control(&state, 0), None);
    }

    #[test]
    fn before_first_control_rejects_any_marker() {
        let mut state = MemoryState::default();
        let identity = identity("budget");
        state
            .projection_controls
            .push(StoredProjectionControlV1::initial(identity.clone()));
        state.projection_applies.push(StoredProjectionApplyV1::new(
            ProjectionApplyKey::new(
                identity,
                ProjectionGeneration::first(),
                CommitSequence::first(),
            ),
            ProjectionApplyHash::from_bytes([9; 32]),
        ));

        assert_eq!(
            inspect_projection_control(&state, 0),
            Some(projection_finding(
                StructuralFindingCode::ProjectionStateMismatch
            ))
        );
        assert_eq!(
            inspect_projection_apply(&state, 0),
            Some(projection_finding(
                StructuralFindingCode::ProjectionStateMismatch
            ))
        );
    }

    #[test]
    fn control_order_uses_length_framed_identity_bytes() {
        let mut state = MemoryState::default();
        state
            .projection_controls
            .push(StoredProjectionControlV1::initial(identity("b")));
        state
            .projection_controls
            .push(StoredProjectionControlV1::initial(identity("aa")));

        assert_eq!(inspect_projection_control(&state, 0), None);
        assert_eq!(inspect_projection_control(&state, 1), None);
    }

    #[test]
    fn retained_frontier_requires_a_complete_marker_prefix() {
        let mut state = MemoryState::default();
        let identity = identity("budget");
        state.projection_controls.push(
            StoredProjectionControlV1::new(
                identity,
                ProjectionGeneration::first(),
                Some(ProjectionGenerationPosition::new(
                    ProjectionGeneration::first(),
                    FrontierPosition::AppliedThrough(CommitSequence::first()),
                )),
                None,
                Some(PublishedApplyModeV1::Enabled),
                ProjectionLifecycleV1::Ready,
                None,
            )
            .expect("ready control"),
        );

        assert_eq!(
            inspect_projection_control(&state, 0),
            Some(projection_finding(
                StructuralFindingCode::ProjectionStateMismatch
            ))
        );
    }
}
