//! Semantic source foundation proofs; no constructed value grants native authority.

use riffdb_types::{
    CommitSequence, ContractLineage, DatabaseId, DualFrontier, HashDomain, LeadershipEpochV1,
    ProjectionCheckpointOrdinal, ProjectionGeneration, ProjectionId, ProjectionIdentity,
    ProjectionPlanHash, ProjectionSourceLocatorV1, hash,
};

use super::*;
use crate::{
    AuthoritativeStateCatalogV2, AuthoritativeTransactionBindingV3, ChangelogHistoryPointV3,
    ChangelogHistoryStateV3, ChangelogLineageV3, ChangelogTransactionSequence, StorageValueError,
};

fn identity(plan: u8) -> ProjectionIdentity {
    ProjectionIdentity::new(
        ContractLineage::new("budget").unwrap(),
        ProjectionId::first(),
        ProjectionPlanHash::from_bytes([plan; 32]),
    )
}

fn sequence(value: u64) -> CommitSequence {
    CommitSequence::new(value).unwrap()
}

fn database_id(seed: u8) -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(42, [seed; 10]).unwrap()
}

fn parts() -> ProjectionSourceDescriptorPartsV1 {
    let database = database_id(1);
    let lineage = ChangelogLineageV3::new(database, 3, LeadershipEpochV1::new(1).unwrap()).unwrap();
    let point = |position, application, hash| {
        ChangelogHistoryPointV3::new(
            ChangelogTransactionSequence::new(position).unwrap(),
            [hash; 32],
            DualFrontier::new(Some(sequence(application)), None),
        )
    };
    ProjectionSourceDescriptorPartsV1 {
        identity: identity(2),
        locator: ProjectionSourceLocatorV1::new(database, 3, ProjectionCheckpointOrdinal::first())
            .unwrap(),
        applied_through: sequence(10),
        captured_source: ChangelogHistoryStateV3::new(
            lineage,
            point(1, 5, 7),
            point(3, 10, 9),
            point(2, 7, 8),
        )
        .unwrap(),
        origin: hash(
            HashDomain::ProjectionSourceOrigin,
            b"independent origin fixture",
        ),
        prior_source_digest: None,
        row_count: 3,
        canonical_row_bytes: 420,
        row_root: hash(
            HashDomain::ProjectionSourceRows,
            b"independent canonical rows fixture",
        ),
    }
}

// req: STO-012, REC-001
#[test]
fn source_allocator_maximum_is_consumed_and_incarnation_mismatch_cannot_allocate() {
    let database = database_id(1);
    assert_eq!(
        ProjectionCheckpointOrdinalAllocatorV1::initial(0),
        Err(StorageValueError::InvalidShape)
    );
    let first = ProjectionCheckpointOrdinalAllocatorV1::initial(3).unwrap();
    let assigned = first.allocate_one(database, 3).unwrap();
    assert_eq!(
        assigned.locator().ordinal(),
        ProjectionCheckpointOrdinal::first()
    );
    assert_eq!(assigned.locator().allocation_history_incarnation(), 3);
    assert_eq!(
        assigned.next().allocate_one(database, 4),
        Err(ProjectionSourceAllocationErrorV1::IncarnationMismatch)
    );
    let maximum = ProjectionCheckpointOrdinalAllocatorV1::new(
        3,
        ProjectionCheckpointOrdinalPositionV1::Next(
            ProjectionCheckpointOrdinal::new(u64::MAX).unwrap(),
        ),
    )
    .unwrap();
    let last = maximum.allocate_one(database, 3).unwrap();
    assert_eq!(last.locator().ordinal().get(), u64::MAX);
    assert_eq!(
        last.next().position(),
        ProjectionCheckpointOrdinalPositionV1::Exhausted
    );
    assert_eq!(
        last.next().allocate_one(database, 3),
        Err(ProjectionSourceAllocationErrorV1::Exhausted)
    );
    let new_incarnation = ProjectionCheckpointOrdinalAllocatorV1::initial(4)
        .unwrap()
        .allocate_one(database, 4)
        .unwrap();
    assert_eq!(
        new_incarnation.locator().ordinal(),
        ProjectionCheckpointOrdinal::first()
    );
    assert_ne!(new_incarnation.locator(), assigned.locator());
    assert_eq!(last.locator().allocation_history_incarnation(), 3);
}

// req: PRJ-004, REC-003
#[test]
fn source_lifecycle_is_one_way_and_never_changes_immutable_descriptor_identity() {
    let descriptor = ProjectionSourceDescriptorV1::new(parts()).unwrap();
    let reference = descriptor.reference().clone();
    let copying = ProjectionSourceObjectV1::new(descriptor, ProjectionSourceLifecycleV1::Copying);
    assert!(
        copying
            .transition(ProjectionSourceLifecycleV1::Superseded)
            .is_err()
    );
    let published = copying
        .transition(ProjectionSourceLifecycleV1::Published)
        .unwrap();
    let superseded = published
        .transition(ProjectionSourceLifecycleV1::Superseded)
        .unwrap();
    let collecting = superseded
        .transition(ProjectionSourceLifecycleV1::Collecting)
        .unwrap();
    let removed = collecting
        .transition(ProjectionSourceLifecycleV1::Removed)
        .unwrap();
    for object in [&copying, &published, &superseded, &collecting, &removed] {
        assert_eq!(object.descriptor().reference(), &reference);
    }
    assert!(
        superseded
            .transition(ProjectionSourceLifecycleV1::Published)
            .is_err()
    );
    assert!(
        removed
            .transition(ProjectionSourceLifecycleV1::Copying)
            .is_err()
    );
    let aborted = copying
        .transition(ProjectionSourceLifecycleV1::Aborting)
        .unwrap();
    assert!(
        aborted
            .transition(ProjectionSourceLifecycleV1::Published)
            .is_err()
    );
    assert!(
        aborted
            .transition(ProjectionSourceLifecycleV1::Removed)
            .is_ok()
    );
    for tag in 1..=6 {
        assert_eq!(
            ProjectionSourceLifecycleV1::from_tag(tag).unwrap().tag(),
            tag
        );
    }
    assert!(ProjectionSourceLifecycleV1::from_tag(0).is_none());
    assert!(ProjectionSourceLifecycleV1::from_tag(7).is_none());
}

// req: PRJ-004, REC-001, REC-003
#[test]
fn superseded_source_retains_only_exact_existing_base_and_cannot_be_new_selection() {
    let descriptor = ProjectionSourceDescriptorV1::new(parts()).unwrap();
    let predecessor = descriptor.captured_source().tail();
    let binding = AuthoritativeTransactionBindingV3 {
        database_id: descriptor.reference().locator().database_id(),
        history_incarnation: descriptor
            .reference()
            .locator()
            .allocation_history_incarnation(),
        predecessor: Some(predecessor.sequence()),
        sequence: predecessor.sequence().checked_next().unwrap(),
        predecessor_frontier: predecessor.frontier(),
        covered_frontier: predecessor.frontier(),
        prior_history_hash: predecessor.history_hash(),
    };
    let root = ProjectionSourceRootV1::new(descriptor.reference().clone(), binding, None).unwrap();
    assert_eq!(
        root.publication_binding().sequence,
        predecessor.sequence().checked_next().unwrap()
    );
    assert_eq!(
        root.publication_binding().prior_history_hash,
        predecessor.history_hash()
    );
    assert_eq!(root.administration_sequence(), None);
    let base = GenerationSourceBaseV1::new(
        ProjectionGeneration::first(),
        descriptor.reference().clone(),
        descriptor.applied_through(),
    );
    let copying = ProjectionSourceObjectV1::new(descriptor, ProjectionSourceLifecycleV1::Copying);
    assert!(!root.matches_published_object(&copying));
    assert!(!base.matches_retained_object(&copying));
    let published = copying
        .transition(ProjectionSourceLifecycleV1::Published)
        .unwrap();
    assert!(root.matches_published_object(&published));
    assert!(base.matches_retained_object(&published));
    let superseded = published
        .transition(ProjectionSourceLifecycleV1::Superseded)
        .unwrap();
    assert!(!root.matches_published_object(&superseded));
    assert!(base.matches_retained_object(&superseded));
    let collecting = superseded
        .transition(ProjectionSourceLifecycleV1::Collecting)
        .unwrap();
    assert!(!base.matches_retained_object(&collecting));
    let wrong_frontier =
        GenerationSourceBaseV1::new(base.generation(), base.reference().clone(), sequence(9));
    assert!(!wrong_frontier.matches_retained_object(&superseded));
    let mut changed = parts();
    changed.identity = identity(3);
    let changed = ProjectionSourceDescriptorV1::new(changed).unwrap();
    let foreign_base =
        GenerationSourceBaseV1::new(base.generation(), changed.reference().clone(), sequence(10));
    assert!(!foreign_base.matches_retained_object(&superseded));
    assert_eq!(
        base.reference().locator(),
        changed.reference().locator(),
        "numeric locator equality alone grants nothing"
    );
    let changed_root =
        ProjectionSourceRootV1::new(changed.reference().clone(), binding, None).unwrap();
    assert!(!changed_root.matches_published_object(&published));
    for wrong_binding in [
        AuthoritativeTransactionBindingV3 {
            database_id: database_id(2),
            ..binding
        },
        AuthoritativeTransactionBindingV3 {
            history_incarnation: 4,
            ..binding
        },
        AuthoritativeTransactionBindingV3 {
            sequence: binding.sequence.checked_next().unwrap(),
            ..binding
        },
    ] {
        assert_eq!(
            ProjectionSourceRootV1::new(root.reference().clone(), wrong_binding, None),
            Err(StorageValueError::IdentityMismatch)
        );
    }
    let audit = riffdb_types::AdministrationSequence::first();
    assert_eq!(
        ProjectionSourceRootV1::new(root.reference().clone(), binding, Some(audit)),
        Err(StorageValueError::InvalidShape)
    );
    let audited = AuthoritativeTransactionBindingV3 {
        covered_frontier: DualFrontier::new(binding.covered_frontier.application(), Some(audit)),
        ..binding
    };
    assert!(ProjectionSourceRootV1::new(root.reference().clone(), audited, Some(audit)).is_ok());
    assert_eq!(
        ProjectionSourceRootV1::new(root.reference().clone(), audited, None),
        Err(StorageValueError::InvalidShape)
    );
}

// req: PRJ-004, REC-003
#[test]
fn source_descriptor_digest_binds_all_immutable_source_fields_and_hash_roles() {
    let original = ProjectionSourceDescriptorV1::new(parts()).unwrap();
    let mutate: [fn(&mut ProjectionSourceDescriptorPartsV1); 8] = [
        |p| p.identity = identity(3),
        |p| {
            p.locator = ProjectionSourceLocatorV1::new(
                p.locator.database_id(),
                3,
                ProjectionCheckpointOrdinal::new(2).unwrap(),
            )
            .unwrap()
        },
        |p| p.applied_through = sequence(9),
        |p| p.origin = hash(HashDomain::ProjectionSourceOrigin, b"different origin"),
        |p| p.prior_source_digest = Some(hash(HashDomain::ProjectionSourceDescriptor, b"prior")),
        |p| p.row_count = 2,
        |p| p.canonical_row_bytes = 421,
        |p| p.row_root = hash(HashDomain::ProjectionSourceRows, b"different rows"),
    ];
    for change in mutate {
        let mut modified = parts();
        change(&mut modified);
        let modified = ProjectionSourceDescriptorV1::new(modified).unwrap();
        assert_ne!(original.reference().digest(), modified.reference().digest());
    }
    // Independently vary every captured receipt root and the two compatible
    // lineage metadata components. These changes keep locator/F valid, so a
    // digest change proves the captured fence itself participates in identity.
    for component in 0..5 {
        let mut modified = parts();
        let old = modified.captured_source;
        let lineage = match component {
            0 => ChangelogLineageV3::new(
                old.lineage().database_id(),
                3,
                LeadershipEpochV1::new(2).unwrap(),
            )
            .unwrap(),
            1 => ChangelogLineageV3::new_with_catalog(
                old.lineage().database_id(),
                3,
                old.lineage().leadership_epoch(),
                AuthoritativeStateCatalogV2.digest(),
            )
            .unwrap(),
            _ => old.lineage(),
        };
        let change_point = |point: ChangelogHistoryPointV3| {
            ChangelogHistoryPointV3::new(point.sequence(), [99; 32], point.frontier())
        };
        modified.captured_source = ChangelogHistoryStateV3::new(
            lineage,
            if component == 2 {
                change_point(old.anchor())
            } else {
                old.anchor()
            },
            if component == 3 {
                change_point(old.tail())
            } else {
                old.tail()
            },
            if component == 4 {
                change_point(old.minimum_resume())
            } else {
                old.minimum_resume()
            },
        )
        .unwrap();
        assert_ne!(
            original.reference().digest(),
            ProjectionSourceDescriptorV1::new(modified)
                .unwrap()
                .reference()
                .digest()
        );
    }
    let mut wrong = parts();
    wrong.origin = wrong.row_root;
    assert_eq!(
        ProjectionSourceDescriptorV1::new(wrong),
        Err(StorageValueError::InvalidShape)
    );
    let mut wrong = parts();
    wrong.row_root = wrong.origin;
    assert_eq!(
        ProjectionSourceDescriptorV1::new(wrong),
        Err(StorageValueError::InvalidShape)
    );
    let mut wrong = parts();
    wrong.applied_through = sequence(11);
    assert_eq!(
        ProjectionSourceDescriptorV1::new(wrong),
        Err(StorageValueError::IdentityMismatch)
    );
    let mut wrong = parts();
    wrong.locator = ProjectionSourceLocatorV1::new(
        wrong.locator.database_id(),
        4,
        ProjectionCheckpointOrdinal::first(),
    )
    .unwrap();
    assert_eq!(
        ProjectionSourceDescriptorV1::new(wrong),
        Err(StorageValueError::IdentityMismatch)
    );
    for changed_lineage in [
        ChangelogLineageV3::new(database_id(2), 3, LeadershipEpochV1::initial()).unwrap(),
        ChangelogLineageV3::new(database_id(1), 4, LeadershipEpochV1::initial()).unwrap(),
    ] {
        let mut wrong = parts();
        let old = wrong.captured_source;
        wrong.captured_source = ChangelogHistoryStateV3::new(
            changed_lineage,
            old.anchor(),
            old.tail(),
            old.minimum_resume(),
        )
        .unwrap();
        assert_eq!(
            ProjectionSourceDescriptorV1::new(wrong),
            Err(StorageValueError::IdentityMismatch)
        );
    }
}

// req: PRJ-004, STO-020
#[test]
fn source_capacity_reserves_full_candidate_without_releasing_selected_or_pinned_bytes() {
    // Every selected root consumes complete nonempty canonical bytes. These
    // structural observations cannot claim existing roots for zero charge.
    assert!(ProjectionSourceTableUsageV1::new(2, 2, 2, None).is_ok());
    assert_eq!(
        ProjectionSourceTableUsageV1::new(2, 1, 2, None),
        Err(StorageValueError::InvalidShape)
    );
    assert_eq!(
        ProjectionSourceTableUsageV1::new(1, 0, 0, None),
        Err(StorageValueError::InvalidShape)
    );
    // The candidate includes the bounded whole object and all three control
    // records. A structurally decoded reservation cannot exceed their sum.
    let maximum_candidate =
        MAX_PROJECTION_SOURCE_OBJECT_BYTES + 3 * MAX_PROJECTION_SOURCE_CONTROL_BYTES as u64;
    assert!(
        ProjectionSourceTableUsageV1::new(0, 0, maximum_candidate, Some(maximum_candidate)).is_ok()
    );
    assert_eq!(
        ProjectionSourceTableUsageV1::new(0, 0, maximum_candidate + 1, Some(maximum_candidate + 1)),
        Err(StorageValueError::InvalidShape)
    );
    let used = MAX_PROJECTION_SOURCE_TABLE_BYTES - 2;
    let usage = ProjectionSourceTableUsageV1::new(1, 4096, used, None).unwrap();
    let admitted = usage.after_candidate_allocation(0, 0, 1, 1).unwrap();
    assert_eq!(admitted.total_bytes(), MAX_PROJECTION_SOURCE_TABLE_BYTES);
    assert_eq!(admitted.selected_bytes(), usage.selected_bytes());
    assert_eq!(admitted.candidate_reserved_bytes(), Some(2));
    assert_eq!(
        usage.after_candidate_allocation(0, 0, 1, 2),
        Err(StorageValueError::LimitExceeded)
    );
    assert_eq!(
        admitted.after_candidate_allocation(0, 0, 1, 1),
        Err(StorageValueError::InvalidShape)
    );
    assert_eq!(
        usage.total_bytes(),
        used,
        "failed pure preflight cannot release or mutate old usage"
    );
    assert_eq!(
        ProjectionSourceTableUsageV1::new(65, 0, 0, None),
        Err(StorageValueError::LimitExceeded)
    );
    assert_eq!(
        ProjectionSourceTableUsageV1::new(
            1,
            MAX_PROJECTION_SELECTED_SOURCE_BYTES + 1,
            MAX_PROJECTION_SOURCE_TABLE_BYTES,
            None
        ),
        Err(StorageValueError::LimitExceeded)
    );
    let empty = ProjectionSourceTableUsageV1::empty();
    assert!(
        empty
            .after_candidate_allocation(
                MAX_PROJECTION_SOURCE_ROWS,
                MAX_PROJECTION_SOURCE_OBJECT_BYTES - 4096,
                4096,
                4096
            )
            .is_ok()
    );
    assert_eq!(
        empty.after_candidate_allocation(
            MAX_PROJECTION_SOURCE_ROWS + 1,
            MAX_PROJECTION_SOURCE_OBJECT_BYTES - 4096,
            4096,
            4096
        ),
        Err(StorageValueError::LimitExceeded)
    );
    assert_eq!(
        empty.after_candidate_allocation(1, MAX_PROJECTION_SOURCE_OBJECT_BYTES - 4095, 4096, 4096),
        Err(StorageValueError::LimitExceeded)
    );
}

// req: PRJ-004, STO-020
#[test]
fn source_worker_memory_and_staging_limits_refuse_independently_at_one_over() {
    let exact = ProjectionSourceWorkingSetV1::new(
        MAX_PROJECTION_SOURCE_WORKING_BYTES,
        MAX_PROJECTION_SOURCE_STAGING_BYTES,
        MAX_PROJECTION_SOURCE_STAGING_ALLOCATED_BYTES,
    )
    .unwrap();
    assert_eq!(exact.memory_bytes(), MAX_PROJECTION_SOURCE_WORKING_BYTES);
    assert_eq!(exact.staging_bytes(), MAX_PROJECTION_SOURCE_STAGING_BYTES);
    assert_eq!(
        exact.allocated_staging_bytes(),
        MAX_PROJECTION_SOURCE_STAGING_ALLOCATED_BYTES
    );
    for (memory, encoded, allocated) in [
        (MAX_PROJECTION_SOURCE_WORKING_BYTES + 1, 0, 0),
        (
            0,
            MAX_PROJECTION_SOURCE_STAGING_BYTES + 1,
            MAX_PROJECTION_SOURCE_STAGING_ALLOCATED_BYTES,
        ),
        (0, 0, MAX_PROJECTION_SOURCE_STAGING_ALLOCATED_BYTES + 1),
    ] {
        assert_eq!(
            ProjectionSourceWorkingSetV1::new(memory, encoded, allocated),
            Err(StorageValueError::LimitExceeded)
        );
    }
    assert_eq!(
        ProjectionSourceWorkingSetV1::new(0, 1, 0),
        Err(StorageValueError::InvalidShape)
    );
}

mod row_maintenance;

// These are structural control laws, not native acquisition, canonical seed,
// Source memory or durable seeding proofs. No mutation operation is added.
// req: PRJ-004, REC-001, REC-003
#[test]
fn source_seed_control_requires_exact_structural_selection_and_unused_candidate() {
    use crate::{ProjectionControlResult, StoredProjectionControlV1};
    use riffdb_types::FrontierPosition;
    let descriptor = ProjectionSourceDescriptorV1::new(parts()).unwrap();
    let predecessor = descriptor.captured_source().tail();
    let selected = ProjectionSourceRootV1::new(
        descriptor.reference().clone(),
        AuthoritativeTransactionBindingV3 {
            database_id: descriptor.reference().locator().database_id(),
            history_incarnation: descriptor
                .reference()
                .locator()
                .allocation_history_incarnation(),
            predecessor: Some(predecessor.sequence()),
            sequence: predecessor.sequence().checked_next().unwrap(),
            predecessor_frontier: predecessor.frontier(),
            covered_frontier: predecessor.frontier(),
            prior_history_hash: predecessor.history_hash(),
        },
        None,
    )
    .unwrap();
    let object = ProjectionSourceObjectV1::new(descriptor, ProjectionSourceLifecycleV1::Published);
    let expected =
        StoredProjectionControlV1::initial(object.descriptor().reference().identity().clone());
    let base = GenerationSourceBaseV1::new(
        ProjectionGeneration::first(),
        object.descriptor().reference().clone(),
        sequence(10),
    );
    let evaluate =
        |current, base: &GenerationSourceBaseV1, object: &ProjectionSourceObjectV1, head| {
            crate::projection::evaluate_projection_source_seed_control(
                current, &expected, &selected, object, base, head,
            )
        };
    let ProjectionControlResult::Updated(seeded) = evaluate(
        Some(&expected),
        &base,
        &object,
        FrontierPosition::AppliedThrough(sequence(10)),
    )
    .unwrap() else {
        panic!("exact structural post-image");
    };
    assert_eq!(seeded.lifecycle(), crate::ProjectionLifecycleV1::CatchingUp);
    assert_eq!(
        seeded.candidate().unwrap().frontier(),
        FrontierPosition::AppliedThrough(sequence(10))
    );
    assert_eq!(seeded.candidate().unwrap().generation(), base.generation());
    assert_eq!(seeded.published(), None);
    assert_eq!(
        expected.candidate().unwrap().frontier(),
        FrontierPosition::BeforeFirst
    );
    assert_eq!(
        evaluate(
            None,
            &base,
            &object,
            FrontierPosition::AppliedThrough(sequence(10))
        )
        .unwrap(),
        ProjectionControlResult::StateChanged
    );
    let superseded = object
        .transition(ProjectionSourceLifecycleV1::Superseded)
        .unwrap();
    assert_eq!(
        evaluate(
            Some(&expected),
            &base,
            &superseded,
            FrontierPosition::AppliedThrough(sequence(10))
        ),
        Err(StorageValueError::InvalidShape)
    );
    assert_eq!(
        evaluate(
            Some(&expected),
            &base,
            &object,
            FrontierPosition::BeforeFirst
        ),
        Err(StorageValueError::InvalidShape)
    );
    let wrong_frontier =
        GenerationSourceBaseV1::new(base.generation(), base.reference().clone(), sequence(9));
    assert_eq!(
        evaluate(
            Some(&expected),
            &wrong_frontier,
            &object,
            FrontierPosition::AppliedThrough(sequence(10))
        ),
        Err(StorageValueError::InvalidShape)
    );
    let wrong_generation = GenerationSourceBaseV1::new(
        ProjectionGeneration::new(2).unwrap(),
        base.reference().clone(),
        sequence(10),
    );
    assert_eq!(
        evaluate(
            Some(&expected),
            &wrong_generation,
            &object,
            FrontierPosition::AppliedThrough(sequence(10))
        ),
        Err(StorageValueError::InvalidShape)
    );
    let mut foreign_parts = parts();
    foreign_parts.identity = identity(3);
    let foreign = ProjectionSourceDescriptorV1::new(foreign_parts).unwrap();
    let foreign_base =
        GenerationSourceBaseV1::new(base.generation(), foreign.reference().clone(), sequence(10));
    assert_eq!(
        foreign_base.reference().locator(),
        base.reference().locator()
    );
    assert_eq!(
        evaluate(
            Some(&expected),
            &foreign_base,
            &object,
            FrontierPosition::AppliedThrough(sequence(10))
        ),
        Err(StorageValueError::IdentityMismatch)
    );
    // A completed or partially advanced candidate cannot be rebound from its
    // public structural DTO, even to an equal numeric frontier.
    assert_eq!(
        crate::projection::evaluate_projection_source_seed_control(
            Some(&seeded),
            &seeded,
            &selected,
            &object,
            &base,
            FrontierPosition::AppliedThrough(sequence(10)),
        ),
        Err(StorageValueError::InvalidShape)
    );
}

// req: PRJ-004, REC-001, REC-003
#[test]
fn source_seed_control_keeps_published_generation_until_complete_suffix_catchup() {
    use crate::{
        ProjectionControlOperation, ProjectionControlResult, ProjectionGenerationPosition,
        ProjectionLifecycleV1, PublishedApplyModeV1, StoredProjectionControlV1,
        evaluate_projection_control_operation,
    };
    use riffdb_types::FrontierPosition;
    let descriptor = ProjectionSourceDescriptorV1::new(parts()).unwrap();
    let predecessor = descriptor.captured_source().tail();
    let selected = ProjectionSourceRootV1::new(
        descriptor.reference().clone(),
        AuthoritativeTransactionBindingV3 {
            database_id: descriptor.reference().locator().database_id(),
            history_incarnation: descriptor
                .reference()
                .locator()
                .allocation_history_incarnation(),
            predecessor: Some(predecessor.sequence()),
            sequence: predecessor.sequence().checked_next().unwrap(),
            predecessor_frontier: predecessor.frontier(),
            covered_frontier: predecessor.frontier(),
            prior_history_hash: predecessor.history_hash(),
        },
        None,
    )
    .unwrap();
    let object = ProjectionSourceObjectV1::new(descriptor, ProjectionSourceLifecycleV1::Published);
    let published = StoredProjectionControlV1::new(
        object.descriptor().reference().identity().clone(),
        ProjectionGeneration::first(),
        Some(ProjectionGenerationPosition::new(
            ProjectionGeneration::first(),
            FrontierPosition::AppliedThrough(sequence(20)),
        )),
        None,
        Some(PublishedApplyModeV1::Enabled),
        ProjectionLifecycleV1::Ready,
        None,
    )
    .unwrap();
    let head = FrontierPosition::AppliedThrough(sequence(20));
    let ProjectionControlResult::Updated(expected) = evaluate_projection_control_operation(
        Some(&published),
        &ProjectionControlOperation::AllocateRebuild {
            expected: published.clone(),
        },
        head,
    )
    .unwrap() else {
        panic!("real existing generation allocation transition");
    };
    let base = GenerationSourceBaseV1::new(
        expected.candidate().unwrap().generation(),
        object.descriptor().reference().clone(),
        sequence(10),
    );
    let ProjectionControlResult::Updated(seeded) =
        crate::projection::evaluate_projection_source_seed_control(
            Some(&expected),
            &expected,
            &selected,
            &object,
            &base,
            head,
        )
        .unwrap()
    else {
        panic!("exact structural seed post-image");
    };
    assert_eq!(seeded.lifecycle(), ProjectionLifecycleV1::Rebuilding);
    assert_eq!(seeded.published(), published.published());
    assert_eq!(
        seeded.published_apply_mode(),
        published.published_apply_mode()
    );
    assert_eq!(
        seeded.highest_allocated_generation(),
        expected.highest_allocated_generation()
    );
    assert_eq!(
        seeded.candidate().unwrap().frontier(),
        FrontierPosition::AppliedThrough(sequence(10))
    );
    assert_eq!(
        evaluate_projection_control_operation(
            Some(&seeded),
            &ProjectionControlOperation::PublishCandidate {
                expected: seeded.clone()
            },
            head,
        ),
        Err(StorageValueError::InvalidShape),
        "seed completion cannot publish below the authoritative or old published frontier"
    );
}
