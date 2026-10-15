//! Incremental analytical provider structures conformance (ADR-0162, WP-714).
// req: PRJ-001, PRJ-002, PRJ-003, PRJ-004, OQ-017, OQ-018, OQ-019, OQ-020, OQ-021, OQ-022, OQ-023, OQ-024
// req: OQ-044, OQ-045, OQ-046, OQ-047, OQ-048, OQ-049, OQ-050, OQ-051, OQ-052, OQ-053, OQ-054, OQ-055

use std::collections::BTreeMap;
use std::num::{NonZeroU16, NonZeroU32};

use riffdb_projection::{
    ExactMeanV1, IncrementalBitmapPredicateV1, IncrementalDegradedReasonV1,
    IncrementalNoValuePlacementV1, IncrementalPartitionIndexV1, IncrementalPolicyAlignmentV1,
    IncrementalProjectionRegistryV1, IncrementalProviderErrorV1, IncrementalRollupFunctionV1,
    IncrementalRollupValueV1, IncrementalRowMutationV1, IncrementalSortDirectionV1,
    IncrementalStructureDescriptorV1, IncrementalStructureIdV1, IncrementalStructureKindV1,
    IncrementalValueKeyV1, ProviderLifecycleV1, evaluate_bitmap_predicate_reference_v1,
    evaluate_rollup_reference_v1, evaluate_top_k_reference_v1,
};
use riffdb_types::{
    ApplicationRoleHash, CanonicalRecord, CanonicalString, CanonicalValue, CommitSequence,
    EntityKey, EntityKeyBuilder, EntityTypeId, FieldId, PartitionKeyHash, ProjectionGeneration,
    ProjectionProviderDescriptorHash, QueryPlanHash,
};

fn key(id: u32) -> EntityKey {
    let mut b = EntityKeyBuilder::new(EntityTypeId::first());
    b.push_u32(id).unwrap();
    b.finish().unwrap()
}

fn seq(n: u64) -> CommitSequence {
    CommitSequence::new(n).unwrap()
}

fn fid(n: u32) -> FieldId {
    FieldId::new(n).unwrap()
}

fn sid(n: u16) -> IncrementalStructureIdV1 {
    IncrementalStructureIdV1::new(n).unwrap()
}

fn str_val(s: &str) -> CanonicalValue {
    CanonicalValue::String(CanonicalString::new(s).unwrap())
}

fn record_with_fields(
    status: Option<Option<&str>>,
    priority: Option<Option<i64>>,
    urgent: bool,
    policy_visible: bool,
) -> CanonicalRecord {
    let mut fields = Vec::new();
    if let Some(st) = status {
        let v = match st {
            Some(s) => str_val(s),
            None => CanonicalValue::Null,
        };
        fields.push((fid(1), v));
    }
    if let Some(pr) = priority {
        let v = match pr {
            Some(p) => CanonicalValue::I64(p),
            None => CanonicalValue::Null,
        };
        fields.push((fid(2), v));
    }
    fields.push((fid(3), CanonicalValue::Bool(urgent)));
    fields.push((fid(4), CanonicalValue::Bool(policy_visible)));
    CanonicalRecord::new(fields).unwrap()
}

// req: PRJ-001, PRJ-002, PRJ-003, PRJ-004, OQ-044, OQ-045, OQ-046, OQ-047, OQ-048, OQ-049, OQ-050, OQ-051, OQ-052
#[test]
fn incremental_bitmap_rollup_and_top_k_match_independent_reference_oracle() {
    let policy = IncrementalPolicyAlignmentV1::RoleInvariantPublicUniverse;
    let role = ApplicationRoleHash::from_bytes([0x11; 32]);
    let desc_hash = ProjectionProviderDescriptorHash::from_bytes([0x22; 32]);
    let plan_bitmap = QueryPlanHash::from_bytes([0x31; 32]);
    let plan_sum = QueryPlanHash::from_bytes([0x32; 32]);
    let plan_mean = QueryPlanHash::from_bytes([0x33; 32]);
    let plan_min = QueryPlanHash::from_bytes([0x34; 32]);
    let plan_distinct = QueryPlanHash::from_bytes([0x35; 32]);
    let plan_any = QueryPlanHash::from_bytes([0x36; 32]);
    let plan_all = QueryPlanHash::from_bytes([0x37; 32]);
    let plan_topk = QueryPlanHash::from_bytes([0x38; 32]);

    let descriptors = vec![
        IncrementalStructureDescriptorV1::new(
            sid(1),
            desc_hash,
            plan_bitmap,
            policy.clone(),
            IncrementalStructureKindV1::SegmentEqualityPresenceBitmap {
                field_id: fid(1),
                max_distinct_values: NonZeroU16::new(32).unwrap(),
                max_bitmap_bytes: NonZeroU32::new(65_536).unwrap(),
            },
        )
        .unwrap(),
        IncrementalStructureDescriptorV1::new(
            sid(2),
            desc_hash,
            plan_sum,
            policy.clone(),
            IncrementalStructureKindV1::ExactMergeableRollup {
                group_fields: vec![fid(1)],
                function: IncrementalRollupFunctionV1::SumI64 { field_id: fid(2) },
                max_groups: NonZeroU16::new(64).unwrap(),
                max_distinct_per_group: NonZeroU16::new(64).unwrap(),
                max_state_bytes: NonZeroU32::new(65_536).unwrap(),
            },
        )
        .unwrap(),
        IncrementalStructureDescriptorV1::new(
            sid(3),
            desc_hash,
            plan_mean,
            policy.clone(),
            IncrementalStructureKindV1::ExactMergeableRollup {
                group_fields: vec![fid(1)],
                function: IncrementalRollupFunctionV1::MeanI64 { field_id: fid(2) },
                max_groups: NonZeroU16::new(64).unwrap(),
                max_distinct_per_group: NonZeroU16::new(64).unwrap(),
                max_state_bytes: NonZeroU32::new(65_536).unwrap(),
            },
        )
        .unwrap(),
        IncrementalStructureDescriptorV1::new(
            sid(4),
            desc_hash,
            plan_min,
            policy.clone(),
            IncrementalStructureKindV1::ExactMergeableRollup {
                group_fields: vec![fid(1)],
                function: IncrementalRollupFunctionV1::MinI64 { field_id: fid(2) },
                max_groups: NonZeroU16::new(64).unwrap(),
                max_distinct_per_group: NonZeroU16::new(64).unwrap(),
                max_state_bytes: NonZeroU32::new(65_536).unwrap(),
            },
        )
        .unwrap(),
        IncrementalStructureDescriptorV1::new(
            sid(5),
            desc_hash,
            plan_distinct,
            policy.clone(),
            IncrementalStructureKindV1::ExactMergeableRollup {
                group_fields: Vec::new(),
                function: IncrementalRollupFunctionV1::CountDistinct { field_id: fid(1) },
                max_groups: NonZeroU16::new(16).unwrap(),
                max_distinct_per_group: NonZeroU16::new(64).unwrap(),
                max_state_bytes: NonZeroU32::new(65_536).unwrap(),
            },
        )
        .unwrap(),
        IncrementalStructureDescriptorV1::new(
            sid(6),
            desc_hash,
            plan_any,
            policy.clone(),
            IncrementalStructureKindV1::ExactMergeableRollup {
                group_fields: vec![fid(1)],
                function: IncrementalRollupFunctionV1::AnyBool { field_id: fid(3) },
                max_groups: NonZeroU16::new(64).unwrap(),
                max_distinct_per_group: NonZeroU16::new(64).unwrap(),
                max_state_bytes: NonZeroU32::new(65_536).unwrap(),
            },
        )
        .unwrap(),
        IncrementalStructureDescriptorV1::new(
            sid(7),
            desc_hash,
            plan_all,
            policy.clone(),
            IncrementalStructureKindV1::ExactMergeableRollup {
                group_fields: vec![fid(1)],
                function: IncrementalRollupFunctionV1::AllBool { field_id: fid(3) },
                max_groups: NonZeroU16::new(64).unwrap(),
                max_distinct_per_group: NonZeroU16::new(64).unwrap(),
                max_state_bytes: NonZeroU32::new(65_536).unwrap(),
            },
        )
        .unwrap(),
        IncrementalStructureDescriptorV1::new(
            sid(8),
            desc_hash,
            plan_topk,
            policy.clone(),
            IncrementalStructureKindV1::MaintainedTopKLane {
                value_field: fid(2),
                direction: IncrementalSortDirectionV1::Descending,
                no_value_placement: IncrementalNoValuePlacementV1::Last,
                k: NonZeroU16::new(3).unwrap(),
                repair_reserve: NonZeroU16::new(4).unwrap(),
                max_state_bytes: NonZeroU32::new(65_536).unwrap(),
            },
        )
        .unwrap(),
    ];

    let registry = IncrementalProjectionRegistryV1::new(descriptors).unwrap();
    let partition = PartitionKeyHash::from_bytes([0x44; 32]);
    let generation = ProjectionGeneration::first();
    let mut index = IncrementalPartitionIndexV1::new(partition, 1, generation, registry.clone());
    let mut reference_rows: BTreeMap<EntityKey, CanonicalRecord> = BTreeMap::new();

    // Commit 1: initial inserts covering present, null, and missing fields.
    let batch_1 = vec![
        (
            key(1),
            record_with_fields(Some(Some("open")), Some(Some(10)), false, true),
        ),
        (
            key(2),
            record_with_fields(Some(Some("open")), Some(Some(50)), true, true),
        ),
        (
            key(3),
            record_with_fields(Some(Some("closed")), Some(Some(30)), false, true),
        ),
        (
            key(4),
            record_with_fields(Some(None), Some(Some(5)), true, true),
        ),
        (key(5), record_with_fields(None, Some(None), false, true)),
    ];
    let mut mutations_1 = Vec::new();
    for (k, r) in batch_1 {
        reference_rows.insert(k.clone(), r.clone());
        mutations_1.push(IncrementalRowMutationV1::Upsert {
            key: k,
            post_image: r,
        });
    }
    index.apply_commit(seq(1), [1u8; 32], &mutations_1).unwrap();

    // Commit 2: replace current minimum in "open" (key 1: 10 -> 25), delete current top-K leader (key 2: 50),
    // and insert key 6 ("open", priority 40).
    let rec_1_updated = record_with_fields(Some(Some("open")), Some(Some(25)), true, true);
    let rec_6 = record_with_fields(Some(Some("open")), Some(Some(40)), true, true);
    reference_rows.insert(key(1), rec_1_updated.clone());
    reference_rows.remove(&key(2));
    reference_rows.insert(key(6), rec_6.clone());

    let mutations_2 = vec![
        IncrementalRowMutationV1::Upsert {
            key: key(1),
            post_image: rec_1_updated,
        },
        IncrementalRowMutationV1::Delete { key: key(2) },
        IncrementalRowMutationV1::Upsert {
            key: key(6),
            post_image: rec_6,
        },
    ];
    index.apply_commit(seq(2), [2u8; 32], &mutations_2).unwrap();

    // Verify bitmap equality, null, missing, and present sets against reference evaluator.
    for pred in [
        vec![IncrementalBitmapPredicateV1::Equals(str_val("open"))],
        vec![IncrementalBitmapPredicateV1::IsNull],
        vec![IncrementalBitmapPredicateV1::IsMissing],
        vec![
            IncrementalBitmapPredicateV1::IsPresent,
            IncrementalBitmapPredicateV1::Equals(str_val("closed")),
        ],
    ] {
        let actual = index
            .query_bitmap(sid(1), plan_bitmap, role, 1, &pred)
            .unwrap();
        let expected =
            evaluate_bitmap_predicate_reference_v1(&reference_rows, &policy, fid(1), &pred);
        assert_eq!(actual, expected);
    }

    // Verify rollups (SumI64, MeanI64, MinI64 after extreme replacement/deletion, CountDistinct, AnyBool, AllBool).
    assert_eq!(
        index.query_rollup(sid(2), plan_sum, role, 1).unwrap(),
        evaluate_rollup_reference_v1(
            &reference_rows,
            &policy,
            &[fid(1)],
            IncrementalRollupFunctionV1::SumI64 { field_id: fid(2) },
        )
        .unwrap()
    );
    let mean_groups = index.query_rollup(sid(3), plan_mean, role, 1).unwrap();
    assert_eq!(
        mean_groups.get(&vec![Some(IncrementalValueKeyV1(str_val("open")))]),
        Some(&IncrementalRollupValueV1::MeanI64(ExactMeanV1 {
            total: 65,
            count: 2,
        }))
    );
    assert_eq!(
        mean_groups,
        evaluate_rollup_reference_v1(
            &reference_rows,
            &policy,
            &[fid(1)],
            IncrementalRollupFunctionV1::MeanI64 { field_id: fid(2) },
        )
        .unwrap()
    );
    assert_eq!(
        index.query_rollup(sid(4), plan_min, role, 1).unwrap(),
        evaluate_rollup_reference_v1(
            &reference_rows,
            &policy,
            &[fid(1)],
            IncrementalRollupFunctionV1::MinI64 { field_id: fid(2) },
        )
        .unwrap()
    );
    assert_eq!(
        index.query_rollup(sid(5), plan_distinct, role, 1).unwrap(),
        evaluate_rollup_reference_v1(
            &reference_rows,
            &policy,
            &[],
            IncrementalRollupFunctionV1::CountDistinct { field_id: fid(1) },
        )
        .unwrap()
    );
    assert_eq!(
        index.query_rollup(sid(6), plan_any, role, 1).unwrap(),
        evaluate_rollup_reference_v1(
            &reference_rows,
            &policy,
            &[fid(1)],
            IncrementalRollupFunctionV1::AnyBool { field_id: fid(3) },
        )
        .unwrap()
    );
    assert_eq!(
        index.query_rollup(sid(7), plan_all, role, 1).unwrap(),
        evaluate_rollup_reference_v1(
            &reference_rows,
            &policy,
            &[fid(1)],
            IncrementalRollupFunctionV1::AllBool { field_id: fid(3) },
        )
        .unwrap()
    );

    // Verify top-K after leader deletion matches reference evaluator.
    let limit = NonZeroU16::new(3).unwrap();
    let actual_topk = index
        .query_top_k(sid(8), plan_topk, role, 1, limit)
        .unwrap();
    let expected_topk = evaluate_top_k_reference_v1(
        &reference_rows,
        &policy,
        fid(2),
        IncrementalSortDirectionV1::Descending,
        IncrementalNoValuePlacementV1::Last,
        limit,
    );
    assert_eq!(actual_topk, expected_topk);

    // Compaction and full snapshot rebuild must produce identical state and query results.
    index.compact().unwrap();
    let rebuilt = IncrementalPartitionIndexV1::rebuild_from_rows(
        partition,
        1,
        generation,
        seq(2),
        [2u8; 32],
        registry,
        &reference_rows,
    )
    .unwrap();
    assert_eq!(
        index
            .query_top_k(sid(8), plan_topk, role, 1, limit)
            .unwrap(),
        rebuilt
            .query_top_k(sid(8), plan_topk, role, 1, limit)
            .unwrap()
    );
}

// req: PRJ-001, PRJ-002, PRJ-003, OQ-020, OQ-021
#[test]
fn incremental_provider_structures_apply_atomically_and_enforce_policy_alignment() {
    let allowed_role = ApplicationRoleHash::from_bytes([0xAA; 32]);
    let other_role = ApplicationRoleHash::from_bytes([0xBB; 32]);
    let desc_hash = ProjectionProviderDescriptorHash::from_bytes([0x55; 32]);
    let plan_count = QueryPlanHash::from_bytes([0x61; 32]);
    let plan_topk = QueryPlanHash::from_bytes([0x62; 32]);

    let policy_aligned = IncrementalPolicyAlignmentV1::PolicyAlignedSubpartition {
        role_hash: allowed_role,
        capability_revision: 7,
        policy_field: fid(4),
        required_value: CanonicalValue::Bool(true),
    };

    let registry = IncrementalProjectionRegistryV1::new(vec![
        IncrementalStructureDescriptorV1::new(
            sid(1),
            desc_hash,
            plan_count,
            policy_aligned.clone(),
            IncrementalStructureKindV1::ExactMergeableRollup {
                group_fields: Vec::new(),
                function: IncrementalRollupFunctionV1::Count,
                max_groups: NonZeroU16::new(4).unwrap(),
                max_distinct_per_group: NonZeroU16::new(4).unwrap(),
                max_state_bytes: NonZeroU32::new(4_096).unwrap(),
            },
        )
        .unwrap(),
        IncrementalStructureDescriptorV1::new(
            sid(2),
            desc_hash,
            plan_topk,
            policy_aligned,
            IncrementalStructureKindV1::MaintainedTopKLane {
                value_field: fid(2),
                direction: IncrementalSortDirectionV1::Descending,
                no_value_placement: IncrementalNoValuePlacementV1::Last,
                k: NonZeroU16::new(2).unwrap(),
                repair_reserve: NonZeroU16::new(1).unwrap(),
                max_state_bytes: NonZeroU32::new(4_096).unwrap(),
            },
        )
        .unwrap(),
    ])
    .unwrap();

    let mut index = IncrementalPartitionIndexV1::new(
        PartitionKeyHash::from_bytes([0x77; 32]),
        3,
        ProjectionGeneration::first(),
        registry,
    );

    // Apply 2 allowed rows and 2 denied rows (`policy_visible = false` with high priority 999).
    let mutations = vec![
        IncrementalRowMutationV1::Upsert {
            key: key(1),
            post_image: record_with_fields(Some(Some("open")), Some(Some(10)), false, true),
        },
        IncrementalRowMutationV1::Upsert {
            key: key(2),
            post_image: record_with_fields(Some(Some("open")), Some(Some(20)), false, true),
        },
        IncrementalRowMutationV1::Upsert {
            key: key(3),
            post_image: record_with_fields(Some(Some("open")), Some(Some(999)), true, false),
        },
        IncrementalRowMutationV1::Upsert {
            key: key(4),
            post_image: record_with_fields(Some(Some("open")), Some(Some(888)), true, false),
        },
    ];
    index.apply_commit(seq(10), [0x10; 32], &mutations).unwrap();

    // Idempotent replay with identical hash is a no-op; mismatched hash or frontier regression fails closed.
    index.apply_commit(seq(10), [0x10; 32], &mutations).unwrap();
    assert_eq!(
        index.apply_commit(seq(10), [0x99; 32], &mutations),
        Err(IncrementalProviderErrorV1::ApplyHashMismatch { sequence: seq(10) })
    );
    assert_eq!(
        index.apply_commit(seq(9), [0x09; 32], &[]),
        Err(IncrementalProviderErrorV1::FrontierRegression {
            current: seq(10),
            attempted: seq(9),
        })
    );

    // Denied rows never influence count or top-K.
    let count_map = index
        .query_rollup(sid(1), plan_count, allowed_role, 7)
        .unwrap();
    assert_eq!(
        count_map.get(&Vec::new()),
        Some(&IncrementalRollupValueV1::Count(2))
    );
    let topk = index
        .query_top_k(
            sid(2),
            plan_topk,
            allowed_role,
            7,
            NonZeroU16::new(2).unwrap(),
        )
        .unwrap();
    assert_eq!(topk.len(), 2);
    assert_eq!(topk[0].key, key(2));
    assert_eq!(topk[1].key, key(1));

    // Wrong role or stale capability revision is refused before reading state.
    assert_eq!(
        index.query_rollup(sid(1), plan_count, other_role, 7),
        Err(IncrementalProviderErrorV1::PolicyAlignmentMismatch)
    );
    assert_eq!(
        index.query_rollup(sid(1), plan_count, allowed_role, 6),
        Err(IncrementalProviderErrorV1::PolicyAlignmentMismatch)
    );

    // Insert 2 more allowed rows so total admitted population is 4 while capacity (k=2 + reserve=1) is 3,
    // causing 1 entry to be evicted beyond the reserve. Then delete 2 top entries to exhaust the reserve.
    index
        .apply_commit(
            seq(11),
            [0x11; 32],
            &[
                IncrementalRowMutationV1::Upsert {
                    key: key(5),
                    post_image: record_with_fields(Some(Some("open")), Some(Some(30)), false, true),
                },
                IncrementalRowMutationV1::Upsert {
                    key: key(6),
                    post_image: record_with_fields(Some(Some("open")), Some(Some(40)), false, true),
                },
            ],
        )
        .unwrap();
    let degrade_err = index
        .apply_commit(
            seq(12),
            [0x12; 32],
            &[
                IncrementalRowMutationV1::Delete { key: key(6) },
                IncrementalRowMutationV1::Delete { key: key(5) },
            ],
        )
        .unwrap_err();
    assert_eq!(
        degrade_err,
        IncrementalProviderErrorV1::ProviderUnavailable {
            lifecycle: ProviderLifecycleV1::Rebuilding,
            reason: Some(IncrementalDegradedReasonV1::TopKReserveExhausted),
        }
    );
    // Visible frontier did not advance to seq(12), and subsequent queries fail typed with Rebuilding.
    assert_eq!(index.visible_frontier(), Some(seq(11)));
    assert_eq!(index.lifecycle(), ProviderLifecycleV1::Rebuilding);
    assert_eq!(
        index.epoch_observation(sid(2)).unwrap().lifecycle(),
        ProviderLifecycleV1::Rebuilding
    );
}

// req: OQ-017, OQ-018, OQ-019, OQ-022, OQ-023, OQ-024, OQ-053, OQ-054, OQ-055
#[test]
fn incremental_provider_descriptor_and_bounds_refuse_unsupported_or_excessive_plans() {
    let desc_hash = ProjectionProviderDescriptorHash::from_bytes([0x88; 32]);
    let plan_a = QueryPlanHash::from_bytes([0x91; 32]);
    let plan_b = QueryPlanHash::from_bytes([0x92; 32]);
    let policy = IncrementalPolicyAlignmentV1::RoleInvariantPublicUniverse;

    // 1. Excessive top-K + reserve (> 1024) is refused at descriptor construction.
    assert_eq!(
        IncrementalStructureDescriptorV1::new(
            sid(1),
            desc_hash,
            plan_a,
            policy.clone(),
            IncrementalStructureKindV1::MaintainedTopKLane {
                value_field: fid(2),
                direction: IncrementalSortDirectionV1::Ascending,
                no_value_placement: IncrementalNoValuePlacementV1::First,
                k: NonZeroU16::new(600).unwrap(),
                repair_reserve: NonZeroU16::new(600).unwrap(),
                max_state_bytes: NonZeroU32::new(4_096).unwrap(),
            },
        ),
        Err(IncrementalProviderErrorV1::ExcessiveStructureBounds)
    );

    // 2. Duplicate group-by field is refused.
    assert_eq!(
        IncrementalStructureDescriptorV1::new(
            sid(1),
            desc_hash,
            plan_a,
            policy.clone(),
            IncrementalStructureKindV1::ExactMergeableRollup {
                group_fields: vec![fid(1), fid(1)],
                function: IncrementalRollupFunctionV1::Count,
                max_groups: NonZeroU16::new(16).unwrap(),
                max_distinct_per_group: NonZeroU16::new(16).unwrap(),
                max_state_bytes: NonZeroU32::new(4_096).unwrap(),
            },
        ),
        Err(IncrementalProviderErrorV1::DuplicateGroupField(fid(1)))
    );

    // 3. Empty registry is refused.
    assert_eq!(
        IncrementalProjectionRegistryV1::new(Vec::new()),
        Err(IncrementalProviderErrorV1::InvalidStructureCount(0))
    );

    // 4. Implicit cross-provider bitmap/row-ID bridge is refused (OQ-023).
    let d1 = IncrementalStructureDescriptorV1::new(
        sid(1),
        desc_hash,
        plan_a,
        policy.clone(),
        IncrementalStructureKindV1::SegmentEqualityPresenceBitmap {
            field_id: fid(1),
            max_distinct_values: NonZeroU16::new(16).unwrap(),
            max_bitmap_bytes: NonZeroU32::new(4_096).unwrap(),
        },
    )
    .unwrap();
    let d2 = IncrementalStructureDescriptorV1::new(
        sid(2),
        desc_hash,
        plan_b,
        policy,
        IncrementalStructureKindV1::SegmentEqualityPresenceBitmap {
            field_id: fid(2),
            max_distinct_values: NonZeroU16::new(16).unwrap(),
            max_bitmap_bytes: NonZeroU32::new(4_096).unwrap(),
        },
    )
    .unwrap();
    let registry = IncrementalProjectionRegistryV1::new(vec![d1, d2]).unwrap();
    assert_eq!(
        registry.verify_single_structure_plan(sid(1), plan_a, Some(sid(2))),
        Err(IncrementalProviderErrorV1::ImplicitCrossProviderBridgeForbidden)
    );
    assert_eq!(
        registry.verify_single_structure_plan(sid(1), plan_b, None),
        Err(IncrementalProviderErrorV1::PlanIdentityMismatch)
    );
}
