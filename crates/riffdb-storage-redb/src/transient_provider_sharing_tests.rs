//! One physical native allocation has one provider owner across source versions.
// req: OQ-040, TXN-042, STO-032, REC-001
use super::*;
use riffdb_types::exact_provider_budget_v1::{EXACT_PROVIDER_BUDGET_BYTES, ProviderBudget};

fn drain(pool: &NativeLocatorPool) {
    for _ in 0..16_384 {
        if !pool.drain_retired_step() {
            return;
        }
    }
    panic!("the bounded fixture must finish physical retirement");
}

#[test]
fn pruned_provider_generation_admits_only_incremental_physical_ownership() {
    const INCREMENTAL_ROOM: u64 = 64 * 1024;
    let (first, first_bytes) = crate::derived::follower_indexes::segment(
        vec![crate::derived::follower_indexes::capsule(
            CommitSequence::first(),
        )],
        None,
    );
    let (retained, retained_bytes) = crate::derived::follower_indexes::segment(
        (2..=129)
            .map(|sequence| {
                crate::derived::follower_indexes::capsule(CommitSequence::new(sequence).unwrap())
            })
            .collect(),
        Some(first.segment_digest()),
    );
    for (segment, bytes) in [(&first, &first_bytes), (&retained, &retained_bytes)] {
        assert_eq!(
            crate::command_prefix::decode_segment(bytes.as_bytes())
                .unwrap()
                .value(),
            segment,
            "canonical reciprocal command/event/audit graphs validate independently"
        );
    }
    let pool = NativeLocatorPool::isolated_for_tests();
    let mut original =
        LocatorGeneration::try_empty_in_pool(Arc::clone(&pool), LocatorAllocationClass::Ordinary)
            .unwrap();
    original.insert_segment(&first).unwrap();
    original.insert_segment(&retained).unwrap();
    drain(&pool);
    let budget = ProviderBudget::new();
    let original_owner = original.admit_provider_ownership(&budget).unwrap();
    let original_owned = budget.observation().unwrap().owned_bytes;
    let physical_before = pool.charged_bytes();
    let next = original
        .prepare_history_prune(
            &[Arc::new(first.clone())],
            LocatorAllocationClass::Maintenance,
        )
        .unwrap();
    assert!(!next.owner_handle().same_owner(&original.owner_handle()));
    assert!(!next.has_command_at(CommitSequence::first()).unwrap());
    assert!(original.has_command_at(CommitSequence::first()).unwrap());
    drain(&pool);
    let added_physical_bytes = pool.charged_bytes().saturating_sub(physical_before);
    assert!(added_physical_bytes < INCREMENTAL_ROOM as usize);
    assert!(next.retained_ownership_bound().unwrap() > INCREMENTAL_ROOM);

    // This independent provider's unpublished work has a legitimate reserved
    // allowance. Neither provider nor native ceilings are changed for the test.
    let observed = budget.observation().unwrap();
    let other_build = budget
        .reserve(
            EXACT_PROVIDER_BUDGET_BYTES
                - observed.fixed_owner_bytes
                - observed.owned_bytes
                - observed.reserved_bytes
                - INCREMENTAL_ROOM,
        )
        .unwrap();
    let next_owner = next.admit_provider_ownership(&budget).expect(
        "the retained graph is already owned; admit only the new physical paths and descriptor",
    );
    let union_owned = budget.observation().unwrap().owned_bytes;
    assert!(union_owned >= original_owned);
    assert!(union_owned - original_owned <= INCREMENTAL_ROOM);
    assert!(original.has_command_at(CommitSequence::first()).unwrap());
    assert!(
        next.has_command_at(retained.first_commit_sequence())
            .unwrap()
    );
    drop(other_build);

    // Retained manifest entries own these real key buffers and inline entry
    // slots regardless of accounting implementation. This is an independent
    // allocation lower bound, not the provider estimator's formula.
    let retained_storage_floor = retained
        .manifest()
        .entries()
        .iter()
        .map(|entry| {
            entry.exact_key().len()
                + std::mem::size_of::<payload::MetadataManifestEntry>()
                + std::mem::size_of::<(ChargedLocatorKey, CommandDerivedLocator)>()
        })
        .sum::<usize>() as u64;
    assert!(retained_storage_floor > INCREMENTAL_ROOM);
    drop(original_owner);
    drop(original);
    drain(&pool);
    assert!(
        budget.observation().unwrap().owned_bytes >= retained_storage_floor,
        "old-generation retirement cannot uncharge physical allocations shared by the successor"
    );
    for entry in retained.manifest().entries() {
        let (_, locator) = next
            .capture_command_member(entry.kind(), entry.exact_key())
            .unwrap()
            .expect("every original retained exact member survives");
        assert_eq!(locator.command_ordinal, entry.command_ordinal());
        assert_eq!(locator.member_ordinal, entry.member_ordinal());
        assert_eq!(locator.member, entry.member());
    }
    let resolved = next
        .capture_command_at(retained.first_commit_sequence())
        .unwrap()
        .unwrap()
        .resolve(|sequence| {
            assert_eq!(sequence, retained.first_commit_sequence());
            Ok(Some(retained_bytes.as_bytes().to_vec()))
        })
        .unwrap();
    assert_eq!(resolved.segment.as_ref(), &retained);
    drop(resolved);
    drop(next_owner);
    drop(next);
    assert!(
        budget.observation().unwrap().owned_bytes >= retained_storage_floor,
        "final reference release retains charges until physical queued destruction"
    );
    drain(&pool);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}
