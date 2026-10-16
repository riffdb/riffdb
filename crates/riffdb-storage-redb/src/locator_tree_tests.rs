// req: TXN-042, STO-032, REC-001
use super::*;
use std::collections::BTreeMap;

fn drain(pool: &NativeLocatorPool) {
    while pool.drain_retired_step() {
        std::thread::yield_now();
    }
}
fn assert_avl(link: &Link<u64, u64>) -> (usize, u8, Option<u64>, Option<u64>) {
    let Some(node) = link else {
        return (0, 0, None, None);
    };
    let left = assert_avl(&node.left);
    let right = assert_avl(&node.right);
    assert!(left.3.is_none_or(|key| key < node.key));
    assert!(right.2.is_none_or(|key| key > node.key));
    assert!((i16::from(left.1) - i16::from(right.1)).abs() <= 1);
    assert_eq!(node.height, 1 + left.1.max(right.1));
    (
        left.0 + right.0 + 1,
        node.height,
        left.2.or(Some(node.key)),
        right.3.or(Some(node.key)),
    )
}
fn assert_matches(tree: &LocatorTree<u64, u64>, expected: &BTreeMap<u64, u64>) {
    assert_eq!(tree.len(), expected.len());
    assert_eq!(tree.is_empty(), expected.is_empty());
    assert_eq!(
        tree.iter()
            .map(|(key, value)| (*key, *value))
            .collect::<Vec<_>>(),
        expected
            .iter()
            .map(|(key, value)| (*key, *value))
            .collect::<Vec<_>>()
    );
    assert_eq!(assert_avl(&tree.root).0, expected.len());
    for key in 0..257 {
        assert_eq!(tree.get(&key), expected.get(&key));
        assert_eq!(tree.predecessor(&key), expected.range(..=key).next_back());
        assert_eq!(tree.iter_from(&key).next(), expected.range(key..).next());
    }
    assert_eq!(tree.first(), expected.first_key_value());
    assert_eq!(tree.last(), expected.last_key_value());
}

#[test]
fn locator_tree_rotations_replacement_and_deletion_preserve_exact_order() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let mut tree = LocatorTree::empty(Arc::clone(&pool), LocatorAllocationClass::Ordinary);
    let mut expected = BTreeMap::new();
    // This permutation exercises both double rotations as well as ascending
    // and descending paths, without randomness or scheduling assumptions.
    for ordinal in 0..257 {
        let key = (ordinal * 113) % 257;
        tree = tree.try_insert(key, ordinal).expect("insert");
        expected.insert(key, ordinal);
        assert_matches(&tree, &expected);
        drain(&pool);
    }
    for key in (0..257).step_by(3) {
        tree = tree.try_insert(key, key + 1000).expect("replacement");
        expected.insert(key, key + 1000);
        assert_matches(&tree, &expected);
        drain(&pool);
    }
    for ordinal in 0..257 {
        let key = (ordinal * 71) % 257;
        tree = tree.try_remove(&key).expect("delete");
        expected.remove(&key);
        assert_matches(&tree, &expected);
        drain(&pool);
    }
    let empty = tree.try_remove(&999).expect("missing delete");
    assert!(empty.is_empty());
    drop(empty);
    drop(tree);
    drain(&pool);
}

#[test]
fn locator_tree_clones_pin_original_membership_without_allocation_or_tombstones() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let mut tree = LocatorTree::empty(Arc::clone(&pool), LocatorAllocationClass::Ordinary);
    for key in 0..100 {
        tree = tree.try_insert(key, key + 10).expect("insert");
        drain(&pool);
    }
    let before_clone = pool.charged_bytes();
    let original = tree.clone();
    assert_eq!(pool.charged_bytes(), before_clone);
    assert!(ChargedLocatorAllocation::ptr_eq(
        original.root.as_ref().expect("root"),
        tree.root.as_ref().expect("root")
    ));
    for key in 0..100 {
        tree = tree.try_remove(&key).expect("remove");
        drain(&pool);
    }
    assert_eq!(tree.len(), 0);
    assert_eq!(original.len(), 100);
    assert_eq!(original.iter().count(), 100);
    for key in 0..100 {
        assert_eq!(original.get(&key), Some(&(key + 10)));
    }
    let successor = tree.try_insert(50, 999).expect("reuse exact key");
    assert_eq!(successor.get(&50), Some(&999));
    assert_eq!(original.get(&50), Some(&60));
    drop(tree);
    drop(successor);
    drop(original);
    drain(&pool);
}

#[test]
fn locator_tree_failed_successor_preserves_pin_and_maintenance_can_progress() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let original = LocatorTree::empty(Arc::clone(&pool), LocatorAllocationClass::Ordinary)
        .try_insert(1_u64, 10_u64)
        .expect("original");
    let capacity = pool.exhaust_ordinary_for_tests();
    assert!(original.try_insert(2, 20).is_err());
    assert!(original.try_remove(&1).is_err());
    assert!(
        original
            .with_allocation_class(LocatorAllocationClass::Maintenance)
            .try_remove(&1)
            .expect("progress scratch reserve")
            .is_empty()
    );
    assert_eq!(original.get(&1), Some(&10));
    assert_eq!(original.get(&2), None);
    let progress = original
        .with_allocation_class(LocatorAllocationClass::Maintenance)
        .try_insert(2, 20)
        .expect("progress reserve");
    assert_eq!(progress.get(&1), Some(&10));
    assert_eq!(progress.get(&2), Some(&20));
    assert_eq!(original.len(), 1);
    drop(capacity);
    drop(progress);
    drop(original);
    drain(&pool);
}

#[test]
fn locator_tree_borrowed_charged_keys_retain_exact_order_across_generations() {
    use crate::locator_memory::ChargedLocatorKey;
    let pool = NativeLocatorPool::isolated_for_tests();
    let a = ChargedLocatorKey::try_new(&pool, LocatorAllocationClass::Ordinary, b"a").expect("key");
    let c = ChargedLocatorKey::try_new(&pool, LocatorAllocationClass::Ordinary, b"c").expect("key");
    let tree = LocatorTree::empty(Arc::clone(&pool), LocatorAllocationClass::Ordinary)
        .try_insert(a, 1_u64)
        .expect("insert")
        .try_insert(c, 3)
        .expect("insert");
    assert_eq!(tree.get(b"a".as_slice()), Some(&1));
    assert_eq!(
        tree.predecessor(b"b".as_slice())
            .map(|(key, value)| (key.as_ref(), *value)),
        Some((b"a".as_slice(), 1))
    );
    assert_eq!(
        tree.iter_from(b"b".as_slice())
            .map(|(key, value)| (key.as_ref(), *value))
            .collect::<Vec<_>>(),
        vec![(b"c".as_slice(), 3)]
    );
    let successor = tree.try_remove(b"a".as_slice()).expect("remove");
    assert_eq!(successor.get(b"a".as_slice()), None);
    assert_eq!(tree.get(b"a".as_slice()), Some(&1));
    drop(successor);
    drop(tree);
    drain(&pool);
}

#[test]
fn locator_large_tree_retained_roots_share_paths_and_preserve_progress_headroom() {
    const LIVE_KEYS: u64 = 65_536;
    const RETAINED_ROOTS: u64 = 32;
    const CHANGES_PER_ROOT: u64 = 8;
    let pool = NativeLocatorPool::isolated_for_tests();
    let mut current = LocatorTree::empty(Arc::clone(&pool), LocatorAllocationClass::Ordinary);
    for key in 0..LIVE_KEYS {
        current = current
            .try_insert(key, key)
            .expect("admit initial population");
        if key % 256 == 255 {
            drain(&pool);
        }
    }
    drain(&pool);
    let original_bytes = pool.charged_bytes();
    let original = current.clone();
    assert_eq!(
        pool.charged_bytes(),
        original_bytes,
        "capture shares physical nodes"
    );
    let mut retained = Vec::with_capacity(RETAINED_ROOTS as usize);
    let mut maximum_sampled_union = original_bytes;
    for ordinal in 0..RETAINED_ROOTS {
        retained.push(current.clone());
        for offset in 0..CHANGES_PER_ROOT {
            let key = ordinal * CHANGES_PER_ROOT + offset;
            current = current
                .try_insert(key, LIVE_KEYS + key)
                .expect("admit copied path");
            maximum_sampled_union = maximum_sampled_union.max(pool.charged_bytes());
            drain(&pool);
        }
    }
    // A full-directory copy for each protected root would retain at least 32
    // original populations. This measured physical union permits four copies
    // as conservative headroom while proving substantial persistent sharing.
    assert!(maximum_sampled_union < original_bytes * 4);
    for (ordinal, root) in retained.iter().enumerate() {
        assert_eq!(root.len(), LIVE_KEYS as usize);
        for key in 0..RETAINED_ROOTS * CHANGES_PER_ROOT {
            let expected = if key < ordinal as u64 * CHANGES_PER_ROOT {
                LIVE_KEYS + key
            } else {
                key
            };
            assert_eq!(root.get(&key), Some(&expected));
        }
        assert_eq!(root.get(&(LIVE_KEYS - 1)), Some(&(LIVE_KEYS - 1)));
        assert_eq!(root.get(&LIVE_KEYS), None);
    }
    let pressure = pool.exhaust_ordinary_for_tests();
    let pressured_bytes = pool.charged_bytes();
    assert!(current.try_insert(LIVE_KEYS, 7).is_err());
    assert_eq!(
        pool.charged_bytes(),
        pressured_bytes,
        "refusal leaves no partial path"
    );
    let progress = current
        .with_allocation_class(LocatorAllocationClass::Maintenance)
        .try_remove(&0)
        .expect("reserved maintenance path");
    assert_eq!(progress.get(&0), None);
    assert_eq!(original.get(&0), Some(&0));
    assert_eq!(current.get(&0), Some(&LIVE_KEYS));
    assert_eq!(original.iter().count(), LIVE_KEYS as usize);
    drop(progress);
    drop(pressure);
    drop(current);
    drain(&pool);
    assert!(
        pool.charged_bytes() >= original_bytes,
        "old roots still own original nodes"
    );
    drop(original);
    drop(retained);
    let queued_bytes = pool.charged_bytes();
    assert!(
        queued_bytes >= original_bytes,
        "final release retains queued physical charges"
    );
    let mut destruction_steps = 0usize;
    loop {
        destruction_steps += 1;
        if !pool.drain_retired_step() {
            break;
        }
    }
    assert!(
        destruction_steps >= LIVE_KEYS as usize / crate::locator_memory::LOCATOR_RETIRE_QUOTA,
        "one scheduling step cannot destroy the retained population recursively"
    );
    assert!(
        pool.charged_bytes() < original_bytes / 2,
        "only reusable, still-charged descriptor capacity remains"
    );
    println!(
        "locator-retained-tree-capacity-v1 keys={LIVE_KEYS} roots={RETAINED_ROOTS} original_bytes={original_bytes} sampled_union={maximum_sampled_union} queued_bytes={queued_bytes} retirement_steps={destruction_steps} final_descriptor_bytes={}",
        pool.charged_bytes()
    );
}

// req: TXN-042, STO-032, REC-001, OQ-040
#[test]
fn locator_tree_rejects_foreign_physical_keys_and_children_before_publication() {
    use crate::locator_memory::ChargedLocatorKey;
    let infrastructure = NativeLocatorPool::isolated_for_tests();
    let first = infrastructure
        .fresh_source_family(LocatorAllocationClass::Ordinary)
        .unwrap();
    let second = infrastructure
        .fresh_source_family(LocatorAllocationClass::Ordinary)
        .unwrap();
    let foreign_key =
        ChargedLocatorKey::try_new(&second, LocatorAllocationClass::Ordinary, b"foreign").unwrap();
    let original = LocatorTree::empty(Arc::clone(&first), LocatorAllocationClass::Ordinary);
    assert_eq!(
        original.try_insert(foreign_key, 1_u64).unwrap_err().kind(),
        StorageErrorKind::InvariantViolation
    );
    assert!(original.is_empty());
    let foreign = LocatorTree::empty(second, LocatorAllocationClass::Ordinary)
        .try_insert(1_u64, 1_u64)
        .unwrap();
    let local = LocatorTree::empty(first, LocatorAllocationClass::Ordinary);
    assert_eq!(
        local
            .node(2_u64, 2_u64, foreign.root.clone(), None)
            .err()
            .unwrap()
            .kind(),
        StorageErrorKind::InvariantViolation
    );
    assert_eq!(foreign.get(&1), Some(&1));
    drop(foreign);
    drop(local);
    drop(original);
    drain(&infrastructure);
}
