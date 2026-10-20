// req: OQ-040, TXN-042, STO-032
use super::*;
use riffdb_types::exact_provider_budget_v1::{PhysicalOwnershipFamily, ProviderBudget};

fn admit_family(family: &PhysicalOwnershipFamily, budget: &ProviderBudget) {
    let mut family = family.lock().unwrap();
    let needed = family.provider_ownership_needed(budget).unwrap();
    let mut work = budget.reserve(needed.checked_add(1).unwrap()).unwrap();
    family
        .prepare_provider_ownership(&mut work, budget)
        .unwrap()
        .commit();
}

#[test]
fn overlay_path_copies_share_payload_owners_and_release_after_last_physical_reference() {
    let base = tests::Base::default();
    let mut builder = CompositeOverlayBuilder::new(tests::checkpoint());
    builder
        .apply_frame(
            &tests::frame(vec![
                CompositeMutationV1::put(
                    CompositeTableV1::Entities,
                    b"a".as_slice(),
                    vec![1; 8192],
                )
                .unwrap(),
                CompositeMutationV1::put(
                    CompositeTableV1::Entities,
                    b"z".as_slice(),
                    vec![2; 8192],
                )
                .unwrap(),
            ]),
            &base,
        )
        .unwrap();
    let original = builder.freeze();
    let family = (original.provider_ownership_family().unwrap()).clone();
    let budget = ProviderBudget::new();
    admit_family(&family, &budget);
    let before = budget.observation().unwrap().owned_bytes;
    let mut successor = CompositeOverlayBuilder::from_published(&original);
    successor
        .apply_frame(
            &tests::second_frame(vec![
                CompositeMutationV1::put(
                    CompositeTableV1::Entities,
                    b"m".as_slice(),
                    vec![3; 1024],
                )
                .unwrap(),
            ]),
            &base,
        )
        .unwrap();
    let successor = successor.freeze();
    assert!(family.same_family(successor.provider_ownership_family().unwrap()));
    let table = CompositeTableV1::Entities.index();
    for key in [b"a".as_slice(), b"z".as_slice()] {
        assert!(
            original.tables[table]
                .key(key)
                .unwrap()
                .0
                .same_allocation(&successor.tables[table].key(key).unwrap().0)
        );
        let (Some(OverlayValue::Value(old)), Some(OverlayValue::Value(new))) = (
            original.tables[table].get(key),
            successor.tables[table].get(key),
        ) else {
            panic!("retained values");
        };
        assert!(old.0.same_allocation(&new.0));
    }
    admit_family(&family, &budget);
    let after = budget.observation().unwrap().owned_bytes;
    assert!(after > before);
    assert!(
        after - before < 8192,
        "a new small row must not recharge the two old buffers"
    );
    drop(original);
    assert_eq!(
        successor.lookup(CompositeTableV1::Entities, b"a".as_slice()),
        OverlayLookup::Value(&[1; 8192])
    );
    assert_eq!(
        successor.lookup(CompositeTableV1::Entities, b"z".as_slice()),
        OverlayLookup::Value(&[2; 8192])
    );
    assert!(budget.observation().unwrap().owned_bytes >= 2 * 8192 + 1024);
    drop(successor);
    assert_eq!(family.observation().unwrap().covered, 0);
    assert!(
        budget.observation().unwrap().owned_bytes > 0,
        "live family control remains charged"
    );
    drop(family);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

#[test]
fn rebased_overlay_keeps_original_byte_owners_with_distinct_new_node_owners() {
    let base = tests::Base::default();
    let mut builder = CompositeOverlayBuilder::new(tests::checkpoint());
    builder
        .apply_frame(
            &tests::frame(vec![
                CompositeMutationV1::put(
                    CompositeTableV1::Entities,
                    b"a".as_slice(),
                    b"first".as_slice(),
                )
                .unwrap(),
            ]),
            &base,
        )
        .unwrap();
    let covered = builder.freeze();
    let mut builder = CompositeOverlayBuilder::from_published(&covered);
    builder
        .apply_frame(
            &tests::second_frame(vec![
                CompositeMutationV1::put(
                    CompositeTableV1::Entities,
                    b"b".as_slice(),
                    b"second".as_slice(),
                )
                .unwrap(),
            ]),
            &base,
        )
        .unwrap();
    let original = builder.freeze();
    let checkpoint = CompositeCheckpointV1::new(
        tests::database_id(),
        1,
        SchemaHash::from_bytes([3; 32]),
        covered.published_application(),
        covered.published_administration(),
        covered.terminal_frame_hash(),
    )
    .unwrap();
    let rebased = original.rebase_after(&covered, checkpoint).unwrap();
    let table = CompositeTableV1::Entities.index();
    let old = original.tables[table].key(b"b".as_slice()).unwrap();
    let new = rebased.tables[table].key(b"b".as_slice()).unwrap();
    assert!(old.0.same_allocation(&new.0));
    assert!(
        !original.tables[table]
            .root
            .as_ref()
            .unwrap()
            .same_allocation(rebased.tables[table].root.as_ref().unwrap())
    );
    let family = (original.provider_ownership_family().unwrap()).clone();
    let budget = ProviderBudget::new();
    admit_family(&family, &budget);
    drop(original);
    drop(covered);
    assert_eq!(
        rebased.lookup(CompositeTableV1::Entities, b"a".as_slice()),
        OverlayLookup::Unchanged
    );
    assert_eq!(
        rebased.lookup(CompositeTableV1::Entities, b"b".as_slice()),
        OverlayLookup::Value(b"second".as_slice())
    );
    assert!(budget.observation().unwrap().owned_bytes >= b"second".as_slice().len() as u64);
    drop(rebased);
    drop(family);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

#[test]
fn overlay_metadata_capacity_refuses_before_allocating_a_family_or_changing_stage() {
    let base = tests::Base::default();
    let original = CompositeOverlayBuilder::new(tests::checkpoint()).freeze();
    let mut stage = CompositeMutationStage::new(&original);
    stage.charged_bytes = MAX_COMPOSITE_OVERLAY_BYTES;
    assert_eq!(
        stage.apply(
            CompositeMutationV1::put(
                CompositeTableV1::Entities,
                b"row".as_slice(),
                b"value".as_slice()
            )
            .unwrap(),
            &base
        ),
        Err(StorageValueError::LimitExceeded)
    );
    assert!(stage.tables[0].family.is_none());
    assert_eq!(stage.mutation_count(), 0);
    assert_eq!(
        stage
            .resolve_point(&base, CompositeTableV1::Entities, b"row".as_slice())
            .unwrap(),
        None
    );
    stage.charged_bytes = 0;
    stage
        .apply(
            CompositeMutationV1::put(
                CompositeTableV1::Entities,
                b"row".as_slice(),
                b"value".as_slice(),
            )
            .unwrap(),
            &base,
        )
        .unwrap();
    assert_eq!(
        stage
            .resolve_point(&base, CompositeTableV1::Entities, b"row".as_slice())
            .unwrap(),
        Some(b"value".as_slice().to_vec())
    );
}

#[test]
fn overlay_node_schedule_is_bounded_and_unexpected_exhaustion_refuses_the_family() {
    let family = PhysicalOwnershipFamily::new(OVERLAY_FAMILY_CONTROL_BYTES as u64);
    assert!(OverlayNodeAdmission::try_new(&family, MAX_OVERLAY_HEIGHT + 1).is_err());
    assert!(family.observation().unwrap().enabled);
    let mut plan = OverlayNodeAdmission::try_new(&family, MAX_OVERLAY_HEIGHT).unwrap();
    let count = 4 * MAX_OVERLAY_HEIGHT + 8;
    for _ in 0..count {
        drop(plan.take().unwrap());
    }
    assert!(plan.take().is_err());
    assert!(!family.observation().unwrap().enabled);
    let budget = ProviderBudget::new();
    assert!(
        family
            .lock()
            .unwrap()
            .provider_ownership_needed(&budget)
            .is_err()
    );
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

#[test]
#[expect(
    clippy::mutable_key_type,
    reason = "overlay ordering uses only immutable bytes; interior mutability tracks allocation ownership"
)]
fn sorted_overlay_rebuild_refuses_foreign_key_or_value_owners_without_binding() {
    let first = PhysicalOwnershipFamily::new(OVERLAY_FAMILY_CONTROL_BYTES as u64);
    let foreign = PhysicalOwnershipFamily::new(OVERLAY_FAMILY_CONTROL_BYTES as u64);
    for foreign_key in [false, true] {
        let key_family = if foreign_key { &foreign } else { &first };
        let value_family = if foreign_key { &first } else { &foreign };
        let key = OverlayBytes::try_new(b"key", key_family).unwrap();
        let value = OverlayValue::Value(OverlayBytes::try_new(b"value", value_family).unwrap());
        let entries = BTreeMap::from([(key, value)]);
        assert_eq!(
            PersistentOverlayMap::from_sorted_entries(entries, first.clone()).err(),
            Some(StorageValueError::IdentityMismatch)
        );
        assert_eq!(first.observation().unwrap().uncovered, 0);
        assert_eq!(foreign.observation().unwrap().uncovered, 0);
        assert!(first.observation().unwrap().enabled);
        assert!(foreign.observation().unwrap().enabled);
    }
    let budget = ProviderBudget::new();
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}
