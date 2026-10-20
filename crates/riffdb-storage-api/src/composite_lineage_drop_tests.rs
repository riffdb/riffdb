//! The supported lineage depth must retire on the production maintenance stack.
use super::*;
use std::sync::{Arc, Barrier};

fn lineage() -> (
    riffdb_types::OwnedCapacitySharedV1<OverlayLineageNode>,
    riffdb_types::exact_provider_budget_v1::PhysicalOwnershipFamily,
) {
    let family = riffdb_types::exact_provider_budget_v1::PhysicalOwnershipFamily::new(
        OVERLAY_FAMILY_CONTROL_BYTES as u64,
    );
    let first = riffdb_types::OwnedCapacitySharedV1::new(OverlayLineageNode {
        predecessor: None,
        keys: vec![(CompositeTableV1::Meta, 0_u64.to_be_bytes().into())].into_boxed_slice(),
        _ownership: family
            .register_member(lineage_key_charge(&[0; 8]).unwrap() as u64)
            .unwrap(),
    });
    let mut head = first;
    for index in 1..MAX_COMPOSITE_OVERLAY_TRANSITIONS {
        head = riffdb_types::OwnedCapacitySharedV1::new(OverlayLineageNode {
            predecessor: Some(head),
            keys: vec![(CompositeTableV1::Meta, (index as u64).to_be_bytes().into())]
                .into_boxed_slice(),
            _ownership: family
                .register_member(lineage_key_charge(&[0; 8]).unwrap() as u64)
                .unwrap(),
        });
    }
    assert!(
        MAX_COMPOSITE_OVERLAY_TRANSITIONS * lineage_key_charge(&[0; 8]).unwrap()
            < MAX_COMPOSITE_OVERLAY_BYTES
    );
    (head, family)
}

// req: END-008
#[test]
fn supported_lineage_retirement_preserves_shared_pins_without_recursive_stack_growth() {
    const CHILD: &str = "RIFFDB_OVERLAY_LINEAGE_DROP_CHILD";
    if std::env::var_os(CHILD).is_none() {
        // Stack overflow aborts rather than unwinds. Isolate the actual node
        // destructor so a regression reports failure without killing CI.
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "composite_view::lineage_drop_tests::supported_lineage_retirement_preserves_shared_pins_without_recursive_stack_growth",
                "--test-threads=1",
            ])
            .env(CHILD, "1")
            .status()
            .unwrap();
        assert!(status.success(), "supported lineage retirement aborted");
        return;
    }
    std::thread::Builder::new()
        .name("overlay-lineage-retirement".into())
        .stack_size(384 * 1024)
        .spawn(|| {
            let (head, family) = lineage();
            let before = family.observation().unwrap().uncovered;
            let retained = (head.predecessor.as_ref().unwrap()).clone();
            let expected = retained.keys[0].1.clone();
            drop(head);
            assert_eq!(
                family.observation().unwrap().uncovered,
                before - lineage_key_charge(&[0; 8]).unwrap() as u64
            );
            let mut oldest = &*retained;
            while let Some(predecessor) = oldest.predecessor.as_ref() {
                oldest = predecessor;
            }
            assert_eq!(oldest.keys[0].1.as_ref(), 0_u64.to_be_bytes());
            assert_eq!(retained.keys[0].1, expected);
            assert!(retained.predecessor.is_some());
            drop(retained);
            assert_eq!(family.observation().unwrap().uncovered, 0);
        })
        .unwrap()
        .join()
        .unwrap();

    let (head, family) = lineage();
    // Distinct successors make both destructors release their last references
    // to the shared predecessor, exercising the into_inner ownership race.
    let owners = [head.clone(), head].map(|predecessor| {
        let ownership = predecessor
            ._ownership
            .family()
            .register_member(std::mem::size_of::<OverlayLineageNode>() as u64 + 32)
            .unwrap();
        riffdb_types::OwnedCapacitySharedV1::new(OverlayLineageNode {
            predecessor: Some(predecessor),
            keys: Box::new([]),
            _ownership: ownership,
        })
    });
    let start = Arc::new(Barrier::new(2));
    let owners = owners
        .into_iter()
        .map(|owner| {
            let start = start.clone();
            std::thread::Builder::new()
                .stack_size(384 * 1024)
                .spawn(move || {
                    start.wait();
                    drop(owner);
                })
                .unwrap()
        })
        .collect::<Vec<_>>();
    for owner in owners {
        owner.join().unwrap();
    }
    assert_eq!(family.observation().unwrap().uncovered, 0);
}
