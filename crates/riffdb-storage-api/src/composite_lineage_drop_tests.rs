//! The supported lineage depth must retire on the production maintenance stack.
use super::*;
use std::sync::{Barrier, Weak};

fn lineage() -> (Arc<OverlayLineageNode>, Weak<OverlayLineageNode>) {
    let first = Arc::new(OverlayLineageNode {
        predecessor: None,
        keys: vec![(CompositeTableV1::Meta, 0_u64.to_be_bytes().into())].into_boxed_slice(),
    });
    let oldest = Arc::downgrade(&first);
    let mut head = first;
    for index in 1..MAX_COMPOSITE_OVERLAY_TRANSITIONS {
        head = Arc::new(OverlayLineageNode {
            predecessor: Some(head),
            keys: vec![(CompositeTableV1::Meta, (index as u64).to_be_bytes().into())]
                .into_boxed_slice(),
        });
    }
    assert!(
        MAX_COMPOSITE_OVERLAY_TRANSITIONS * lineage_key_charge(&[0; 8]).unwrap()
            < MAX_COMPOSITE_OVERLAY_BYTES
    );
    (head, oldest)
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
            let (head, oldest) = lineage();
            let newest = Arc::downgrade(&head);
            let retained = Arc::clone(head.predecessor.as_ref().unwrap());
            let expected = retained.keys[0].1.clone();
            drop(head);
            assert!(newest.upgrade().is_none());
            assert!(oldest.upgrade().is_some());
            assert_eq!(retained.keys[0].1, expected);
            assert!(retained.predecessor.is_some());
            drop(retained);
            assert!(oldest.upgrade().is_none());
        })
        .unwrap()
        .join()
        .unwrap();

    let (head, oldest) = lineage();
    // Distinct successors make both destructors release their last references
    // to the shared predecessor, exercising the into_inner ownership race.
    let owners = [Arc::clone(&head), head].map(|predecessor| {
        Arc::new(OverlayLineageNode {
            predecessor: Some(predecessor),
            keys: Box::new([]),
        })
    });
    let start = Arc::new(Barrier::new(2));
    let owners = owners
        .into_iter()
        .map(|owner| {
            let start = Arc::clone(&start);
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
    assert!(oldest.upgrade().is_none());
}
