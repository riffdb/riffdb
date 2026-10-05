//! Actual native publication qualification and independent pinned-state oracles.
use super::*;
use crate::changelog_v3_retained_validation::RetainedHistoryValidation;
use riffdb_storage_api::{ChangelogAttributionV3, EvidencePageLimit};

fn append_native(ports: &crate::RedbOperationalPorts, ordinal: u64) {
    let _lease = ports.shared.mutation_gate.acquire().unwrap();
    let write = crate::changelog_v3_write::CapturedImmediateWrite::begin(
        &ports.shared.database,
        crate::RedbCommitProfile::Hardened,
        ChangelogAttributionV3::OutboxTransition,
    )
    .unwrap();
    write
        .open_table(crate::layout::OUTBOX_STATUS)
        .unwrap()
        .insert(
            b"continuation-fixture".as_slice(),
            ordinal.to_be_bytes().as_slice(),
        )
        .unwrap();
    write.finish().unwrap().commit(&ports.shared).unwrap();
}

fn qualified_walk(ports: &crate::RedbOperationalPorts) -> RetainedHistoryValidation {
    let _lease = ports.shared.mutation_gate.acquire().unwrap();
    let epoch = ports
        .shared
        .source_continuation_epoch
        .load(std::sync::atomic::Ordering::Acquire);
    let mut walk = RetainedHistoryValidation::new(ports.shared.capture_checkpoint_root().unwrap())
        .unwrap()
        .unwrap();
    walk.bind_qualification(epoch);
    walk
}

// req: STO-030, STO-031, STO-032, END-008
#[test]
fn native_continuation_reads_one_initial_population_then_only_added_receipts() {
    for population in [7_u64, 300, 600] {
        let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
        for ordinal in 8..=population {
            append_native(&ports, ordinal);
        }
        let mut walk = qualified_walk(&ports);
        while !walk.advance(EvidencePageLimit::new(64).unwrap()).unwrap() {}
        assert_eq!(walk.rows_read(), [population, 0]);
        let old_pin = Arc::downgrade(walk.proof().unwrap().root());
        for ordinal in population + 1..=population + 5 {
            append_native(&ports, ordinal);
        }
        let epoch = ports
            .shared
            .source_continuation_epoch
            .load(std::sync::atomic::Ordering::Acquire);
        let current = ports.shared.capture_checkpoint_root().unwrap();
        let expected = crate::changelog_v3_roots::validate_retained_history(&current)
            .unwrap()
            .unwrap();
        assert!(walk.continue_on(current.clone(), epoch).unwrap());
        assert!(
            old_pin.upgrade().is_none(),
            "replaced validation pin must retire"
        );
        assert!(walk.close_suffix_page().unwrap());
        assert_eq!(walk.proof().unwrap().history(), expected);
        assert_eq!(walk.rows_read(), [population + 5, 0]);
        assert!(walk.continue_on(current, epoch).unwrap());
        assert!(walk.close_suffix_page().unwrap());
        assert_eq!(
            walk.rows_read(),
            [population + 5, 0],
            "an idle certificate never walks again"
        );
        let final_pin = Arc::downgrade(walk.proof().unwrap().root());
        drop(walk);
        assert!(
            final_pin.upgrade().is_none(),
            "owner retirement releases the final pin"
        );
    }
}

// req: STO-030, STO-032, REC-001
#[test]
fn unclassified_publication_invalidates_a_proof_even_when_source_roots_are_equal() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let mut walk = qualified_walk(&ports);
    while !walk.advance(EvidencePageLimit::new(64).unwrap()).unwrap() {}
    let before = walk.proof().unwrap().history();
    let old_epoch = ports
        .shared
        .source_continuation_epoch
        .load(std::sync::atomic::Ordering::Acquire);
    let lease = ports.shared.mutation_gate.acquire().unwrap();
    let write = ports.shared.database.begin_write().unwrap();
    write
        .open_table(crate::layout::OUTBOX_STATUS)
        .unwrap()
        .insert(
            b"uncovered-native-owner".as_slice(),
            b"actual-change".as_slice(),
        )
        .unwrap();
    ports.shared.commit_durable(write).unwrap();
    drop(lease);
    let epoch = ports
        .shared
        .source_continuation_epoch
        .load(std::sync::atomic::Ordering::Acquire);
    let current = ports.shared.capture_checkpoint_root().unwrap();
    assert_ne!(old_epoch, epoch);
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&current).unwrap(),
        Some(before)
    );
    assert!(!walk.continue_on(current, epoch).unwrap());
}

// req: STO-030, STO-032, REC-001
#[test]
fn qualification_overflow_cannot_revalidate_an_old_epoch() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let mut walk = qualified_walk(&ports);
    while !walk.advance(EvidencePageLimit::new(64).unwrap()).unwrap() {}
    ports
        .shared
        .source_continuation_epoch
        .store(u64::MAX - 1, std::sync::atomic::Ordering::Release);
    ports.shared.invalidate_source_continuation();
    ports.shared.invalidate_source_continuation();
    assert_eq!(
        ports
            .shared
            .source_continuation_epoch
            .load(std::sync::atomic::Ordering::Acquire),
        u64::MAX
    );
    assert!(!walk.qualifies(u64::MAX));
    walk.bind_qualification(u64::MAX);
    assert!(!walk.qualifies(u64::MAX));
}

// req: STO-030, STO-031, STO-032, END-008
#[test]
fn a_completed_native_certificate_drops_its_reader_pin_between_turns() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let mut walk = qualified_walk(&ports);
    while !walk.advance(EvidencePageLimit::new(64).unwrap()).unwrap() {}
    let old_pin = Arc::downgrade(walk.proof().unwrap().root());
    let old_rows = walk.rows_read();
    walk.release_completed_pin();
    assert!(old_pin.upgrade().is_none());
    assert_eq!(
        walk.proof().err().unwrap().kind(),
        StorageErrorKind::Unavailable
    );
    append_native(&ports, 8);
    let epoch = ports
        .shared
        .source_continuation_epoch
        .load(Ordering::Acquire);
    let current = ports.shared.capture_checkpoint_root().unwrap();
    let expected = crate::changelog_v3_roots::validate_retained_history(&current)
        .unwrap()
        .unwrap();
    assert!(walk.continue_on(current, epoch).unwrap());
    assert!(walk.close_suffix_page().unwrap());
    assert_eq!(walk.proof().unwrap().history(), expected);
    assert_eq!(walk.rows_read(), [old_rows[0] + 1, old_rows[1]]);
}

// req: STO-030, STO-032, REC-001
#[test]
fn changed_receipt_preimages_and_gaps_refuse_continuation_and_release_failed_pins() {
    use crate::changelog_v3_activation::HISTORY;
    for substitute in [false, true] {
        let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
        let mut walk = qualified_walk(&ports);
        while !walk.advance(EvidencePageLimit::new(64).unwrap()).unwrap() {}
        walk.release_completed_pin();
        let write = ports.shared.database.begin_write().unwrap();
        {
            let mut table = write.open_table(HISTORY).unwrap();
            if substitute {
                let replacement = table
                    .get(3_u64.to_be_bytes().as_slice())
                    .unwrap()
                    .unwrap()
                    .value()
                    .to_vec();
                table
                    .insert(2_u64.to_be_bytes().as_slice(), replacement.as_slice())
                    .unwrap();
            } else {
                table.remove(2_u64.to_be_bytes().as_slice()).unwrap();
            }
        }
        ports.shared.commit_durable(write).unwrap();
        let epoch = ports
            .shared
            .source_continuation_epoch
            .load(Ordering::Acquire);
        let current = ports.shared.capture_checkpoint_root().unwrap();
        assert!(!walk.continue_on(current.clone(), epoch).unwrap());
        assert_eq!(
            crate::changelog_v3_roots::validate_retained_history(&current)
                .unwrap_err()
                .kind(),
            StorageErrorKind::CorruptData
        );
        drop(current);
        drop(walk);
        let mut control = ports.replication_source_control();
        assert_eq!(
            control.reclaim_history_online().unwrap_err().kind(),
            StorageErrorKind::CorruptData
        );
        assert!(!control.receipt_validation_pending());
        assert!(
            ports
                .shared
                .extent_roots
                .lock()
                .unwrap()
                .iter()
                .all(|pin| pin.strong_count() == 0),
            "failed validation releases every nondurable candidate pin"
        );
    }
}
